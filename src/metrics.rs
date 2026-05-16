//! Prometheus metrics exposed at `GET /metrics` for scraping by Alloy /
//! Prometheus / Mimir. The metric set is intentionally small and bounded:
//! every label value comes from a controlled vocabulary (fixed endpoint
//! template, status class buckets, job result strings) so series cardinality
//! is predictable.

use std::sync::Mutex;

use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct RequestLabels {
    /// Path template, not the live URL. e.g. `/status/:id`, never `/status/abc-123`.
    pub endpoint: String,
    /// `2xx` | `4xx` | `5xx`
    pub status_class: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct EndpointLabels {
    pub endpoint: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct JobResultLabels {
    /// `done` | `timeout`
    pub result: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BuildInfoLabels {
    pub version: String,
}

/// Bucket boundaries for HTTP handler latency. Broker handlers themselves are
/// fast (the heavy lifting is downstream of `/submit` returning); a 1s tail is
/// already abnormal.
const REQUEST_DURATION_BUCKETS: &[f64] = &[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5];

/// Bucket boundaries for job duration (submit → done). Claude inference can
/// take tens of seconds for complex prompts, so the upper end reaches the
/// default 10-minute timeout.
const JOB_DURATION_BUCKETS: &[f64] = &[1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0];

pub fn status_class(status: u16) -> &'static str {
    match status {
        200..=299 => "2xx",
        300..=399 => "3xx",
        400..=499 => "4xx",
        500..=599 => "5xx",
        _ => "other",
    }
}

pub struct Metrics {
    pub registry: Mutex<Registry>,
    pub requests: Family<RequestLabels, Counter>,
    pub request_duration: Family<EndpointLabels, Histogram>,
    pub in_flight_requests: Gauge,
    pub jobs: Family<JobResultLabels, Counter>,
    pub job_duration: Family<JobResultLabels, Histogram>,
    pub jobs_in_flight: Gauge,
    pub salon_send_failures: Counter,
    pub build_info: Family<BuildInfoLabels, Gauge>,
}

impl Metrics {
    pub fn new() -> Self {
        let mut registry = Registry::with_prefix("agent_salon_broker");

        let requests = Family::<RequestLabels, Counter>::default();
        registry.register(
            "requests",
            "HTTP requests handled, by endpoint template and status class",
            requests.clone(),
        );

        let request_duration = Family::<EndpointLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(REQUEST_DURATION_BUCKETS.iter().copied())
        });
        registry.register(
            "request_duration_seconds",
            "HTTP handler duration, by endpoint template",
            request_duration.clone(),
        );

        let in_flight_requests = Gauge::default();
        registry.register(
            "in_flight_requests",
            "Currently executing HTTP handlers",
            in_flight_requests.clone(),
        );

        let jobs = Family::<JobResultLabels, Counter>::default();
        registry.register(
            "jobs",
            "Terminated jobs, by result",
            jobs.clone(),
        );

        let job_duration = Family::<JobResultLabels, Histogram>::new_with_constructor(|| {
            Histogram::new(JOB_DURATION_BUCKETS.iter().copied())
        });
        registry.register(
            "job_duration_seconds",
            "End-to-end job duration from submit to terminal state, by result",
            job_duration.clone(),
        );

        let jobs_in_flight = Gauge::default();
        registry.register(
            "jobs_in_flight",
            "Jobs currently in Queued or Assigned state",
            jobs_in_flight.clone(),
        );

        let salon_send_failures = Counter::default();
        registry.register(
            "salon_send_failures",
            "send_message calls to agent-salon that returned an error",
            salon_send_failures.clone(),
        );

        let build_info = Family::<BuildInfoLabels, Gauge>::default();
        registry.register(
            "build_info",
            "Build identity (always 1); label-only carrier for the running version",
            build_info.clone(),
        );

        Self {
            registry: Mutex::new(registry),
            requests,
            request_duration,
            in_flight_requests,
            jobs,
            job_duration,
            jobs_in_flight,
            salon_send_failures,
            build_info,
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::status_class;

    #[test]
    fn maps_status_to_class() {
        assert_eq!(status_class(200), "2xx");
        assert_eq!(status_class(204), "2xx");
        assert_eq!(status_class(301), "3xx");
        assert_eq!(status_class(404), "4xx");
        assert_eq!(status_class(502), "5xx");
        assert_eq!(status_class(99), "other");
    }
}
