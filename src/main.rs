//! agent-salon-broker — broker daemon for routing `claude -p`-style jobs
//! through agent-salon to a persistent claude code session.

mod jsonl;
mod metrics;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use crate::jsonl::JsonlLogger;
use crate::metrics::{
    BuildInfoLabels, EndpointLabels, JobResultLabels, Metrics, RequestLabels, status_class,
};

use anyhow::Result;
use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::{DateTime, Utc};
use rmcp::{
    ClientHandler, RoleClient, ServiceExt,
    model::{
        CallToolRequestParams, ClientCapabilities, ClientInfo, CustomNotification, Implementation,
    },
    service::{NotificationContext, RunningService},
    transport::StreamableHttpClientTransport,
};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

const DEFAULT_SALON_URL: &str = "http://127.0.0.1:9315/mcp?label=broker";
const DEFAULT_TARGET: &str = "claudep";
const DEFAULT_LISTEN: &str = "127.0.0.1:9316";
const DEFAULT_TIMEOUT_SEC: i64 = 600;
const SWEEPER_INTERVAL_SEC: u64 = 1;

/// Resolve a config value, preferring the live process environment over
/// any value loaded from the config file. Empty environment values are
/// treated as "set" (returned as-is) — same behavior as `std::env::var`.
fn cfg_var(config: &HashMap<String, String>, key: &str) -> Option<String> {
    std::env::var(key).ok().or_else(|| config.get(key).cloned())
}

/// Standard Homebrew prefixes searched when `AGENT_SALON_BROKER_CONFIG` is
/// unset. The first existing file wins. Order: Apple Silicon, Intel Mac,
/// Linuxbrew. Lets `agent-salon-broker submit` from a user shell auto-pick
/// up the same conf the launchd service uses, without requiring the env var
/// in shell rc.
const FALLBACK_CONFIG_PATHS: &[&str] = &[
    "/opt/homebrew/etc/agent-salon-broker.conf",
    "/usr/local/etc/agent-salon-broker.conf",
    "/home/linuxbrew/.linuxbrew/etc/agent-salon-broker.conf",
];

/// Resolve the config file path. If `AGENT_SALON_BROKER_CONFIG` is set we
/// honor it verbatim (missing file → warning). Otherwise we probe the
/// well-known Homebrew prefixes and return the first one that exists; if
/// none exist, no config file is used.
fn resolve_config_path() -> Option<String> {
    if let Ok(p) = std::env::var("AGENT_SALON_BROKER_CONFIG") {
        return Some(p);
    }
    FALLBACK_CONFIG_PATHS
        .iter()
        .find(|p| std::path::Path::new(p).exists())
        .map(|p| (*p).to_string())
}

/// Read the resolved config path and parse it. Returns an empty map when
/// no path resolves or the file is unreadable.
fn load_config_file() -> HashMap<String, String> {
    let Some(path) = resolve_config_path() else {
        return HashMap::new();
    };
    match std::fs::read_to_string(&path) {
        Ok(s) => {
            let map = parse_config(&s);
            tracing::info!("loaded {} setting(s) from {path}", map.len());
            map
        }
        Err(e) => {
            tracing::warn!("skipping config {path}: {e}");
            HashMap::new()
        }
    }
}

/// Parse a `KEY=VALUE` config file. Lines starting with `#` and blank lines
/// are skipped. Keys with no `=` are skipped. Surrounding double quotes
/// around the value (`KEY="value"`) are stripped. Whitespace around the key
/// and around the value (outside the quotes) is trimmed.
fn parse_config(s: &str) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for line in s.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some((k, v)) = line.split_once('=') else {
            continue;
        };
        let k = k.trim();
        if k.is_empty() {
            continue;
        }
        let v = v.trim();
        let v = if v.len() >= 2 && v.starts_with('"') && v.ends_with('"') {
            &v[1..v.len() - 1]
        } else {
            v
        };
        out.insert(k.to_string(), v.to_string());
    }
    out
}

/// Worker prompt emitted by `agent-salon-broker prompt`. Read into a claude
/// code session (e.g. via `! agent-salon-broker prompt`) to set up the session
/// as a worker for this broker.
const WORKER_PROMPT: &str = include_str!("worker_prompt.md");

// ---------- Job model ----------

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum JobStatus {
    Queued,
    Assigned,
    Done,
    Timeout,
}

#[derive(Debug, Clone, Serialize)]
struct Job {
    job_id: String,
    target: String,
    prompt: String,
    status: JobStatus,
    result: Option<String>,
    error: Option<String>,
    timeout_sec: i64,
    created_at: DateTime<Utc>,
    assigned_at: Option<DateTime<Utc>>,
    completed_at: Option<DateTime<Utc>>,
}

// ---------- In-memory job store ----------
//
// Jobs live for the lifetime of the daemon process. `claude -p`-style use
// cases consume results synchronously, and reviving an `assigned` job after
// a daemon restart wouldn't actually recover it (the worker's reply would
// have gone to the previous MCP connection and been dropped by the salon).
// So persistence is intentionally absent.

#[derive(Clone, Default)]
struct JobStore {
    jobs: Arc<Mutex<HashMap<String, Job>>>,
}

impl JobStore {
    fn new() -> Self {
        Self::default()
    }

    fn insert(&self, job: Job) {
        self.jobs.lock().unwrap().insert(job.job_id.clone(), job);
    }

    fn get(&self, id: &str) -> Option<Job> {
        self.jobs.lock().unwrap().get(id).cloned()
    }

    fn list(&self) -> Vec<Job> {
        let map = self.jobs.lock().unwrap();
        let mut v: Vec<Job> = map.values().cloned().collect();
        v.sort_by_key(|j| std::cmp::Reverse(j.created_at));
        v
    }

    /// Queued → Assigned. No-op if already in a later state (preserves a Done
    /// that landed before send_message returned in self-loop scenarios).
    /// `assigned_at` is always recorded.
    fn mark_assigned(&self, id: &str) {
        let mut map = self.jobs.lock().unwrap();
        if let Some(j) = map.get_mut(id) {
            j.assigned_at = Some(Utc::now());
            if matches!(j.status, JobStatus::Queued) {
                j.status = JobStatus::Assigned;
            }
        }
    }

    /// Mark job as done with given result. Returns the freshly-terminated Job
    /// when this call transitioned it (None if it was already in a terminal
    /// state or unknown). The returned Job is observed by the caller to emit
    /// jobs_total / job_duration_seconds metrics and the kind="job" log entry.
    fn complete(&self, id: &str, result: &str) -> Option<Job> {
        let mut map = self.jobs.lock().unwrap();
        let j = map.get_mut(id)?;
        if matches!(j.status, JobStatus::Done | JobStatus::Timeout) {
            return None;
        }
        j.status = JobStatus::Done;
        j.result = Some(result.to_string());
        j.completed_at = Some(Utc::now());
        Some(j.clone())
    }

    /// Mark every Queued / Assigned job whose deadline has passed as `Timeout`.
    /// Returns the freshly-terminated Jobs so the caller can emit metrics +
    /// log entries for each.
    fn sweep_timeouts(&self, now: DateTime<Utc>) -> Vec<Job> {
        let mut transitioned = Vec::new();
        let mut map = self.jobs.lock().unwrap();
        for job in map.values_mut() {
            if !matches!(job.status, JobStatus::Queued | JobStatus::Assigned) {
                continue;
            }
            // Anchor on assigned_at (the moment we entrusted the work to salon),
            // falling back to created_at for jobs that never reached the send.
            let anchor = job.assigned_at.unwrap_or(job.created_at);
            let deadline = anchor + chrono::Duration::seconds(job.timeout_sec);
            if now > deadline {
                job.status = JobStatus::Timeout;
                job.error = Some(format!("timed out after {}s", job.timeout_sec));
                job.completed_at = Some(now);
                transitioned.push(job.clone());
            }
        }
        transitioned
    }

    fn in_flight_count(&self) -> i64 {
        let map = self.jobs.lock().unwrap();
        map.values()
            .filter(|j| matches!(j.status, JobStatus::Queued | JobStatus::Assigned))
            .count() as i64
    }
}

/// Compute job duration from created_at to completed_at, in seconds.
/// Returns 0.0 if completed_at is missing (shouldn't happen for terminated jobs).
fn job_duration_sec(job: &Job) -> f64 {
    match job.completed_at {
        Some(end) => (end - job.created_at).num_milliseconds() as f64 / 1000.0,
        None => 0.0,
    }
}

// ---------- Salon client handler ----------

#[derive(Clone)]
struct BrokerClient {
    store: JobStore,
    /// Fallback receiver for notifications that don't match a known job_id.
    debug_tx: mpsc::UnboundedSender<serde_json::Value>,
    metrics: Arc<Metrics>,
    jsonl: Arc<JsonlLogger>,
}

impl ClientHandler for BrokerClient {
    fn on_custom_notification(
        &self,
        notification: CustomNotification,
        _ctx: NotificationContext<RoleClient>,
    ) -> impl std::future::Future<Output = ()> + Send + '_ {
        let store = self.store.clone();
        let debug_tx = self.debug_tx.clone();
        let metrics = self.metrics.clone();
        let jsonl = self.jsonl.clone();
        async move {
            if notification.method != "notifications/claude/channel" {
                return;
            }
            let Some(params_value) = notification.params else {
                return;
            };

            let job_id = params_value
                .get("meta")
                .and_then(|m| m.get("job_id"))
                .and_then(|v| v.as_str())
                .map(|s| s.to_string());
            let content = params_value
                .get("content")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();

            match job_id {
                Some(id) => match store.complete(&id, &content) {
                    Some(job) => {
                        tracing::info!(%id, "job completed via salon notification");
                        let labels = JobResultLabels {
                            result: "done".to_string(),
                        };
                        metrics.jobs.get_or_create(&labels).inc();
                        metrics
                            .job_duration
                            .get_or_create(&labels)
                            .observe(job_duration_sec(&job));
                        metrics.jobs_in_flight.set(store.in_flight_count());
                        jsonl.job(
                            &job.job_id,
                            &job.target,
                            "done",
                            job_duration_sec(&job),
                            job.prompt.len(),
                            Some(content.len()),
                            None,
                        );
                    }
                    None => {
                        tracing::debug!(%id, "notification matched no pending job (already done or unknown)");
                    }
                },
                None => {
                    tracing::debug!("notification has no meta.job_id; forwarding to debug channel");
                    let _ = debug_tx.send(params_value);
                }
            }
        }
    }

    fn get_info(&self) -> ClientInfo {
        ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("agent-salon-broker", env!("CARGO_PKG_VERSION")),
        )
    }
}

// ---------- HTTP layer ----------

#[derive(Clone)]
struct AppState {
    store: JobStore,
    client: Arc<RunningService<RoleClient, BrokerClient>>,
    default_target: String,
    default_timeout_sec: i64,
    metrics: Arc<Metrics>,
    jsonl: Arc<JsonlLogger>,
}

/// Wraps a handler closure with metrics + JSONL request log instrumentation.
/// `endpoint` is the path template (e.g. `/status/:id`), never the live URL.
async fn record_request<F, Fut>(
    state: &AppState,
    endpoint: &'static str,
    method: &'static str,
    job_id: Option<String>,
    f: F,
) -> axum::response::Response
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = axum::response::Response>,
{
    state.metrics.in_flight_requests.inc();
    let start = Instant::now();
    let response = f().await;
    let elapsed = start.elapsed();
    state.metrics.in_flight_requests.dec();

    let status = response.status().as_u16();
    let labels = RequestLabels {
        endpoint: endpoint.to_string(),
        status_class: status_class(status).to_string(),
    };
    state.metrics.requests.get_or_create(&labels).inc();
    state
        .metrics
        .request_duration
        .get_or_create(&EndpointLabels {
            endpoint: endpoint.to_string(),
        })
        .observe(elapsed.as_secs_f64());
    state.jsonl.request(
        endpoint,
        method,
        status,
        elapsed.as_millis() as u64,
        job_id.as_deref(),
    );
    response
}

#[derive(Debug, Deserialize)]
struct SubmitBody {
    prompt: String,
    /// Override the salon target label for this job. Defaults to env-configured target.
    target: Option<String>,
    /// Job-specific timeout in seconds. Defaults to broker's configured default.
    timeout_sec: Option<i64>,
}

#[derive(Debug, Serialize)]
struct SubmitResponse {
    job_id: String,
}

async fn handle_submit(
    State(state): State<AppState>,
    Json(body): Json<SubmitBody>,
) -> axum::response::Response {
    let job_id = Uuid::new_v4().to_string();
    let target = body.target.unwrap_or_else(|| state.default_target.clone());
    let timeout_sec = body.timeout_sec.unwrap_or(state.default_timeout_sec);
    let prompt = body.prompt;
    let job_id_for_log = job_id.clone();

    record_request(&state, "/submit", "POST", Some(job_id.clone()), || async {
        let job = Job {
            job_id: job_id.clone(),
            target: target.clone(),
            prompt: prompt.clone(),
            status: JobStatus::Queued,
            result: None,
            error: None,
            timeout_sec,
            created_at: Utc::now(),
            assigned_at: None,
            completed_at: None,
        };
        state.store.insert(job);
        state
            .metrics
            .jobs_in_flight
            .set(state.store.in_flight_count());

        let send_result = state
            .client
            .call_tool(
                CallToolRequestParams::new("send_message").with_arguments(
                    serde_json::json!({
                        "content": prompt,
                        "target": target,
                        "meta": { "job_id": job_id, "kind": "request" },
                    })
                    .as_object()
                    .cloned()
                    .unwrap_or_default(),
                ),
            )
            .await;

        match send_result {
            Ok(_) => {
                state.store.mark_assigned(&job_id);
                tracing::info!(%job_id_for_log, %target, "job dispatched");
                (StatusCode::OK, Json(SubmitResponse { job_id })).into_response()
            }
            Err(e) => {
                state.metrics.salon_send_failures.inc();
                tracing::warn!(%job_id_for_log, error=?e, "send_message failed");
                (
                    StatusCode::BAD_GATEWAY,
                    format!("salon send_message failed: {e}"),
                )
                    .into_response()
            }
        }
    })
    .await
}

async fn handle_status(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> axum::response::Response {
    record_request(&state, "/status/:id", "GET", Some(id.clone()), || async {
        match state.store.get(&id) {
            Some(job) => (StatusCode::OK, Json(job)).into_response(),
            None => (StatusCode::NOT_FOUND, "no such job").into_response(),
        }
    })
    .await
}

async fn handle_list(State(state): State<AppState>) -> axum::response::Response {
    record_request(&state, "/jobs", "GET", None, || async {
        Json(state.store.list()).into_response()
    })
    .await
}

async fn handle_metrics(State(state): State<AppState>) -> axum::response::Response {
    let body = {
        let registry = state.metrics.registry.lock().unwrap();
        let mut buf = String::new();
        if let Err(e) = prometheus_client::encoding::text::encode(&mut buf, &registry) {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("metrics encode failed: {e}"),
            )
                .into_response();
        }
        buf
    };
    (
        StatusCode::OK,
        [(
            "content-type",
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

// ---------- Caller mode ----------

const CALLER_USAGE: &str = "usage: agent-salon-broker submit <prompt> [--target <label>] [--timeout <sec>] [--base-url <url>]";

async fn run_caller(args: &[String]) -> Result<()> {
    let config = load_config_file();
    let mut prompt: Option<String> = None;
    let mut target: Option<String> = None;
    let mut timeout_sec: Option<i64> = None;
    let mut base_url = cfg_var(&config, "AGENT_SALON_BROKER_BASE_URL").unwrap_or_else(|| {
        let listen = cfg_var(&config, "AGENT_SALON_BROKER_LISTEN")
            .unwrap_or_else(|| DEFAULT_LISTEN.to_string());
        // listen can be 0.0.0.0:N (caller can't connect to that) — rewrite to loopback.
        let host_port = listen.replacen("0.0.0.0:", "127.0.0.1:", 1);
        format!("http://{host_port}")
    });

    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--target" => {
                target = Some(
                    args.get(i + 1)
                        .cloned()
                        .ok_or_else(|| anyhow::anyhow!(CALLER_USAGE))?,
                );
                i += 2;
            }
            "--timeout" => {
                let v = args
                    .get(i + 1)
                    .ok_or_else(|| anyhow::anyhow!(CALLER_USAGE))?;
                timeout_sec = Some(v.parse()?);
                i += 2;
            }
            "--base-url" => {
                base_url = args
                    .get(i + 1)
                    .cloned()
                    .ok_or_else(|| anyhow::anyhow!(CALLER_USAGE))?;
                i += 2;
            }
            other if other.starts_with("--") => {
                anyhow::bail!("unknown flag: {other}\n{CALLER_USAGE}");
            }
            _ => {
                if prompt.is_some() {
                    anyhow::bail!("only one positional <prompt> allowed\n{CALLER_USAGE}");
                }
                prompt = Some(args[i].clone());
                i += 1;
            }
        }
    }

    let prompt = prompt.ok_or_else(|| anyhow::anyhow!(CALLER_USAGE))?;

    let mut body = serde_json::json!({ "prompt": prompt });
    if let Some(t) = target {
        body["target"] = serde_json::Value::String(t);
    }
    if let Some(t) = timeout_sec {
        body["timeout_sec"] = serde_json::Value::Number(t.into());
    }

    let http = reqwest::Client::new();
    let submit: serde_json::Value = http
        .post(format!("{base_url}/submit"))
        .json(&body)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let job_id = submit
        .get("job_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("missing job_id in /submit response: {submit}"))?
        .to_string();
    eprintln!("submitted job_id={job_id}");

    // long-poll /status until terminal
    loop {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let job: serde_json::Value = http
            .get(format!("{base_url}/status/{job_id}"))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let status = job
            .get("status")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        match status {
            "done" => {
                let result = job.get("result").and_then(|v| v.as_str()).unwrap_or("");
                print!("{result}");
                if !result.ends_with('\n') {
                    println!();
                }
                return Ok(());
            }
            "failed" | "timeout" => {
                let err = job.get("error").and_then(|v| v.as_str()).unwrap_or(status);
                anyhow::bail!("job {status}: {err}");
            }
            _ => continue,
        }
    }
}

// ---------- Daemon entrypoint ----------

async fn run_daemon() -> Result<()> {
    let config = load_config_file();
    let salon_url =
        cfg_var(&config, "AGENT_SALON_URL").unwrap_or_else(|| DEFAULT_SALON_URL.to_string());
    let default_target =
        cfg_var(&config, "AGENT_SALON_BROKER_TARGET").unwrap_or_else(|| DEFAULT_TARGET.to_string());
    let default_timeout_sec: i64 = cfg_var(&config, "AGENT_SALON_BROKER_TIMEOUT_SEC")
        .and_then(|s| s.parse().ok())
        .unwrap_or(DEFAULT_TIMEOUT_SEC);
    let listen: SocketAddr = cfg_var(&config, "AGENT_SALON_BROKER_LISTEN")
        .unwrap_or_else(|| DEFAULT_LISTEN.to_string())
        .parse()?;

    tracing::info!(%salon_url, %default_target, default_timeout_sec, %listen, "starting agent-salon-broker");

    let metrics = Arc::new(Metrics::new());
    metrics
        .build_info
        .get_or_create(&BuildInfoLabels {
            version: env!("CARGO_PKG_VERSION").to_string(),
        })
        .set(1);
    let jsonl = Arc::new(JsonlLogger::from_env());
    jsonl.event(
        "broker_started",
        serde_json::json!({
            "version": env!("CARGO_PKG_VERSION"),
            "listen": listen.to_string(),
            "default_target": default_target,
        }),
    );

    let store = JobStore::new();
    let (debug_tx, _debug_rx) = mpsc::unbounded_channel::<serde_json::Value>();
    let handler = BrokerClient {
        store: store.clone(),
        debug_tx,
        metrics: metrics.clone(),
        jsonl: jsonl.clone(),
    };

    let transport = StreamableHttpClientTransport::from_uri(salon_url);
    let client = handler.serve(transport).await?;
    let peer_name = client
        .peer_info()
        .as_ref()
        .map(|p| p.server_info.name.clone())
        .unwrap_or_else(|| "unknown".to_string());
    tracing::info!(peer = %peer_name, "salon connected");
    jsonl.event("salon_connected", serde_json::json!({"peer": peer_name}));

    // Timeout sweeper.
    let sweeper_store = store.clone();
    let sweeper_metrics = metrics.clone();
    let sweeper_jsonl = jsonl.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(SWEEPER_INTERVAL_SEC));
        loop {
            tick.tick().await;
            for job in sweeper_store.sweep_timeouts(Utc::now()) {
                tracing::warn!(job_id = %job.job_id, "job timed out");
                let labels = JobResultLabels {
                    result: "timeout".to_string(),
                };
                sweeper_metrics.jobs.get_or_create(&labels).inc();
                sweeper_metrics
                    .job_duration
                    .get_or_create(&labels)
                    .observe(job_duration_sec(&job));
                sweeper_metrics
                    .jobs_in_flight
                    .set(sweeper_store.in_flight_count());
                sweeper_jsonl.job(
                    &job.job_id,
                    &job.target,
                    "timeout",
                    job_duration_sec(&job),
                    job.prompt.len(),
                    None,
                    job.error.as_deref(),
                );
            }
        }
    });

    let app_state = AppState {
        store,
        client: Arc::new(client),
        default_target,
        default_timeout_sec,
        metrics,
        jsonl,
    };

    let app = Router::new()
        .route("/submit", post(handle_submit))
        .route("/status/{id}", get(handle_status))
        .route("/jobs", get(handle_list))
        .route("/metrics", get(handle_metrics))
        .with_state(app_state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    tracing::info!(%listen, "http listening");

    let server = axum::serve(listener, app.into_make_service());

    tokio::select! {
        res = server => {
            tracing::warn!(?res, "http server exited");
        }
        _ = tokio::signal::ctrl_c() => {
            tracing::info!("ctrl-c received, shutting down");
        }
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() >= 2 && args[1] == "prompt" {
        // Emit the worker setup prompt; no logging, no async work.
        print!("{WORKER_PROMPT}");
        return Ok(());
    }
    if args.len() >= 2 && args[1] == "submit" {
        // caller mode: minimal logging to stderr only
        tracing_subscriber::registry()
            .with(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "warn".into()),
            )
            .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
            .init();
        return run_caller(&args[2..]).await;
    }

    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,agent_salon_broker=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();
    run_daemon().await
}

#[cfg(test)]
mod tests {
    use super::parse_config;

    #[test]
    fn parses_config_basic() {
        let s = "\
            AGENT_SALON_BROKER_LISTEN=0.0.0.0:9316\n\
            AGENT_SALON_BROKER_TARGET=claudep\n\
            AGENT_SALON_URL=http://127.0.0.1:9315/mcp?label=broker\n\
        ";
        let m = parse_config(s);
        assert_eq!(m.get("AGENT_SALON_BROKER_LISTEN").unwrap(), "0.0.0.0:9316");
        assert_eq!(m.get("AGENT_SALON_BROKER_TARGET").unwrap(), "claudep");
        assert_eq!(
            m.get("AGENT_SALON_URL").unwrap(),
            "http://127.0.0.1:9315/mcp?label=broker"
        );
    }

    #[test]
    fn parses_config_skips_blank_comment_and_malformed() {
        let s = "\n# comment\n  # indented comment\n\nLISTEN=0.0.0.0:9316\nnokeyeq\n=onlyvalue\nGOOD=ok\n";
        let m = parse_config(s);
        assert_eq!(m.get("LISTEN").unwrap(), "0.0.0.0:9316");
        assert_eq!(m.get("GOOD").unwrap(), "ok");
        assert_eq!(m.len(), 2);
    }

    #[test]
    fn parses_config_strips_outer_quotes_and_preserves_inner() {
        let s = "URL=\"http://host:9315/mcp?label=broker&x=1\"\n";
        let m = parse_config(s);
        assert_eq!(
            m.get("URL").unwrap(),
            "http://host:9315/mcp?label=broker&x=1"
        );
    }
}
