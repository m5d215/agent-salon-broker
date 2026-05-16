//! Append-only JSONL event log for shipping to Loki / external observability.
//!
//! When `AGENT_SALON_BROKER_JSONL_LOG` points at a writable path, the broker
//! emits one line of JSON per event (HTTP request, job lifecycle). Each line
//! shares a `ts` (RFC3339) and `kind` discriminator so LogQL queries like
//! `{job="agent-salon-broker"} | json | kind="job" | result="timeout"` work.
//! When the env var is unset the logger is a no-op.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Mutex;

use chrono::{SecondsFormat, Utc};
use serde::Serialize;

pub struct JsonlLogger {
    file: Mutex<Option<File>>,
    path: Option<PathBuf>,
}

impl JsonlLogger {
    /// Open the logger if `AGENT_SALON_BROKER_JSONL_LOG` is set. Failure to
    /// open the path is logged once and produces a no-op logger — we never
    /// want observability plumbing to crash the broker.
    pub fn from_env() -> Self {
        let path = std::env::var("AGENT_SALON_BROKER_JSONL_LOG")
            .ok()
            .map(PathBuf::from);
        let file = match path.as_ref() {
            Some(p) => match OpenOptions::new().create(true).append(true).open(p) {
                Ok(f) => {
                    tracing::info!("jsonl log -> {}", p.display());
                    Some(f)
                }
                Err(e) => {
                    tracing::warn!("cannot open jsonl log {}: {e}", p.display());
                    None
                }
            },
            None => None,
        };
        Self {
            file: Mutex::new(file),
            path,
        }
    }

    pub fn enabled(&self) -> bool {
        self.path.is_some()
    }

    fn write_line<T: Serialize>(&self, value: &T) {
        let Ok(mut guard) = self.file.lock() else {
            return;
        };
        let Some(f) = guard.as_mut() else { return };
        let Ok(line) = serde_json::to_string(value) else {
            return;
        };
        let _ = writeln!(f, "{line}");
    }

    fn now_rfc3339() -> String {
        Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
    }

    /// Emit a "request" line for an HTTP handler completion.
    pub fn request(
        &self,
        endpoint: &str,
        method: &str,
        status: u16,
        duration_ms: u64,
        job_id: Option<&str>,
    ) {
        if !self.enabled() {
            return;
        }
        #[derive(Serialize)]
        struct Entry<'a> {
            ts: String,
            kind: &'static str,
            endpoint: &'a str,
            method: &'a str,
            status: u16,
            duration_ms: u64,
            #[serde(skip_serializing_if = "Option::is_none")]
            job_id: Option<&'a str>,
        }
        self.write_line(&Entry {
            ts: Self::now_rfc3339(),
            kind: "request",
            endpoint,
            method,
            status,
            duration_ms,
            job_id,
        });
    }

    /// Emit a "job" line for a job reaching a terminal state.
    pub fn job(
        &self,
        job_id: &str,
        target: &str,
        result: &str,
        duration_sec: f64,
        prompt_len: usize,
        result_len: Option<usize>,
        error: Option<&str>,
    ) {
        if !self.enabled() {
            return;
        }
        #[derive(Serialize)]
        struct Entry<'a> {
            ts: String,
            kind: &'static str,
            job_id: &'a str,
            target: &'a str,
            result: &'a str,
            duration_sec: f64,
            prompt_len: usize,
            #[serde(skip_serializing_if = "Option::is_none")]
            result_len: Option<usize>,
            #[serde(skip_serializing_if = "Option::is_none")]
            error: Option<&'a str>,
        }
        self.write_line(&Entry {
            ts: Self::now_rfc3339(),
            kind: "job",
            job_id,
            target,
            result,
            duration_sec,
            prompt_len,
            result_len,
            error,
        });
    }

    /// Emit a free-form "event" line.
    pub fn event(&self, event: &str, fields: serde_json::Value) {
        if !self.enabled() {
            return;
        }
        #[derive(Serialize)]
        struct Entry<'a> {
            ts: String,
            kind: &'static str,
            event: &'a str,
            #[serde(flatten)]
            fields: serde_json::Value,
        }
        self.write_line(&Entry {
            ts: Self::now_rfc3339(),
            kind: "event",
            event,
            fields,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn parse_lines(path: &std::path::Path) -> Vec<serde_json::Value> {
        let s = std::fs::read_to_string(path).unwrap();
        s.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn no_op_when_env_unset() {
        // SAFETY: tests in this module are sequential; we touch a process-wide env.
        unsafe { std::env::remove_var("AGENT_SALON_BROKER_JSONL_LOG") };
        let logger = JsonlLogger::from_env();
        assert!(!logger.enabled());
        logger.request("/submit", "POST", 200, 12, Some("abc"));
        logger.job("abc", "claudep", "done", 3.5, 100, Some(200), None);
        logger.event("broker_started", serde_json::json!({"version": "0.3.0"}));
    }

    #[test]
    fn writes_lines() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.jsonl");
        // SAFETY: tests in this module are sequential.
        unsafe { std::env::set_var("AGENT_SALON_BROKER_JSONL_LOG", &path) };
        let logger = JsonlLogger::from_env();
        assert!(logger.enabled());
        logger.request("/submit", "POST", 200, 12, Some("abc"));
        logger.job("abc", "claudep", "done", 3.5, 100, Some(200), None);
        logger.job(
            "def",
            "claudep",
            "timeout",
            600.0,
            50,
            None,
            Some("timed out after 600s"),
        );
        logger.event("broker_started", serde_json::json!({"version": "0.3.0"}));
        // SAFETY: as above.
        unsafe { std::env::remove_var("AGENT_SALON_BROKER_JSONL_LOG") };

        let lines = parse_lines(&path);
        assert_eq!(lines.len(), 4);
        assert_eq!(lines[0]["kind"], "request");
        assert_eq!(lines[0]["endpoint"], "/submit");
        assert_eq!(lines[0]["status"], 200);
        assert_eq!(lines[1]["kind"], "job");
        assert_eq!(lines[1]["result"], "done");
        assert_eq!(lines[1]["result_len"], 200);
        assert_eq!(lines[2]["result"], "timeout");
        assert!(lines[2]["result_len"].is_null());
        assert_eq!(lines[2]["error"], "timed out after 600s");
        assert_eq!(lines[3]["event"], "broker_started");
        assert_eq!(lines[3]["version"], "0.3.0");
    }
}
