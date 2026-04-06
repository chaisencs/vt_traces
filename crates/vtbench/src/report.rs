use std::{collections::BTreeMap, time::Duration};

use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, Serialize)]
pub struct CanonicalRunReport {
    pub report_version: u32,
    pub replay_id: String,
    pub mode: String,
    pub git_sha: String,
    pub command_display: String,
    pub raw_args: Vec<String>,
    pub started_at_unix_ms: u128,
    pub ended_at_unix_ms: u128,
    pub duration_ms: f64,
    pub operations: usize,
    pub errors: usize,
    pub ops_per_sec: f64,
    pub options: BTreeMap<String, Value>,
    pub metadata: BTreeMap<String, String>,
    pub public_report: bool,
    pub comparison_arms: Vec<String>,
    pub latency: Option<LatencyStats>,
    pub timeline: Option<Vec<Value>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LatencyStats {
    pub samples: usize,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub p999_ms: f64,
    pub max_ms: f64,
}

impl LatencyStats {
    pub(crate) fn from_summary(summary: &crate::LatencySummary) -> Option<Self> {
        if summary.sample_count() == 0 {
            return None;
        }

        Some(Self {
            samples: summary.sample_count(),
            p50_ms: summary.quantile_ms(0.50),
            p95_ms: summary.quantile_ms(0.95),
            p99_ms: summary.quantile_ms(0.99),
            p999_ms: summary.quantile_ms(0.999),
            max_ms: summary.max_ms(),
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct CanonicalRunManifest {
    pub replay_id: String,
    pub mode: String,
    pub git_sha: String,
    pub command_display: String,
    pub raw_args: Vec<String>,
    pub cwd: String,
    pub started_at_unix_ms: u128,
    pub ended_at_unix_ms: u128,
    pub duration_ms: f64,
    pub public_report: bool,
    pub comparison_arms: Vec<String>,
    pub files: ManifestFiles,
}

#[derive(Debug, Clone, Serialize)]
pub struct ManifestFiles {
    pub report: String,
    pub env: String,
    pub context: String,
    pub commands: String,
    pub decision: String,
    pub rerun: String,
    pub stdout: String,
    pub stderr: String,
    pub metrics: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ContextSnapshot {
    pub mode: String,
    pub options: BTreeMap<String, Value>,
    pub metadata: BTreeMap<String, String>,
    pub public_report: bool,
    pub comparison_arms: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct CommandTrace {
    pub tool: String,
    pub cwd: String,
    pub argv: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DecisionSnapshot {
    pub status: String,
    pub public_report: bool,
    pub publishable: bool,
    pub error_count: usize,
}

pub fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1000.0
}
