use std::{fs, path::PathBuf};

use anyhow::{anyhow, bail, Context};
use serde_json::{Map, Value};

const REPORT_SCHEMA_VERSION: u64 = 1;
const SHAPE_KEYS: &[&str] = &[
    "report_schema_version",
    "mode",
    "duration_secs",
    "warmup_secs",
    "sample_interval_secs",
    "fault_after_secs",
    "fault_duration_secs",
    "rows",
    "batch_size",
    "blocks_per_append",
    "traces",
    "spans_per_trace",
    "queries",
    "spans_per_request",
    "spans_per_block",
    "payload_variants",
    "trace_shards",
    "concurrency",
    "binary_target_arch",
    "binary_target_os",
];

#[derive(Debug, Clone)]
pub(crate) struct CompareOptions {
    pub(crate) baseline_file: PathBuf,
    pub(crate) candidate_file: PathBuf,
    pub(crate) min_throughput_ratio: f64,
    pub(crate) max_p99_ratio: f64,
}

#[derive(Debug)]
struct CompareSummary {
    baseline_git_sha: Option<String>,
    candidate_git_sha: Option<String>,
    baseline_replay_id: Option<String>,
    candidate_replay_id: Option<String>,
    throughput_ratio: f64,
    p99_ratio: f64,
    min_throughput_ratio: f64,
    max_p99_ratio: f64,
}

pub(crate) fn run_compare(options: CompareOptions) -> anyhow::Result<()> {
    let baseline = read_report(&options.baseline_file)?;
    let candidate = read_report(&options.candidate_file)?;
    validate_shape_match(&baseline, &candidate)?;

    let summary = CompareSummary {
        baseline_git_sha: string_field(&baseline, "git_sha"),
        candidate_git_sha: string_field(&candidate, "git_sha"),
        baseline_replay_id: string_field(&baseline, "replay_id"),
        candidate_replay_id: string_field(&candidate, "replay_id"),
        throughput_ratio: ratio(
            number_field(&baseline, "ops_per_sec")?,
            number_field(&candidate, "ops_per_sec")?,
            "ops_per_sec",
        )?,
        p99_ratio: ratio(
            number_field(&baseline, "latency_p99_ms")?,
            number_field(&candidate, "latency_p99_ms")?,
            "latency_p99_ms",
        )?,
        min_throughput_ratio: options.min_throughput_ratio,
        max_p99_ratio: options.max_p99_ratio,
    };

    if summary.throughput_ratio < summary.min_throughput_ratio {
        bail!(failure_message("throughput regression", &summary));
    }
    if summary.p99_ratio > summary.max_p99_ratio {
        bail!(failure_message("p99 regression", &summary));
    }

    print_summary("pass", &summary);
    Ok(())
}

pub(crate) fn default_report_schema_version() -> u64 {
    REPORT_SCHEMA_VERSION
}

fn read_report(path: &PathBuf) -> anyhow::Result<Map<String, Value>> {
    let payload = fs::read(path).with_context(|| format!("read report {}", path.display()))?;
    let value: Value = serde_json::from_slice(&payload)
        .with_context(|| format!("parse report {}", path.display()))?;
    match value {
        Value::Object(object) => Ok(object),
        _ => Err(anyhow!("report {} must be a JSON object", path.display())),
    }
}

fn validate_shape_match(
    baseline: &Map<String, Value>,
    candidate: &Map<String, Value>,
) -> anyhow::Result<()> {
    for key in SHAPE_KEYS {
        let baseline_value = baseline.get(*key);
        let candidate_value = candidate.get(*key);
        if baseline_value == candidate_value {
            continue;
        }
        if baseline_value.is_none() && candidate_value.is_none() {
            continue;
        }
        bail!(
            "metadata mismatch: {key} baseline={} candidate={}",
            display_value(baseline_value),
            display_value(candidate_value)
        );
    }
    Ok(())
}

fn number_field(report: &Map<String, Value>, key: &str) -> anyhow::Result<f64> {
    report
        .get(key)
        .and_then(Value::as_f64)
        .ok_or_else(|| anyhow!("report missing numeric field `{key}`"))
}

fn string_field(report: &Map<String, Value>, key: &str) -> Option<String> {
    report.get(key).and_then(Value::as_str).map(str::to_string)
}

fn ratio(baseline: f64, candidate: f64, field: &str) -> anyhow::Result<f64> {
    if baseline < 0.0 || candidate < 0.0 {
        bail!("{field} must be non-negative");
    }
    if baseline == 0.0 {
        if candidate == 0.0 {
            return Ok(1.0);
        }
        bail!("baseline {field} cannot be zero when candidate is non-zero");
    }
    Ok(candidate / baseline)
}

fn display_value(value: Option<&Value>) -> String {
    value
        .map(Value::to_string)
        .unwrap_or_else(|| "<missing>".to_string())
}

fn failure_message(reason: &str, summary: &CompareSummary) -> String {
    let mut lines = vec![
        format!("{reason}"),
        "result=fail".to_string(),
        "within_noise_budget=false".to_string(),
        format!("throughput_ratio={:.6}", summary.throughput_ratio),
        format!("p99_ratio={:.6}", summary.p99_ratio),
        format!("min_throughput_ratio={:.6}", summary.min_throughput_ratio),
        format!("max_p99_ratio={:.6}", summary.max_p99_ratio),
    ];
    push_optional_line(&mut lines, "baseline_git_sha", &summary.baseline_git_sha);
    push_optional_line(&mut lines, "candidate_git_sha", &summary.candidate_git_sha);
    push_optional_line(
        &mut lines,
        "baseline_replay_id",
        &summary.baseline_replay_id,
    );
    push_optional_line(
        &mut lines,
        "candidate_replay_id",
        &summary.candidate_replay_id,
    );
    lines.join("\n")
}

fn print_summary(result: &str, summary: &CompareSummary) {
    println!("mode=compare");
    println!("result={result}");
    println!("within_noise_budget=true");
    println!("throughput_ratio={:.6}", summary.throughput_ratio);
    println!("p99_ratio={:.6}", summary.p99_ratio);
    println!("min_throughput_ratio={:.6}", summary.min_throughput_ratio);
    println!("max_p99_ratio={:.6}", summary.max_p99_ratio);
    push_optional_print("baseline_git_sha", &summary.baseline_git_sha);
    push_optional_print("candidate_git_sha", &summary.candidate_git_sha);
    push_optional_print("baseline_replay_id", &summary.baseline_replay_id);
    push_optional_print("candidate_replay_id", &summary.candidate_replay_id);
}

fn push_optional_print(key: &str, value: &Option<String>) {
    if let Some(value) = value {
        println!("{key}={value}");
    }
}

fn push_optional_line(lines: &mut Vec<String>, key: &str, value: &Option<String>) {
    if let Some(value) = value {
        lines.push(format!("{key}={value}"));
    }
}
