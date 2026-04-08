use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::{json, Value};

#[test]
fn compare_accepts_runs_within_noise_budget() {
    let temp_dir = temp_test_dir("compare-pass");
    let baseline = write_report(
        &temp_dir,
        "baseline.json",
        base_report(100.0, 1.0, "baseline-sha"),
    );
    let candidate = write_report(
        &temp_dir,
        "candidate.json",
        report_with_overrides(
            base_report(100.0, 1.0, "baseline-sha"),
            &[
                ("ops_per_sec", json!(98.0)),
                ("latency_p99_ms", json!(1.05)),
                ("git_sha", json!("candidate-sha")),
                ("replay_id", json!("incident-123")),
            ],
        ),
    );

    let output = run_compare(&baseline, &candidate, "0.97", "1.10");

    assert!(
        output.status.success(),
        "expected compare to pass, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("result=pass"));
    assert!(stdout.contains("within_noise_budget=true"));
    assert!(stdout.contains("throughput_ratio=0.980000"));
    assert!(stdout.contains("p99_ratio=1.050000"));
    assert!(stdout.contains("candidate_replay_id=incident-123"));
}

#[test]
fn compare_rejects_throughput_regression_past_tolerance() {
    let temp_dir = temp_test_dir("compare-throughput-fail");
    let baseline = write_report(
        &temp_dir,
        "baseline.json",
        base_report(100.0, 1.0, "baseline-sha"),
    );
    let candidate = write_report(
        &temp_dir,
        "candidate.json",
        report_with_overrides(
            base_report(100.0, 1.0, "baseline-sha"),
            &[
                ("ops_per_sec", json!(94.0)),
                ("git_sha", json!("candidate-sha")),
            ],
        ),
    );

    let output = run_compare(&baseline, &candidate, "0.97", "1.10");

    assert!(
        !output.status.success(),
        "expected compare to fail, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("throughput regression"));
    assert!(stderr.contains("throughput_ratio=0.940000"));
}

#[test]
fn compare_rejects_p99_regression_past_tolerance() {
    let temp_dir = temp_test_dir("compare-p99-fail");
    let baseline = write_report(
        &temp_dir,
        "baseline.json",
        base_report(100.0, 1.0, "baseline-sha"),
    );
    let candidate = write_report(
        &temp_dir,
        "candidate.json",
        report_with_overrides(
            base_report(100.0, 1.0, "baseline-sha"),
            &[
                ("latency_p99_ms", json!(1.25)),
                ("git_sha", json!("candidate-sha")),
            ],
        ),
    );

    let output = run_compare(&baseline, &candidate, "0.97", "1.10");

    assert!(
        !output.status.success(),
        "expected compare to fail, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("p99 regression"));
    assert!(stderr.contains("p99_ratio=1.250000"));
}

#[test]
fn compare_rejects_benchmark_shape_mismatch() {
    let temp_dir = temp_test_dir("compare-shape-fail");
    let baseline = write_report(
        &temp_dir,
        "baseline.json",
        base_report(100.0, 1.0, "baseline-sha"),
    );
    let candidate = write_report(
        &temp_dir,
        "candidate.json",
        report_with_overrides(
            base_report(100.0, 1.0, "baseline-sha"),
            &[
                ("concurrency", json!("16")),
                ("git_sha", json!("candidate-sha")),
            ],
        ),
    );

    let output = run_compare(&baseline, &candidate, "0.97", "1.10");

    assert!(
        !output.status.success(),
        "expected compare to fail, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("metadata mismatch"));
    assert!(stderr.contains("concurrency"));
}

#[test]
fn compare_ignores_completed_request_count_drift() {
    let temp_dir = temp_test_dir("compare-requests-ignored");
    let baseline = write_report(
        &temp_dir,
        "baseline.json",
        report_with_overrides(
            base_report(100.0, 1.0, "baseline-sha"),
            &[("requests", json!("249344"))],
        ),
    );
    let candidate = write_report(
        &temp_dir,
        "candidate.json",
        report_with_overrides(
            base_report(100.0, 1.0, "candidate-sha"),
            &[
                ("ops_per_sec", json!(99.0)),
                ("latency_p99_ms", json!(1.02)),
                ("requests", json!("233024")),
            ],
        ),
    );

    let output = run_compare(&baseline, &candidate, "0.97", "1.10");

    assert!(
        output.status.success(),
        "expected compare to ignore completed request drift, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn report_file_keeps_git_sha_even_when_run_outside_repo_cwd() {
    let temp_dir = temp_test_dir("report-git-sha");
    let report = temp_dir.join("report.json");
    fs::create_dir_all(&temp_dir).expect("create temp cwd");

    let output = Command::new(env!("CARGO_BIN_EXE_vtbench"))
        .current_dir(&temp_dir)
        .env("VTBENCH_ALLOW_ARCH_MISMATCH", "1")
        .args([
            "storage-ingest",
            "--rows=1",
            "--batch-size=1",
            &format!("--report-file={}", report.display()),
        ])
        .output()
        .expect("run vtbench outside repo cwd");

    assert!(
        output.status.success(),
        "expected report generation to succeed, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let report: Value =
        serde_json::from_slice(&fs::read(&report).expect("read report")).expect("parse report");
    let git_sha = report
        .get("git_sha")
        .and_then(Value::as_str)
        .unwrap_or_default();
    assert!(
        !git_sha.is_empty(),
        "expected git_sha in report even outside repo cwd, report={report}"
    );
}

fn run_compare(
    baseline: &Path,
    candidate: &Path,
    min_throughput_ratio: &str,
    max_p99_ratio: &str,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_vtbench"))
        .args([
            "compare",
            &format!("--baseline-file={}", baseline.display()),
            &format!("--candidate-file={}", candidate.display()),
            &format!("--min-throughput-ratio={min_throughput_ratio}"),
            &format!("--max-p99-ratio={max_p99_ratio}"),
        ])
        .output()
        .expect("run vtbench compare")
}

fn base_report(ops_per_sec: f64, latency_p99_ms: f64, git_sha: &str) -> Value {
    json!({
        "report_schema_version": 1,
        "mode": "otlp-protobuf-load",
        "duration_secs": 5,
        "warmup_secs": 1,
        "concurrency": "32",
        "spans_per_request": "5",
        "payload_variants": "1024",
        "binary_target_arch": "aarch64",
        "binary_target_os": "macos",
        "ops_per_sec": ops_per_sec,
        "latency_p99_ms": latency_p99_ms,
        "git_sha": git_sha
    })
}

fn report_with_overrides(base: Value, overrides: &[(&str, Value)]) -> Value {
    let mut object = match base {
        Value::Object(object) => object,
        _ => panic!("base report must be object"),
    };
    for (key, value) in overrides {
        object.insert((*key).to_string(), value.clone());
    }
    Value::Object(object)
}

fn write_report(temp_dir: &Path, name: &str, report: Value) -> PathBuf {
    fs::create_dir_all(temp_dir).expect("create temp dir");
    let path = temp_dir.join(name);
    let payload = serde_json::to_vec_pretty(&report).expect("encode report");
    fs::write(&path, payload).expect("write report");
    path
}

fn temp_test_dir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!("vtbench-{name}-{unique}"))
}
