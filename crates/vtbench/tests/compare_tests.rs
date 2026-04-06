mod common;

use serde_json::json;

use common::{assert_failure, assert_success, path_string, run_vtbench, temp_path, write_json};

fn sample_report(
    replay_id: &str,
    ops_per_sec: f64,
    p99_ms: f64,
    comparison_arms: &[&str],
) -> serde_json::Value {
    json!({
        "report_version": 1,
        "replay_id": replay_id,
        "mode": "otlp-protobuf-load",
        "git_sha": "deadbeef",
        "command_display": "/tmp/vtbench otlp-protobuf-load --public-report=true",
        "raw_args": ["--public-report=true"],
        "started_at_unix_ms": 1,
        "ended_at_unix_ms": 2,
        "duration_ms": 1.0,
        "operations": 1000,
        "errors": 0,
        "ops_per_sec": ops_per_sec,
        "options": {
            "requests": 100,
            "spans_per_request": 5,
            "warmup_secs": 1,
            "public_report": true,
            "comparison_arms": comparison_arms,
            "trace_shards": 16
        },
        "metadata": {
            "requests": "100",
            "spans_per_request": "5"
        },
        "public_report": true,
        "comparison_arms": comparison_arms,
        "latency": {
            "samples": 100,
            "p50_ms": 0.2,
            "p95_ms": 0.4,
            "p99_ms": p99_ms,
            "p999_ms": 0.8,
            "max_ms": 1.0
        },
        "timeline": []
    })
}

#[test]
fn compare_tests_accept_candidate_within_tolerance() {
    let temp = temp_path("compare-ok");
    let baseline = temp.join("baseline.json");
    let candidate = temp.join("candidate.json");
    write_json(
        &baseline,
        &sample_report("baseline-1", 1000.0, 1.0, &["official", "memory", "disk"]),
    );
    write_json(
        &candidate,
        &sample_report("candidate-1", 980.0, 1.05, &["official", "memory", "disk"]),
    );

    let output = run_vtbench(&[
        "compare-report".to_string(),
        format!("--baseline-file={}", path_string(&baseline)),
        format!("--candidate-file={}", path_string(&candidate)),
        "--throughput-regression-pct=5".to_string(),
        "--p99-regression-pct=10".to_string(),
    ]);

    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("within_noise_budget=true"));
    assert!(stdout.contains("comparison_status=pass"));
}

#[test]
fn compare_tests_reject_metadata_mismatch() {
    let temp = temp_path("compare-mismatch");
    let baseline = temp.join("baseline.json");
    let candidate = temp.join("candidate.json");
    write_json(
        &baseline,
        &sample_report("baseline-1", 1000.0, 1.0, &["official", "memory", "disk"]),
    );
    write_json(
        &candidate,
        &sample_report("candidate-1", 1000.0, 1.0, &["official", "disk"]),
    );

    let output = run_vtbench(&[
        "compare-report".to_string(),
        format!("--baseline-file={}", path_string(&baseline)),
        format!("--candidate-file={}", path_string(&candidate)),
        "--throughput-regression-pct=5".to_string(),
        "--p99-regression-pct=10".to_string(),
    ]);

    assert_failure(&output, "metadata mismatch");
}
