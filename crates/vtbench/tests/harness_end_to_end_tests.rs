mod common;

use common::{assert_success, find_single_run_dir, path_string, read_json, run_vtbench, temp_path};

#[test]
fn http_ingest_bundle_matches_expected_counts() {
    let temp = temp_path("http-e2e");
    let artifacts_root = temp.join("runs");
    let output = run_vtbench(&[
        "http-ingest".to_string(),
        "--requests=6".to_string(),
        "--spans-per-request=2".to_string(),
        "--concurrency=2".to_string(),
        format!("--artifacts-root={}", path_string(&artifacts_root)),
    ]);
    assert_success(&output);

    let run_dir = find_single_run_dir(&artifacts_root);
    let report = read_json(&run_dir.join("report.json"));
    let commands = read_json(&run_dir.join("commands.json"));

    assert_eq!(report["mode"], "http-ingest");
    assert_eq!(report["operations"], 12);
    assert_eq!(report["errors"], 0);
    assert_eq!(report["metadata"]["requests"], "6");
    assert_eq!(report["metadata"]["spans_per_request"], "2");
    assert_eq!(commands[0]["argv"][0], "http-ingest");
}

#[test]
fn fault_injection_run_records_error_counts() {
    let temp = temp_path("fault-e2e");
    let artifacts_root = temp.join("runs");
    let output = run_vtbench(&[
        "http-ingest".to_string(),
        "--requests=32".to_string(),
        "--spans-per-request=1".to_string(),
        "--concurrency=8".to_string(),
        "--warmup-secs=0".to_string(),
        "--fault-after-secs=0".to_string(),
        "--fault-duration-secs=60".to_string(),
        format!("--artifacts-root={}", path_string(&artifacts_root)),
    ]);
    assert_success(&output);

    let run_dir = find_single_run_dir(&artifacts_root);
    let report = read_json(&run_dir.join("report.json"));

    assert_eq!(report["mode"], "http-ingest");
    assert!(report["errors"].as_u64().unwrap() > 0);
}
