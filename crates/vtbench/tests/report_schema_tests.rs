mod common;

use std::fs;

use common::{assert_success, find_single_run_dir, path_string, read_json, run_vtbench, temp_path};

#[test]
fn report_schema_includes_canonical_metadata() {
    let temp = temp_path("report-schema");
    let artifacts_root = temp.join("runs");
    let report_file = temp.join("standalone").join("report.json");
    let output = run_vtbench(&[
        "storage-ingest".to_string(),
        "--rows=20".to_string(),
        "--batch-size=5".to_string(),
        format!("--artifacts-root={}", path_string(&artifacts_root)),
        format!("--report-file={}", path_string(&report_file)),
    ]);
    assert_success(&output);

    let run_dir = find_single_run_dir(&artifacts_root);
    let manifest = read_json(&run_dir.join("manifest.json"));
    let report = read_json(&run_dir.join("report.json"));
    let standalone = read_json(&report_file);

    assert_eq!(report["report_version"], 1);
    assert_eq!(report["mode"], "storage-ingest");
    assert_eq!(report["operations"], 20);
    assert_eq!(report["errors"], 0);
    assert!(report["replay_id"].as_str().unwrap().contains("storage-ingest"));
    assert!(!report["git_sha"].as_str().unwrap().is_empty());
    assert!(report["started_at_unix_ms"].as_u64().unwrap() > 0);
    assert!(report["ended_at_unix_ms"].as_u64().unwrap() >= report["started_at_unix_ms"].as_u64().unwrap());
    assert!(report["duration_ms"].as_f64().unwrap() >= 0.0);
    assert_eq!(report["options"]["rows"], 20);
    assert_eq!(report["options"]["batch_size"], 5);
    assert_eq!(report["metadata"]["batch_size"], "5");
    assert_eq!(manifest["files"]["report"], "report.json");
    assert_eq!(manifest["replay_id"], report["replay_id"]);
    assert_eq!(standalone, report);

    let stdout_log = fs::read_to_string(run_dir.join("stdout.log")).expect("stdout log");
    assert!(stdout_log.contains("mode=storage-ingest"));
    assert!(stdout_log.contains("operations=20"));
}
