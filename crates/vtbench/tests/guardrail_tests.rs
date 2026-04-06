mod common;

use std::fs;

use common::{assert_failure, path_string, run_vtbench, temp_path};

#[test]
fn public_report_requires_full_control_arms() {
    let temp = temp_path("guardrails-public");
    let output = run_vtbench(&[
        "storage-ingest".to_string(),
        "--rows=10".to_string(),
        "--public-report=true".to_string(),
        "--warmup-secs=1".to_string(),
        "--comparison-arms=memory,disk".to_string(),
        format!("--artifacts-root={}", path_string(&temp.join("runs"))),
    ]);

    assert_failure(&output, "comparison-arms");
}

#[test]
fn clean_start_disk_run_rejects_non_empty_storage_path() {
    let temp = temp_path("guardrails-clean-start");
    let storage_path = temp.join("disk-data");
    fs::create_dir_all(&storage_path).expect("create storage path");
    fs::write(storage_path.join("stale.txt"), b"stale").expect("write stale file");

    let output = run_vtbench(&[
        "disk-trace-block-append".to_string(),
        "--requests=1".to_string(),
        "--blocks-per-append=1".to_string(),
        "--spans-per-block=1".to_string(),
        format!("--storage-path={}", path_string(&storage_path)),
        "--clean-start=true".to_string(),
    ]);

    assert_failure(&output, "empty storage path");
}
