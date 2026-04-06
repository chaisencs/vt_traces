mod common;

use std::{fs, process::Command};

use common::{assert_success, find_single_run_dir, path_string, read_json, run_vtbench, temp_path};
use vtstorage::{DiskStorageEngine, StorageEngine};

#[test]
fn fault_recovery_tests_disk_run_reopens_and_metrics_persist() {
    let temp = temp_path("fault-disk");
    let artifacts_root = temp.join("runs");
    let storage_path = temp.join("storage");
    let output = run_vtbench(&[
        "disk-trace-block-append".to_string(),
        "--requests=2".to_string(),
        "--blocks-per-append=1".to_string(),
        "--spans-per-block=2".to_string(),
        format!("--storage-path={}", path_string(&storage_path)),
        "--clean-start=true".to_string(),
        format!("--artifacts-root={}", path_string(&artifacts_root)),
    ]);
    assert_success(&output);

    let reopened = DiskStorageEngine::open(&storage_path).expect("reopen disk engine");
    assert!(reopened.stats().rows_ingested >= 4);

    let run_dir = find_single_run_dir(&artifacts_root);
    let report = read_json(&run_dir.join("report.json"));
    let metrics = fs::read_to_string(run_dir.join("metrics.txt")).expect("metrics");
    assert_eq!(report["mode"], "disk-trace-block-append");
    assert!(metrics.contains("ops_per_sec="));
}

#[test]
fn fault_recovery_tests_rerun_script_replays_captured_bundle() {
    let temp = temp_path("fault-rerun");
    let artifacts_root = temp.join("runs");
    let output = run_vtbench(&[
        "storage-ingest".to_string(),
        "--rows=12".to_string(),
        "--batch-size=3".to_string(),
        format!("--artifacts-root={}", path_string(&artifacts_root)),
    ]);
    assert_success(&output);

    let run_dir = find_single_run_dir(&artifacts_root);
    let rerun = run_dir.join("rerun.sh");

    let output = Command::new("bash")
        .arg(&rerun)
        .current_dir(&run_dir)
        .output()
        .expect("run rerun script");
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
