mod common;

use std::fs;

use common::{assert_success, find_single_run_dir, path_string, read_json, run_vtbench, temp_path};

#[test]
fn replay_bundle_emits_required_artifacts() {
    let temp = temp_path("replay-bundle");
    let artifacts_root = temp.join("runs");
    let output = run_vtbench(&[
        "storage-query".to_string(),
        "--traces=8".to_string(),
        "--spans-per-trace=2".to_string(),
        "--queries=5".to_string(),
        format!("--artifacts-root={}", path_string(&artifacts_root)),
    ]);
    assert_success(&output);

    let run_dir = find_single_run_dir(&artifacts_root);
    for name in [
        "manifest.json",
        "report.json",
        "env.json",
        "context.json",
        "commands.json",
        "decision.json",
        "rerun.sh",
        "stdout.log",
        "stderr.log",
        "metrics.txt",
    ] {
        assert!(run_dir.join(name).exists(), "missing {name}");
    }

    let env_json = read_json(&run_dir.join("env.json"));
    let commands = read_json(&run_dir.join("commands.json"));
    let context = read_json(&run_dir.join("context.json"));
    let decision = read_json(&run_dir.join("decision.json"));
    let rerun_script = fs::read_to_string(run_dir.join("rerun.sh")).expect("rerun script");

    assert!(!env_json["git_sha"].as_str().unwrap().is_empty());
    assert_eq!(commands[0]["tool"], "vtbench");
    assert_eq!(context["mode"], "storage-query");
    assert_eq!(decision["status"], "success");
    assert!(rerun_script.contains("vtbench storage-query"));
}
