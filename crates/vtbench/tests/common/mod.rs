#![allow(dead_code)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};

use serde_json::Value;

pub fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("repo root")
}

pub fn temp_path(label: &str) -> PathBuf {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_millis();
    let path = std::env::temp_dir().join(format!("vtbench-test-{label}-{millis}-{}", std::process::id()));
    fs::create_dir_all(&path).expect("create temp path");
    path
}

pub fn run_vtbench(args: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_vtbench"))
        .args(args)
        .current_dir(repo_root())
        .output()
        .expect("run vtbench")
}

pub fn assert_success(output: &Output) {
    assert!(
        output.status.success(),
        "stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

pub fn assert_failure(output: &Output, needle: &str) {
    assert!(
        !output.status.success(),
        "expected failure, stdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains(needle),
        "stderr missing {needle:?}:\n{stderr}"
    );
}

pub fn find_single_run_dir(root: &Path) -> PathBuf {
    let mut entries = fs::read_dir(root)
        .expect("read artifacts root")
        .map(|entry| entry.expect("dir entry").path())
        .collect::<Vec<_>>();
    entries.sort();
    assert_eq!(entries.len(), 1, "expected one run dir under {}", root.display());
    entries.remove(0)
}

pub fn read_json(path: &Path) -> Value {
    serde_json::from_slice(&fs::read(path).expect("read json")).expect("parse json")
}

pub fn path_string(path: &Path) -> String {
    path.display().to_string()
}

pub fn write_json(path: &Path, value: &Value) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).expect("create parent");
    }
    fs::write(path, serde_json::to_vec_pretty(value).expect("json"))
        .expect("write json");
}
