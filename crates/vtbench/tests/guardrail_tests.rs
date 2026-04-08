use std::process::Command;

#[test]
fn benchmark_modes_reject_x86_binary_on_apple_silicon_hosts() {
    if !cfg!(target_os = "macos") || !cfg!(target_arch = "x86_64") || !host_is_apple_silicon() {
        eprintln!("skipping arch mismatch guardrail test on this host/target");
        return;
    }

    let output = Command::new(env!("CARGO_BIN_EXE_vtbench"))
        .args(["storage-ingest", "--rows=1", "--batch-size=1"])
        .output()
        .expect("run vtbench");

    assert!(
        !output.status.success(),
        "expected vtbench to reject x86_64 benchmark binary on Apple Silicon host, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("aarch64-apple-darwin"),
        "expected guidance about native Apple Silicon target, stderr={stderr}"
    );
}

#[test]
fn arch_mismatch_override_allows_benchmark_run() {
    if !cfg!(target_os = "macos") || !cfg!(target_arch = "x86_64") || !host_is_apple_silicon() {
        eprintln!("skipping arch mismatch override test on this host/target");
        return;
    }

    let output = Command::new(env!("CARGO_BIN_EXE_vtbench"))
        .env("VTBENCH_ALLOW_ARCH_MISMATCH", "1")
        .args(["storage-ingest", "--rows=1", "--batch-size=1"])
        .output()
        .expect("run vtbench with override");

    assert!(
        output.status.success(),
        "expected override to allow benchmark run, stdout={}, stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn host_is_apple_silicon() -> bool {
    Command::new("sysctl")
        .args(["-in", "hw.optional.arm64"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim() == "1")
        .unwrap_or(false)
}
