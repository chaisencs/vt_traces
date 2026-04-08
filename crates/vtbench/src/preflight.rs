use std::{env, process::Command};

use anyhow::{anyhow, bail, Context};
use reqwest::{Client, Url};

pub(crate) const ARCH_MISMATCH_OVERRIDE_ENV: &str = "VTBENCH_ALLOW_ARCH_MISMATCH";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExecutionContext {
    host_is_apple_silicon: bool,
    binary_target_arch: &'static str,
    binary_target_os: &'static str,
}

pub(crate) fn validate_benchmark_preflight(mode: &str) -> anyhow::Result<()> {
    if !is_benchmark_mode(mode) || arch_mismatch_override_enabled() {
        return Ok(());
    }

    validate_binary_arch_guard(mode, detect_execution_context()?)
}

pub(crate) async fn validate_otlp_benchmark_target(
    client: &Client,
    url: &str,
) -> anyhow::Result<()> {
    if arch_mismatch_override_enabled() {
        return Ok(());
    }

    let context = detect_execution_context()?;
    if !requires_native_apple_silicon_benchmark_binary(context) {
        return Ok(());
    }

    let metrics_url = match local_metrics_url(url) {
        Ok(Some(metrics_url)) => metrics_url,
        Ok(None) => return Ok(()),
        Err(error) => return Err(error),
    };
    let response = match client.get(metrics_url.clone()).send().await {
        Ok(response) => response,
        Err(_) => return Ok(()),
    };
    if !response.status().is_success() {
        return Ok(());
    }
    let body = response.text().await.unwrap_or_default();
    if let Some(target_arch) = rust_vt_target_arch_from_metrics(&body) {
        if target_arch == "x86_64" {
            bail!(
                "vtbench otlp-protobuf-load is pointing at a local Rust vtapi built for x86_64 on an Apple Silicon host. \
Rebuild and rerun with `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`, then use \
`target/aarch64-apple-darwin/release/vtapi` and `target/aarch64-apple-darwin/release/vtbench`. \
Set {ARCH_MISMATCH_OVERRIDE_ENV}=1 only if you intentionally want Rosetta benchmark numbers."
            );
        }
    }
    Ok(())
}

fn validate_binary_arch_guard(mode: &str, context: ExecutionContext) -> anyhow::Result<()> {
    if requires_native_apple_silicon_benchmark_binary(context) {
        bail!(
            "vtbench benchmark mode `{mode}` is running as an x86_64 binary on an Apple Silicon host. \
This silently skews throughput and latency results. Rebuild and rerun with \
`cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`, then use \
`target/aarch64-apple-darwin/release/vtbench ...`. Set {ARCH_MISMATCH_OVERRIDE_ENV}=1 only if you intentionally want Rosetta benchmark numbers."
        );
    }
    Ok(())
}

fn detect_execution_context() -> anyhow::Result<ExecutionContext> {
    Ok(ExecutionContext {
        host_is_apple_silicon: host_is_apple_silicon()?,
        binary_target_arch: env::consts::ARCH,
        binary_target_os: env::consts::OS,
    })
}

fn requires_native_apple_silicon_benchmark_binary(context: ExecutionContext) -> bool {
    context.binary_target_os == "macos"
        && context.host_is_apple_silicon
        && context.binary_target_arch == "x86_64"
}

fn host_is_apple_silicon() -> anyhow::Result<bool> {
    if env::consts::OS != "macos" {
        return Ok(false);
    }

    let output = Command::new("sysctl")
        .args(["-in", "hw.optional.arm64"])
        .output()
        .context("run `sysctl -in hw.optional.arm64`")?;
    if !output.status.success() {
        return Err(anyhow!(
            "`sysctl -in hw.optional.arm64` exited with status {}",
            output.status
        ));
    }
    let stdout = String::from_utf8(output.stdout).context("decode sysctl output")?;
    Ok(stdout.trim() == "1")
}

fn is_benchmark_mode(mode: &str) -> bool {
    matches!(
        mode,
        "storage-ingest"
            | "storage-query"
            | "http-ingest"
            | "otlp-protobuf-load"
            | "disk-trace-block-append"
    )
}

fn arch_mismatch_override_enabled() -> bool {
    env::var_os(ARCH_MISMATCH_OVERRIDE_ENV)
        .and_then(|value| value.into_string().ok())
        .map(|value| {
            let normalized = value.trim().to_ascii_lowercase();
            !normalized.is_empty() && normalized != "0" && normalized != "false"
        })
        .unwrap_or(false)
}

fn local_metrics_url(url: &str) -> anyhow::Result<Option<Url>> {
    let mut url = Url::parse(url).with_context(|| format!("parse benchmark url `{url}`"))?;
    let Some(host) = url.host_str() else {
        return Ok(None);
    };
    if !matches!(host, "127.0.0.1" | "localhost" | "::1") {
        return Ok(None);
    }
    url.set_path("/metrics");
    url.set_query(None);
    url.set_fragment(None);
    Ok(Some(url))
}

fn rust_vt_target_arch_from_metrics(metrics: &str) -> Option<&str> {
    const MARKER: &str = "target_arch=\"";

    metrics.lines().find_map(|line| {
        if !line.starts_with("vt_build_info{") {
            return None;
        }
        let start = line.find(MARKER)? + MARKER.len();
        let rest = &line[start..];
        let end = rest.find('"')?;
        Some(&rest[..end])
    })
}

#[cfg(test)]
mod tests {
    use super::{
        local_metrics_url, rust_vt_target_arch_from_metrics, validate_binary_arch_guard,
        ExecutionContext,
    };

    #[test]
    fn rejects_x86_benchmark_binary_on_apple_silicon() {
        let error = validate_binary_arch_guard(
            "storage-ingest",
            ExecutionContext {
                host_is_apple_silicon: true,
                binary_target_arch: "x86_64",
                binary_target_os: "macos",
            },
        )
        .expect_err("x86 benchmark binary should be rejected");

        let message = error.to_string();
        assert!(message.contains("aarch64-apple-darwin"));
        assert!(message.contains("VTBENCH_ALLOW_ARCH_MISMATCH"));
    }

    #[test]
    fn allows_native_benchmark_binary_on_apple_silicon() {
        validate_binary_arch_guard(
            "storage-ingest",
            ExecutionContext {
                host_is_apple_silicon: true,
                binary_target_arch: "aarch64",
                binary_target_os: "macos",
            },
        )
        .expect("native Apple Silicon binary should be allowed");
    }

    #[test]
    fn extracts_target_arch_from_vt_metrics() {
        let metrics = r#"# TYPE vt_build_info gauge
vt_build_info{version="0.1.0",target_arch="x86_64",target_os="macos"} 1
"#;

        assert_eq!(rust_vt_target_arch_from_metrics(metrics), Some("x86_64"));
    }

    #[test]
    fn rewrites_local_ingest_url_to_metrics() {
        let url = local_metrics_url("http://127.0.0.1:13083/v1/traces?foo=bar")
            .expect("url should parse")
            .expect("local url should produce metrics endpoint");

        assert_eq!(url.as_str(), "http://127.0.0.1:13083/metrics");
    }
}
