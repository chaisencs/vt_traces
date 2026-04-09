use std::{
    collections::BTreeMap,
    fs::{self, File},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::Duration,
};

use anyhow::{anyhow, bail, Context};
use reqwest::Client;
use serde_json::{json, Map, Value};
use tokio::time::{sleep, Instant};

use crate::{parse_options_with_defaults, run_otlp_protobuf_load, temp_bench_dir, BenchOptions};

const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_OFFICIAL_PORT: u16 = 13081;
const DEFAULT_MEMORY_PORT: u16 = 13082;
const DEFAULT_DISK_PORT: u16 = 13083;
const DEFAULT_ROUNDS: usize = 1;
const DEFAULT_STARTUP_TIMEOUT_SECS: u64 = 30;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum BenchmarkTarget {
    Official,
    Memory,
    Disk,
}

impl BenchmarkTarget {
    fn slug(self) -> &'static str {
        match self {
            Self::Official => "official",
            Self::Memory => "memory",
            Self::Disk => "disk",
        }
    }

    fn requires_rust_binary(self) -> bool {
        !matches!(self, Self::Official)
    }

    fn bench_path(self) -> &'static str {
        match self {
            Self::Official => "/insert/opentelemetry/v1/traces",
            Self::Memory | Self::Disk => "/v1/traces",
        }
    }

    fn readiness_path(self) -> &'static str {
        match self {
            Self::Official => "/metrics",
            Self::Memory | Self::Disk => "/readyz",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OfficialCompareOptions {
    pub(crate) official_bin: PathBuf,
    pub(crate) rust_bin: Option<PathBuf>,
    pub(crate) output_dir: PathBuf,
    pub(crate) rounds: usize,
    pub(crate) host: String,
    pub(crate) official_port: u16,
    pub(crate) memory_port: u16,
    pub(crate) disk_port: u16,
    pub(crate) startup_timeout_secs: u64,
    pub(crate) targets: Vec<BenchmarkTarget>,
    pub(crate) bench: BenchOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PlannedRun {
    pub(crate) round: usize,
    pub(crate) target: BenchmarkTarget,
    pub(crate) bench_url: String,
    pub(crate) readiness_url: String,
    pub(crate) report_file: PathBuf,
    pub(crate) stdout_log: PathBuf,
    pub(crate) stderr_log: PathBuf,
    pub(crate) data_dir: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct TargetSummary {
    target: BenchmarkTarget,
    summary_file: PathBuf,
    throughput_ops_per_sec: f64,
    latency_p99_ms: f64,
}

pub(crate) fn parse_official_compare_options(
    args: impl Iterator<Item = String>,
) -> anyhow::Result<OfficialCompareOptions> {
    let mut official_bin = None;
    let mut rust_bin = None;
    let mut output_dir = Some(temp_bench_dir("official-compare"));
    let mut rounds = DEFAULT_ROUNDS;
    let mut host = DEFAULT_HOST.to_string();
    let mut official_port = DEFAULT_OFFICIAL_PORT;
    let mut memory_port = DEFAULT_MEMORY_PORT;
    let mut disk_port = DEFAULT_DISK_PORT;
    let mut startup_timeout_secs = DEFAULT_STARTUP_TIMEOUT_SECS;
    let mut targets = vec![BenchmarkTarget::Official, BenchmarkTarget::Disk];
    let mut bench_args = Vec::new();

    for arg in args {
        let (key, value) = arg
            .strip_prefix("--")
            .and_then(|value| value.split_once('='))
            .ok_or_else(|| anyhow!("invalid argument: {arg}"))?;
        match key {
            "official-bin" => official_bin = Some(PathBuf::from(value)),
            "rust-bin" => rust_bin = Some(PathBuf::from(value)),
            "output-dir" => output_dir = Some(PathBuf::from(value)),
            "rounds" => rounds = value.parse()?,
            "host" => host = value.to_string(),
            "official-port" => official_port = value.parse()?,
            "memory-port" => memory_port = value.parse()?,
            "disk-port" => disk_port = value.parse()?,
            "startup-timeout-secs" => startup_timeout_secs = value.parse()?,
            "targets" => targets = parse_targets(value)?,
            "url" | "report-file" => {
                bail!("official-compare manages `{key}` automatically; remove --{key}")
            }
            _ => bench_args.push(arg),
        }
    }

    if rounds == 0 {
        bail!("--rounds must be greater than zero");
    }
    if startup_timeout_secs == 0 {
        bail!("--startup-timeout-secs must be greater than zero");
    }
    if !targets.contains(&BenchmarkTarget::Official) {
        bail!("--targets must include `official` for same-shape comparison");
    }
    if targets.iter().any(|target| target.requires_rust_binary()) && rust_bin.is_none() {
        bail!("--rust-bin is required when targets include `memory` or `disk`");
    }

    let mut bench_defaults = BenchOptions::default();
    bench_defaults.duration_secs = Some(5);
    bench_defaults.warmup_secs = Some(1);
    let bench = parse_options_with_defaults(bench_args.into_iter(), bench_defaults)?;
    if bench.url.is_some() || bench.report_file.is_some() {
        bail!("official-compare owns benchmark urls and report paths");
    }

    Ok(OfficialCompareOptions {
        official_bin: official_bin.ok_or_else(|| anyhow!("--official-bin is required"))?,
        rust_bin,
        output_dir: output_dir.expect("output_dir default should always exist"),
        rounds,
        host,
        official_port,
        memory_port,
        disk_port,
        startup_timeout_secs,
        targets,
        bench,
    })
}

pub(crate) fn build_run_plans(options: &OfficialCompareOptions) -> Vec<PlannedRun> {
    let mut plans = Vec::new();
    for round in 1..=options.rounds {
        for target in &options.targets {
            let round_dir = options.output_dir.join(format!("round-{round:02}"));
            let target_slug = target.slug();
            let port = target_port(options, *target);
            let base_url = format!("http://{}:{port}", options.host);
            plans.push(PlannedRun {
                round,
                target: *target,
                bench_url: format!("{base_url}{}", target.bench_path()),
                readiness_url: format!("{base_url}{}", target.readiness_path()),
                report_file: round_dir.join(format!("{target_slug}.json")),
                stdout_log: round_dir.join(format!("{target_slug}.stdout.log")),
                stderr_log: round_dir.join(format!("{target_slug}.stderr.log")),
                data_dir: match target {
                    BenchmarkTarget::Official | BenchmarkTarget::Disk => {
                        Some(round_dir.join(format!("{target_slug}-data")))
                    }
                    BenchmarkTarget::Memory => None,
                },
            });
        }
    }
    plans
}

pub(crate) async fn run_official_compare(options: OfficialCompareOptions) -> anyhow::Result<()> {
    fs::create_dir_all(&options.output_dir)
        .with_context(|| format!("create {}", options.output_dir.display()))?;
    let client = Client::builder()
        .no_proxy()
        .build()
        .context("build reqwest client for official-compare")?;
    let plans = build_run_plans(&options);
    write_manifest(&options, &plans)?;

    let mut report_files_by_target: BTreeMap<BenchmarkTarget, Vec<PathBuf>> = BTreeMap::new();
    for plan in &plans {
        println!(
            "official_compare_round={} target={} url={} report_file={}",
            plan.round,
            plan.target.slug(),
            plan.bench_url,
            plan.report_file.display()
        );

        let mut child = spawn_target_process(&options, plan)?;
        let run_result = run_single_plan(&client, &options, plan).await;
        let stop_result = stop_child(&mut child, plan.target.slug());

        run_result?;
        stop_result?;
        report_files_by_target
            .entry(plan.target)
            .or_default()
            .push(plan.report_file.clone());
    }

    let summaries = write_target_summaries(&options.output_dir, &report_files_by_target)?;
    write_comparison_summary(&options.output_dir, &summaries)?;
    print_summary(&summaries);
    Ok(())
}

async fn run_single_plan(
    client: &Client,
    options: &OfficialCompareOptions,
    plan: &PlannedRun,
) -> anyhow::Result<()> {
    wait_for_ready(
        client,
        &plan.readiness_url,
        Duration::from_secs(options.startup_timeout_secs),
    )
    .await?;
    let mut bench = options.bench.clone();
    bench.url = Some(plan.bench_url.clone());
    bench.report_file = Some(plan.report_file.clone());
    run_otlp_protobuf_load(bench).await
}

fn spawn_target_process(
    options: &OfficialCompareOptions,
    plan: &PlannedRun,
) -> anyhow::Result<Child> {
    if let Some(parent) = plan.report_file.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    if let Some(data_dir) = &plan.data_dir {
        recreate_dir(data_dir)?;
    }

    let stdout = File::create(&plan.stdout_log)
        .with_context(|| format!("create {}", plan.stdout_log.display()))?;
    let stderr = File::create(&plan.stderr_log)
        .with_context(|| format!("create {}", plan.stderr_log.display()))?;

    let mut command = match plan.target {
        BenchmarkTarget::Official => {
            let data_dir = plan
                .data_dir
                .as_ref()
                .expect("official target must have a data dir");
            let mut command = Command::new(&options.official_bin);
            command.args([
                "-loggerLevel=ERROR",
                &format!("-httpListenAddr={}:{}", options.host, options.official_port),
                &format!("-storageDataPath={}", data_dir.display()),
            ]);
            command
        }
        BenchmarkTarget::Memory => {
            let rust_bin = options
                .rust_bin
                .as_ref()
                .ok_or_else(|| anyhow!("memory target requires --rust-bin"))?;
            let mut command = Command::new(rust_bin);
            command
                .env("RUST_LOG", "error")
                .env(
                    "VT_BIND_ADDR",
                    format!("{}:{}", options.host, options.memory_port),
                )
                .env("VT_STORAGE_MODE", "memory")
                .env_remove("VT_STORAGE_PATH");
            command
        }
        BenchmarkTarget::Disk => {
            let rust_bin = options
                .rust_bin
                .as_ref()
                .ok_or_else(|| anyhow!("disk target requires --rust-bin"))?;
            let data_dir = plan
                .data_dir
                .as_ref()
                .expect("disk target must have a data dir");
            let mut command = Command::new(rust_bin);
            command
                .env("RUST_LOG", "error")
                .env(
                    "VT_BIND_ADDR",
                    format!("{}:{}", options.host, options.disk_port),
                )
                .env("VT_STORAGE_MODE", "disk")
                .env("VT_STORAGE_PATH", data_dir)
                .env("VT_TRACE_INGEST_PROFILE", "throughput")
                .env("VT_STORAGE_SYNC_POLICY", "none");
            command
        }
    };

    command
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));

    command.spawn().with_context(|| {
        format!(
            "spawn {} benchmark target",
            match plan.target {
                BenchmarkTarget::Official => options.official_bin.display().to_string(),
                BenchmarkTarget::Memory | BenchmarkTarget::Disk => options
                    .rust_bin
                    .as_ref()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|| "<missing rust binary>".to_string()),
            }
        )
    })
}

async fn wait_for_ready(client: &Client, url: &str, timeout: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let attempt_error = match client.get(url).send().await {
            Ok(response) if response.status().is_success() => return Ok(()),
            Ok(response) => anyhow!("{url} returned {}", response.status()),
            Err(error) => anyhow::Error::from(error),
        };
        if Instant::now() >= deadline {
            return Err(attempt_error);
        }
        sleep(Duration::from_millis(250)).await;
    }
}

fn stop_child(child: &mut Child, target: &str) -> anyhow::Result<()> {
    if child
        .try_wait()
        .with_context(|| format!("poll {target} child status"))?
        .is_none()
    {
        let _ = child.kill();
    }
    let _ = child.wait();
    Ok(())
}

fn recreate_dir(path: &Path) -> anyhow::Result<()> {
    if path.exists() {
        fs::remove_dir_all(path).with_context(|| format!("remove {}", path.display()))?;
    }
    fs::create_dir_all(path).with_context(|| format!("create {}", path.display()))?;
    Ok(())
}

fn write_manifest(options: &OfficialCompareOptions, plans: &[PlannedRun]) -> anyhow::Result<()> {
    let manifest = json!({
        "mode": "official-compare",
        "official_bin": options.official_bin,
        "rust_bin": options.rust_bin,
        "output_dir": options.output_dir,
        "rounds": options.rounds,
        "host": options.host,
        "official_port": options.official_port,
        "memory_port": options.memory_port,
        "disk_port": options.disk_port,
        "startup_timeout_secs": options.startup_timeout_secs,
        "targets": options.targets.iter().map(|target| target.slug()).collect::<Vec<_>>(),
        "bench": {
            "duration_secs": options.bench.duration_secs,
            "warmup_secs": options.bench.warmup_secs,
            "concurrency": options.bench.concurrency,
            "spans_per_request": options.bench.spans_per_request,
            "payload_variants": options.bench.payload_variants,
        },
        "plans": plans.iter().map(|plan| {
            json!({
                "round": plan.round,
                "target": plan.target.slug(),
                "bench_url": plan.bench_url,
                "readiness_url": plan.readiness_url,
                "report_file": plan.report_file,
                "stdout_log": plan.stdout_log,
                "stderr_log": plan.stderr_log,
                "data_dir": plan.data_dir,
            })
        }).collect::<Vec<_>>(),
    });
    let manifest_path = options.output_dir.join("manifest.json");
    fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).context("serialize official-compare manifest")?,
    )
    .with_context(|| format!("write {}", manifest_path.display()))
}

fn write_target_summaries(
    output_dir: &Path,
    report_files_by_target: &BTreeMap<BenchmarkTarget, Vec<PathBuf>>,
) -> anyhow::Result<Vec<TargetSummary>> {
    let summary_dir = output_dir.join("summaries");
    fs::create_dir_all(&summary_dir)
        .with_context(|| format!("create {}", summary_dir.display()))?;

    let mut summaries = Vec::new();
    for (target, files) in report_files_by_target {
        let reports = files
            .iter()
            .map(|path| read_report(path))
            .collect::<anyhow::Result<Vec<_>>>()?;
        let first = reports
            .first()
            .cloned()
            .ok_or_else(|| anyhow!("missing reports for target {}", target.slug()))?;
        let throughput_ops_per_sec = median_numeric_field(&reports, "ops_per_sec")?;
        let latency_p99_ms = median_numeric_field(&reports, "latency_p99_ms")?;

        let mut summary = first;
        summary.insert("aggregation".to_string(), json!("median"));
        summary.insert("rounds".to_string(), json!(reports.len()));
        summary.insert("benchmark_target".to_string(), json!(target.slug()));
        summary.insert("ops_per_sec".to_string(), json!(throughput_ops_per_sec));
        summary.insert("latency_p99_ms".to_string(), json!(latency_p99_ms));
        for key in [
            "latency_p50_ms",
            "latency_p95_ms",
            "latency_p999_ms",
            "latency_max_ms",
            "elapsed_ms",
            "operations",
            "errors",
        ] {
            if let Ok(value) = median_numeric_field(&reports, key) {
                summary.insert(key.to_string(), json!(value));
            }
        }

        let summary_file = summary_dir.join(format!("{}-median.json", target.slug()));
        fs::write(
            &summary_file,
            serde_json::to_vec_pretty(&Value::Object(summary))
                .context("serialize target median summary")?,
        )
        .with_context(|| format!("write {}", summary_file.display()))?;
        summaries.push(TargetSummary {
            target: *target,
            summary_file,
            throughput_ops_per_sec,
            latency_p99_ms,
        });
    }
    Ok(summaries)
}

fn write_comparison_summary(output_dir: &Path, summaries: &[TargetSummary]) -> anyhow::Result<()> {
    let by_target = summaries
        .iter()
        .map(|summary| (summary.target, summary))
        .collect::<BTreeMap<_, _>>();
    let official = by_target
        .get(&BenchmarkTarget::Official)
        .ok_or_else(|| anyhow!("official summary missing"))?;

    let comparisons = [BenchmarkTarget::Disk, BenchmarkTarget::Memory]
        .into_iter()
        .filter_map(|target| by_target.get(&target).copied())
        .map(|candidate| {
            json!({
                "baseline": "official",
                "candidate": candidate.target.slug(),
                "throughput_ratio": candidate.throughput_ops_per_sec / official.throughput_ops_per_sec.max(f64::EPSILON),
                "p99_ratio": candidate.latency_p99_ms / official.latency_p99_ms.max(f64::EPSILON),
                "baseline_summary_file": official.summary_file,
                "candidate_summary_file": candidate.summary_file,
            })
        })
        .collect::<Vec<_>>();

    let summary = json!({
        "mode": "official-compare",
        "targets": summaries.iter().map(|summary| {
            json!({
                "target": summary.target.slug(),
                "summary_file": summary.summary_file,
                "ops_per_sec": summary.throughput_ops_per_sec,
                "latency_p99_ms": summary.latency_p99_ms,
            })
        }).collect::<Vec<_>>(),
        "comparisons": comparisons,
    });
    let summary_path = output_dir.join("summary.json");
    fs::write(
        &summary_path,
        serde_json::to_vec_pretty(&summary).context("serialize official-compare summary")?,
    )
    .with_context(|| format!("write {}", summary_path.display()))
}

fn print_summary(summaries: &[TargetSummary]) {
    println!("mode=official-compare");
    for summary in summaries {
        println!(
            "target={} median_ops_per_sec={:.3} median_latency_p99_ms={:.3} summary_file={}",
            summary.target.slug(),
            summary.throughput_ops_per_sec,
            summary.latency_p99_ms,
            summary.summary_file.display()
        );
    }
    let by_target = summaries
        .iter()
        .map(|summary| (summary.target, summary))
        .collect::<BTreeMap<_, _>>();
    if let Some(official) = by_target.get(&BenchmarkTarget::Official) {
        for target in [BenchmarkTarget::Disk, BenchmarkTarget::Memory] {
            if let Some(candidate) = by_target.get(&target) {
                println!(
                    "baseline=official candidate={} throughput_ratio={:.6} p99_ratio={:.6}",
                    target.slug(),
                    candidate.throughput_ops_per_sec
                        / official.throughput_ops_per_sec.max(f64::EPSILON),
                    candidate.latency_p99_ms / official.latency_p99_ms.max(f64::EPSILON)
                );
            }
        }
    }
}

fn read_report(path: &Path) -> anyhow::Result<Map<String, Value>> {
    let payload = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let value: Value =
        serde_json::from_slice(&payload).with_context(|| format!("parse {}", path.display()))?;
    match value {
        Value::Object(object) => Ok(object),
        _ => bail!("report {} must contain a JSON object", path.display()),
    }
}

fn median_numeric_field(reports: &[Map<String, Value>], key: &str) -> anyhow::Result<f64> {
    let mut values = reports
        .iter()
        .map(|report| {
            report
                .get(key)
                .and_then(Value::as_f64)
                .ok_or_else(|| anyhow!("report missing numeric field `{key}`"))
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    values.sort_by(f64::total_cmp);
    let midpoint = values.len() / 2;
    if values.len() % 2 == 1 {
        Ok(values[midpoint])
    } else {
        Ok((values[midpoint - 1] + values[midpoint]) / 2.0)
    }
}

fn parse_targets(value: &str) -> anyhow::Result<Vec<BenchmarkTarget>> {
    let mut targets = Vec::new();
    for raw in value.split(',') {
        let target = match raw.trim() {
            "official" => BenchmarkTarget::Official,
            "memory" => BenchmarkTarget::Memory,
            "disk" => BenchmarkTarget::Disk,
            other => bail!("unknown official-compare target `{other}`"),
        };
        if !targets.contains(&target) {
            targets.push(target);
        }
    }
    if targets.is_empty() {
        bail!("--targets cannot be empty");
    }
    Ok(targets)
}

fn target_port(options: &OfficialCompareOptions, target: BenchmarkTarget) -> u16 {
    match target {
        BenchmarkTarget::Official => options.official_port,
        BenchmarkTarget::Memory => options.memory_port,
        BenchmarkTarget::Disk => options.disk_port,
    }
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        build_run_plans, parse_official_compare_options, BenchmarkTarget, OfficialCompareOptions,
    };
    use crate::BenchOptions;

    #[test]
    fn parse_official_compare_options_reads_harness_and_bench_knobs() {
        let options = parse_official_compare_options(
            [
                "--official-bin=/tmp/victoria-traces-prod".to_string(),
                "--rust-bin=/tmp/vtapi".to_string(),
                "--output-dir=/tmp/official-bench".to_string(),
                "--rounds=5".to_string(),
                "--targets=official,disk,memory".to_string(),
                "--official-port=13091".to_string(),
                "--memory-port=13092".to_string(),
                "--disk-port=13093".to_string(),
                "--duration-secs=7".to_string(),
                "--warmup-secs=2".to_string(),
                "--concurrency=48".to_string(),
            ]
            .into_iter(),
        )
        .expect("official compare options should parse");

        assert_eq!(
            options.official_bin,
            PathBuf::from("/tmp/victoria-traces-prod")
        );
        assert_eq!(options.rust_bin, Some(PathBuf::from("/tmp/vtapi")));
        assert_eq!(options.output_dir, PathBuf::from("/tmp/official-bench"));
        assert_eq!(options.rounds, 5);
        assert_eq!(
            options.targets,
            vec![
                BenchmarkTarget::Official,
                BenchmarkTarget::Disk,
                BenchmarkTarget::Memory,
            ]
        );
        assert_eq!(options.official_port, 13091);
        assert_eq!(options.memory_port, 13092);
        assert_eq!(options.disk_port, 13093);
        assert_eq!(options.bench.duration_secs, Some(7));
        assert_eq!(options.bench.warmup_secs, Some(2));
        assert_eq!(options.bench.concurrency, 48);
    }

    #[test]
    fn parse_official_compare_options_rejects_manual_url_override() {
        let error = parse_official_compare_options(
            [
                "--official-bin=/tmp/victoria-traces-prod".to_string(),
                "--url=http://127.0.0.1:13081/v1/traces".to_string(),
            ]
            .into_iter(),
        )
        .expect_err("official compare should own benchmark urls");

        assert!(error.to_string().contains("--url"));
    }

    #[test]
    fn build_run_plans_uses_expected_urls_and_report_paths() {
        let options = OfficialCompareOptions {
            official_bin: PathBuf::from("/tmp/victoria-traces-prod"),
            rust_bin: Some(PathBuf::from("/tmp/vtapi")),
            output_dir: PathBuf::from("/tmp/official-bench"),
            rounds: 2,
            host: "127.0.0.1".to_string(),
            official_port: 13081,
            memory_port: 13082,
            disk_port: 13083,
            startup_timeout_secs: 30,
            targets: vec![
                BenchmarkTarget::Official,
                BenchmarkTarget::Disk,
                BenchmarkTarget::Memory,
            ],
            bench: BenchOptions::default(),
        };

        let plans = build_run_plans(&options);
        assert_eq!(plans.len(), 6);

        assert_eq!(
            plans[0].bench_url,
            "http://127.0.0.1:13081/insert/opentelemetry/v1/traces"
        );
        assert_eq!(plans[1].bench_url, "http://127.0.0.1:13083/v1/traces");
        assert_eq!(plans[2].bench_url, "http://127.0.0.1:13082/v1/traces");
        assert_eq!(
            plans[0].report_file,
            Path::new("/tmp/official-bench/round-01/official.json")
        );
        assert_eq!(
            plans[4].report_file,
            Path::new("/tmp/official-bench/round-02/disk.json")
        );
    }
}
