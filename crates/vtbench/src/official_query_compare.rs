use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context};
use bytes::Bytes;
use reqwest::{Client, Url};
use serde_json::{json, Map, Value};
use tokio::time::{sleep, Instant};
use vtingest::{
    encode_export_trace_service_request_protobuf, AttributeValue, ExportTraceServiceRequest,
    KeyValue, ResourceSpans, ScopeSpans, SpanRecord, Status,
};

use crate::{
    parse_options_with_defaults, preflight::validate_otlp_benchmark_target,
    print_summary_and_report, temp_bench_dir, BenchOptions, LatencySummary, TimelineSummary,
};

const DEFAULT_ROUNDS: usize = 1;
const DEFAULT_STARTUP_TIMEOUT_SECS: u64 = 30;
const DEFAULT_HOST: &str = "127.0.0.1";
const DEFAULT_OFFICIAL_PORT: u16 = 13081;
const DEFAULT_DISK_PORT: u16 = 13083;
const DEFAULT_FIXTURE_REQUESTS: usize = 20_000;
const DEFAULT_QUERY_LIMIT: usize = 20;
const QUERY_FIXTURE_REQUEST_SPACING_NANOS: i64 = 1_000_000;
const QUERY_FIXTURE_SPAN_SPACING_NANOS: i64 = 25_000;
const QUERY_FIXTURE_LOOKBACK_NANOS: i64 = 5 * 60 * 1_000_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum QueryBenchmarkTarget {
    Official,
    Disk,
}

impl QueryBenchmarkTarget {
    fn slug(self) -> &'static str {
        match self {
            Self::Official => "official",
            Self::Disk => "disk",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct OfficialQueryCompareOptions {
    pub(crate) official_bin: PathBuf,
    pub(crate) rust_bin: PathBuf,
    pub(crate) output_dir: PathBuf,
    pub(crate) rounds: usize,
    pub(crate) host: String,
    pub(crate) official_port: u16,
    pub(crate) disk_port: u16,
    pub(crate) startup_timeout_secs: u64,
    pub(crate) fixture_requests: usize,
    pub(crate) targets: Vec<QueryBenchmarkTarget>,
    pub(crate) bench: BenchOptions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct QueryRunPlan {
    pub(crate) round: usize,
    pub(crate) target: QueryBenchmarkTarget,
    pub(crate) ingest_url: String,
    pub(crate) readiness_url: String,
    pub(crate) report_file: PathBuf,
    pub(crate) stdout_log: PathBuf,
    pub(crate) stderr_log: PathBuf,
    pub(crate) data_dir: PathBuf,
}

#[derive(Debug, Clone, Copy)]
struct QueryCase {
    slug: &'static str,
    service: &'static str,
    operation: &'static str,
    http_method: &'static str,
    http_route: &'static str,
}

const QUERY_CASES: [QueryCase; 4] = [
    QueryCase {
        slug: "checkout-get",
        service: "checkout",
        operation: "GET /checkout",
        http_method: "GET",
        http_route: "/checkout",
    },
    QueryCase {
        slug: "checkout-post",
        service: "checkout",
        operation: "POST /checkout",
        http_method: "POST",
        http_route: "/checkout",
    },
    QueryCase {
        slug: "payments-post",
        service: "payments",
        operation: "POST /pay",
        http_method: "POST",
        http_route: "/pay",
    },
    QueryCase {
        slug: "inventory-get",
        service: "inventory",
        operation: "GET /stock",
        http_method: "GET",
        http_route: "/stock",
    },
];

#[derive(Debug, Clone)]
struct TargetSummary {
    target: QueryBenchmarkTarget,
    summary_file: PathBuf,
    throughput_ops_per_sec: f64,
    latency_p99_ms: f64,
}

pub(crate) fn parse_official_query_compare_options(
    args: impl Iterator<Item = String>,
) -> anyhow::Result<OfficialQueryCompareOptions> {
    let mut official_bin = None;
    let mut rust_bin = None;
    let mut output_dir = Some(temp_bench_dir("official-query-compare"));
    let mut rounds = DEFAULT_ROUNDS;
    let mut host = DEFAULT_HOST.to_string();
    let mut official_port = DEFAULT_OFFICIAL_PORT;
    let mut disk_port = DEFAULT_DISK_PORT;
    let mut startup_timeout_secs = DEFAULT_STARTUP_TIMEOUT_SECS;
    let mut fixture_requests = DEFAULT_FIXTURE_REQUESTS;
    let mut targets = vec![QueryBenchmarkTarget::Official, QueryBenchmarkTarget::Disk];
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
            "disk-port" => disk_port = value.parse()?,
            "startup-timeout-secs" => startup_timeout_secs = value.parse()?,
            "fixture-requests" => fixture_requests = value.parse()?,
            "targets" => targets = parse_targets(value)?,
            "url" | "report-file" => {
                bail!("official-query-compare manages `{key}` automatically; remove --{key}")
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
    if fixture_requests == 0 {
        bail!("--fixture-requests must be greater than zero");
    }

    let mut bench_defaults = BenchOptions::default();
    bench_defaults.duration_secs = Some(5);
    bench_defaults.warmup_secs = Some(1);
    let bench = parse_options_with_defaults(bench_args.into_iter(), bench_defaults)?;
    if bench.url.is_some() || bench.report_file.is_some() {
        bail!("official-query-compare owns benchmark urls and report paths");
    }

    Ok(OfficialQueryCompareOptions {
        official_bin: official_bin.ok_or_else(|| anyhow!("--official-bin is required"))?,
        rust_bin: rust_bin.ok_or_else(|| anyhow!("--rust-bin is required"))?,
        output_dir: output_dir.expect("output_dir default should exist"),
        rounds,
        host,
        official_port,
        disk_port,
        startup_timeout_secs,
        fixture_requests,
        targets,
        bench,
    })
}

pub(crate) fn build_query_run_plans(options: &OfficialQueryCompareOptions) -> Vec<QueryRunPlan> {
    let mut plans = Vec::new();
    for round in 1..=options.rounds {
        for target in &options.targets {
            let round_dir = options.output_dir.join(format!("round-{round:02}"));
            let target_slug = target.slug();
            let port = target_port(options, *target);
            let base_url = format!("http://{}:{port}", options.host);
            plans.push(QueryRunPlan {
                round,
                target: *target,
                ingest_url: format!("{base_url}/insert/opentelemetry/v1/traces"),
                readiness_url: readiness_url(target, &base_url),
                report_file: round_dir.join(format!("{target_slug}.json")),
                stdout_log: round_dir.join(format!("{target_slug}.stdout.log")),
                stderr_log: round_dir.join(format!("{target_slug}.stderr.log")),
                data_dir: round_dir.join(format!("{target_slug}-data")),
            });
        }
    }
    plans
}

pub(crate) async fn run_official_query_compare(
    options: OfficialQueryCompareOptions,
) -> anyhow::Result<()> {
    fs::create_dir_all(&options.output_dir)
        .with_context(|| format!("create {}", options.output_dir.display()))?;
    let client = Client::builder()
        .no_proxy()
        .build()
        .context("build reqwest client for official-query-compare")?;
    let plans = build_query_run_plans(&options);
    let fixture_base_start_nanos = current_unix_nanos()?.saturating_sub(QUERY_FIXTURE_LOOKBACK_NANOS);
    write_manifest(&options, &plans, fixture_base_start_nanos)?;

    let mut report_files_by_target: BTreeMap<QueryBenchmarkTarget, Vec<PathBuf>> = BTreeMap::new();
    for plan in &plans {
        println!(
            "official_query_compare_round={} target={} ingest_url={} report_file={}",
            plan.round,
            plan.target.slug(),
            plan.ingest_url,
            plan.report_file.display()
        );

        let mut prep_child = spawn_target_process(&options, plan)?;
        let prep_result =
            run_fixture_stage(&client, &options, plan, fixture_base_start_nanos).await;
        let prep_stop_result = stop_child(&mut prep_child, plan.target.slug());
        prep_result?;
        prep_stop_result?;

        let mut bench_child = spawn_target_process(&options, plan)?;
        let bench_result = run_query_benchmark_stage(
            &client,
            &options,
            plan,
            fixture_base_start_nanos,
        )
        .await;
        let bench_stop_result = stop_child(&mut bench_child, plan.target.slug());
        bench_result?;
        bench_stop_result?;

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

async fn run_fixture_stage(
    client: &Client,
    options: &OfficialQueryCompareOptions,
    plan: &QueryRunPlan,
    fixture_base_start_nanos: i64,
) -> anyhow::Result<()> {
    wait_for_ready(
        client,
        &plan.readiness_url,
        Duration::from_secs(options.startup_timeout_secs),
    )
    .await?;
    validate_otlp_benchmark_target(client, &plan.ingest_url).await?;
    let payloads = make_query_fixture_payload_variants(
        options.bench.payload_variants.max(1),
        options.bench.spans_per_request.max(1),
        fixture_base_start_nanos,
    )?;
    ingest_query_fixture(
        client,
        &plan.ingest_url,
        options.fixture_requests,
        options.bench.concurrency.max(1),
        payloads,
    )
    .await?;
    if matches!(plan.target, QueryBenchmarkTarget::Official) {
        force_flush(client, &format!("http://{}:{}/internal/force_flush", options.host, options.official_port)).await?;
        sleep(Duration::from_millis(500)).await;
    }
    Ok(())
}

async fn run_query_benchmark_stage(
    client: &Client,
    options: &OfficialQueryCompareOptions,
    plan: &QueryRunPlan,
    fixture_base_start_nanos: i64,
) -> anyhow::Result<()> {
    wait_for_ready(
        client,
        &plan.readiness_url,
        Duration::from_secs(options.startup_timeout_secs),
    )
    .await?;
    let port = target_port(options, plan.target);
    let base_url = format!("http://{}:{port}", options.host);
    let query_urls = build_query_urls(
        &base_url,
        fixture_base_start_nanos,
        options.fixture_requests,
    )?;
    run_jaeger_query_load(
        client,
        &options.bench,
        &plan.report_file,
        query_urls,
        options.fixture_requests,
    )
    .await
}

fn spawn_target_process(
    options: &OfficialQueryCompareOptions,
    plan: &QueryRunPlan,
) -> anyhow::Result<Child> {
    if let Some(parent) = plan.report_file.parent() {
        fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    }
    if !plan.data_dir.exists() {
        fs::create_dir_all(&plan.data_dir)
            .with_context(|| format!("create {}", plan.data_dir.display()))?;
    }

    let stdout = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&plan.stdout_log)
        .with_context(|| format!("open {}", plan.stdout_log.display()))?;
    let stderr = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&plan.stderr_log)
        .with_context(|| format!("open {}", plan.stderr_log.display()))?;

    let mut command = match plan.target {
        QueryBenchmarkTarget::Official => {
            let mut command = Command::new(&options.official_bin);
            command.args([
                "-loggerLevel=ERROR",
                &format!("-httpListenAddr={}:{}", options.host, options.official_port),
                &format!("-storageDataPath={}", plan.data_dir.display()),
                "-insert.indexFlushInterval=1s",
                "-search.latencyOffset=0s",
            ]);
            command
        }
        QueryBenchmarkTarget::Disk => {
            let mut command = Command::new(&options.rust_bin);
            command
                .env("RUST_LOG", "error")
                .env(
                    "VT_BIND_ADDR",
                    format!("{}:{}", options.host, options.disk_port),
                )
                .env("VT_STORAGE_MODE", "disk")
                .env("VT_STORAGE_PATH", &plan.data_dir)
                .env("VT_TRACE_INGEST_PROFILE", "throughput")
                .env("VT_STORAGE_SYNC_POLICY", "none");
            command
        }
    };

    command
        .stdout(Stdio::from(File::from(stdout)))
        .stderr(Stdio::from(File::from(stderr)));

    command.spawn().with_context(|| {
        format!(
            "spawn {} query benchmark target",
            match plan.target {
                QueryBenchmarkTarget::Official => options.official_bin.display().to_string(),
                QueryBenchmarkTarget::Disk => options.rust_bin.display().to_string(),
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

async fn ingest_query_fixture(
    client: &Client,
    ingest_url: &str,
    fixture_requests: usize,
    concurrency: usize,
    payloads: Vec<Bytes>,
) -> anyhow::Result<()> {
    let next_request = Arc::new(AtomicUsize::new(0));
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let ingest_url = ingest_url.to_string();
        let next_request = next_request.clone();
        let payloads = payloads.clone();
        workers.push(tokio::spawn(async move {
            loop {
                let request_index = next_request.fetch_add(1, Ordering::Relaxed);
                if request_index >= fixture_requests {
                    break Ok::<(), anyhow::Error>(());
                }

                let payload = payloads[request_index % payloads.len()].clone();
                let response = client
                    .post(ingest_url.clone())
                    .header("content-type", "application/x-protobuf")
                    .body(payload)
                    .send()
                    .await
                    .with_context(|| format!("POST {ingest_url}"))?;
                if !response.status().is_success() {
                    bail!(
                        "fixture ingest request {} returned {}",
                        request_index,
                        response.status()
                    );
                }
                let _ = response.bytes().await;
            }
        }));
    }

    for worker in workers {
        worker.await.context("join fixture ingest worker")??;
    }
    Ok(())
}

async fn force_flush(client: &Client, url: &str) -> anyhow::Result<()> {
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if !response.status().is_success() {
        bail!("force flush returned {}", response.status());
    }
    let _ = response.bytes().await;
    Ok(())
}

async fn run_jaeger_query_load(
    client: &Client,
    bench: &BenchOptions,
    report_file: &Path,
    query_urls: Vec<String>,
    fixture_requests: usize,
) -> anyhow::Result<()> {
    let query_count = bench.queries.max(1);
    let concurrency = bench.concurrency.max(1);
    let started = Instant::now();
    let deadline = bench
        .duration_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let latencies = Arc::new(Mutex::new(LatencySummary::default()));
    let timeline = Arc::new(Mutex::new(TimelineSummary::new(sample_interval(bench))));
    let completed_queries = Arc::new(AtomicUsize::new(0));
    let next_query = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let warmup_deadline = bench
        .warmup_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let measured_started = Arc::new(Mutex::new(None::<Instant>));

    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let query_urls = query_urls.clone();
        let latencies = latencies.clone();
        let timeline = timeline.clone();
        let completed_queries = completed_queries.clone();
        let next_query = next_query.clone();
        let error_count = error_count.clone();
        let measured_started = measured_started.clone();
        let deadline = deadline;
        let warmup_deadline = warmup_deadline;
        workers.push(tokio::spawn(async move {
            loop {
                let query_index = next_query.fetch_add(1, Ordering::Relaxed);
                if deadline.is_none() && query_index >= query_count {
                    break;
                }
                if let Some(deadline) = deadline {
                    if Instant::now() >= deadline {
                        break;
                    }
                }

                let query_url = query_urls[query_index % query_urls.len()].clone();
                let request_started = Instant::now();
                let response = client.get(query_url.clone()).send().await;
                let success = match response {
                    Ok(response) => {
                        let success = response.status().is_success();
                        let _ = response.bytes().await;
                        success
                    }
                    Err(_) => false,
                };
                let latency = request_started.elapsed();
                completed_queries.fetch_add(1, Ordering::Relaxed);

                if warmup_deadline
                    .map(|deadline| Instant::now() >= deadline)
                    .unwrap_or(true)
                {
                    let measured_origin = {
                        let mut guard = measured_started.lock().expect("measured start lock");
                        *guard.get_or_insert_with(Instant::now)
                    };
                    if success {
                        latencies.lock().expect("latency lock").record(latency);
                    } else {
                        error_count.fetch_add(1, Ordering::Relaxed);
                    }
                    timeline.lock().expect("timeline lock").record(
                        measured_origin.elapsed(),
                        latency,
                        1,
                        success,
                    );
                }
            }
        }));
    }

    for worker in workers {
        worker.await.expect("join query worker");
    }

    let elapsed = measured_started
        .lock()
        .expect("measured start lock")
        .map(|started| started.elapsed())
        .unwrap_or_else(|| started.elapsed());
    let completed_queries = completed_queries.load(Ordering::Relaxed);
    let latencies = latencies.lock().expect("latency lock");
    let timeline = timeline.lock().expect("timeline lock");

    let mut report_options = bench.clone();
    report_options.report_file = Some(report_file.to_path_buf());
    print_summary_and_report(
        &report_options,
        "jaeger-query-load",
        completed_queries,
        elapsed,
        &[
            ("queries".to_string().as_str(), completed_queries.to_string()),
            ("concurrency".to_string().as_str(), concurrency.to_string()),
            ("fixture_requests".to_string().as_str(), fixture_requests.to_string()),
            ("query_cases".to_string().as_str(), QUERY_CASES.len().to_string()),
            ("query_limit".to_string().as_str(), DEFAULT_QUERY_LIMIT.to_string()),
        ],
        Some(&latencies),
        &timeline,
        error_count.load(Ordering::Relaxed),
    )
}

fn make_query_fixture_payload_variants(
    payload_variants: usize,
    spans_per_request: usize,
    fixture_base_start_nanos: i64,
) -> anyhow::Result<Vec<Bytes>> {
    (0..payload_variants.max(1))
        .map(|request_index| {
            make_query_fixture_payload(
                request_index,
                spans_per_request,
                fixture_base_start_nanos,
            )
        })
        .collect()
}

fn make_query_fixture_payload(
    request_index: usize,
    spans_per_request: usize,
    fixture_base_start_nanos: i64,
) -> anyhow::Result<Bytes> {
    let case = QUERY_CASES[request_index % QUERY_CASES.len()];
    let trace_id = format!("{:032x}", request_index + 1);
    let base_start = fixture_base_start_nanos
        + (request_index as i64) * QUERY_FIXTURE_REQUEST_SPACING_NANOS;
    let mut spans = Vec::with_capacity(spans_per_request);
    for span_offset in 0..spans_per_request {
        let start_time_unix_nano = base_start + (span_offset as i64) * QUERY_FIXTURE_SPAN_SPACING_NANOS;
        let end_time_unix_nano = start_time_unix_nano + 80_000 + (span_offset as i64) * 5_000;
        spans.push(SpanRecord {
            trace_id: trace_id.clone(),
            span_id: format!("{:016x}", request_index * spans_per_request + span_offset + 1),
            parent_span_id: None,
            name: case.operation.to_string(),
            start_time_unix_nano,
            end_time_unix_nano,
            attributes: vec![
                KeyValue::new(
                    "http.method",
                    AttributeValue::String(case.http_method.to_string()),
                ),
                KeyValue::new(
                    "http.route",
                    AttributeValue::String(case.http_route.to_string()),
                ),
                KeyValue::new(
                    "benchmark.case",
                    AttributeValue::String(case.slug.to_string()),
                ),
            ],
            status: Some(Status {
                code: 0,
                message: "OK".to_string(),
            }),
        });
    }
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String(case.service.to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("vtbench.query".to_string()),
                scope_version: Some("0.1.0".to_string()),
                scope_attributes: Vec::new(),
                spans,
            }],
        }],
    };
    Ok(Bytes::from(encode_export_trace_service_request_protobuf(
        &request,
    )?))
}

fn build_query_urls(
    base_url: &str,
    fixture_base_start_nanos: i64,
    fixture_requests: usize,
) -> anyhow::Result<Vec<String>> {
    let start_us = (fixture_base_start_nanos / 1_000).saturating_sub(5_000_000);
    let end_us = fixture_base_start_nanos / 1_000
        + (fixture_requests as i64) * (QUERY_FIXTURE_REQUEST_SPACING_NANOS / 1_000)
        + 5_000_000;
    QUERY_CASES
        .iter()
        .map(|case| build_query_url(base_url, *case, start_us, end_us))
        .collect()
}

fn build_query_url(
    base_url: &str,
    case: QueryCase,
    start_us: i64,
    end_us: i64,
) -> anyhow::Result<String> {
    let mut url = Url::parse(&format!("{base_url}/select/jaeger/api/traces"))
        .with_context(|| format!("parse base url `{base_url}`"))?;
    let tags = serde_json::to_string(&json!({
        "http.method": case.http_method,
        "benchmark.case": case.slug,
    }))
    .context("serialize jaeger query tags")?;
    url.query_pairs_mut()
        .append_pair("service", case.service)
        .append_pair("operation", case.operation)
        .append_pair("tags", &tags)
        .append_pair("start", &start_us.to_string())
        .append_pair("end", &end_us.to_string())
        .append_pair("limit", &DEFAULT_QUERY_LIMIT.to_string());
    Ok(url.into())
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

fn write_manifest(
    options: &OfficialQueryCompareOptions,
    plans: &[QueryRunPlan],
    fixture_base_start_nanos: i64,
) -> anyhow::Result<()> {
    let query_urls = build_query_urls(
        "http://127.0.0.1:0",
        fixture_base_start_nanos,
        options.fixture_requests,
    )?;
    let manifest = json!({
        "mode": "official-query-compare",
        "official_bin": options.official_bin,
        "rust_bin": options.rust_bin,
        "output_dir": options.output_dir,
        "rounds": options.rounds,
        "host": options.host,
        "official_port": options.official_port,
        "disk_port": options.disk_port,
        "startup_timeout_secs": options.startup_timeout_secs,
        "fixture_requests": options.fixture_requests,
        "fixture_base_start_nanos": fixture_base_start_nanos,
        "targets": options.targets.iter().map(|target| target.slug()).collect::<Vec<_>>(),
        "bench": {
            "queries": options.bench.queries,
            "duration_secs": options.bench.duration_secs,
            "warmup_secs": options.bench.warmup_secs,
            "concurrency": options.bench.concurrency,
            "spans_per_request": options.bench.spans_per_request,
            "payload_variants": options.bench.payload_variants,
        },
        "query_cases": query_urls,
        "plans": plans.iter().map(|plan| {
            json!({
                "round": plan.round,
                "target": plan.target.slug(),
                "ingest_url": plan.ingest_url,
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
        serde_json::to_vec_pretty(&manifest)
            .context("serialize official-query-compare manifest")?,
    )
    .with_context(|| format!("write {}", manifest_path.display()))
}

fn write_target_summaries(
    output_dir: &Path,
    report_files_by_target: &BTreeMap<QueryBenchmarkTarget, Vec<PathBuf>>,
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
        .get(&QueryBenchmarkTarget::Official)
        .ok_or_else(|| anyhow!("official summary missing"))?;
    let disk = by_target
        .get(&QueryBenchmarkTarget::Disk)
        .ok_or_else(|| anyhow!("disk summary missing"))?;

    let summary = json!({
        "mode": "official-query-compare",
        "targets": summaries.iter().map(|summary| {
            json!({
                "target": summary.target.slug(),
                "summary_file": summary.summary_file,
                "ops_per_sec": summary.throughput_ops_per_sec,
                "latency_p99_ms": summary.latency_p99_ms,
            })
        }).collect::<Vec<_>>(),
        "comparison": {
            "baseline": "official",
            "candidate": "disk",
            "throughput_ratio": disk.throughput_ops_per_sec / official.throughput_ops_per_sec.max(f64::EPSILON),
            "p99_ratio": disk.latency_p99_ms / official.latency_p99_ms.max(f64::EPSILON),
            "baseline_summary_file": official.summary_file,
            "candidate_summary_file": disk.summary_file,
        }
    });
    let summary_path = output_dir.join("summary.json");
    fs::write(
        &summary_path,
        serde_json::to_vec_pretty(&summary).context("serialize official-query-compare summary")?,
    )
    .with_context(|| format!("write {}", summary_path.display()))
}

fn print_summary(summaries: &[TargetSummary]) {
    println!("mode=official-query-compare");
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
    if let (Some(official), Some(disk)) = (
        by_target.get(&QueryBenchmarkTarget::Official),
        by_target.get(&QueryBenchmarkTarget::Disk),
    ) {
        println!(
            "baseline=official candidate=disk throughput_ratio={:.6} p99_ratio={:.6}",
            disk.throughput_ops_per_sec / official.throughput_ops_per_sec.max(f64::EPSILON),
            disk.latency_p99_ms / official.latency_p99_ms.max(f64::EPSILON),
        );
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

fn parse_targets(value: &str) -> anyhow::Result<Vec<QueryBenchmarkTarget>> {
    let mut targets = Vec::new();
    for raw in value.split(',') {
        let target = match raw.trim() {
            "official" => QueryBenchmarkTarget::Official,
            "disk" => QueryBenchmarkTarget::Disk,
            other => bail!("unknown official-query-compare target `{other}`"),
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

fn target_port(options: &OfficialQueryCompareOptions, target: QueryBenchmarkTarget) -> u16 {
    match target {
        QueryBenchmarkTarget::Official => options.official_port,
        QueryBenchmarkTarget::Disk => options.disk_port,
    }
}

fn readiness_url(target: &QueryBenchmarkTarget, base_url: &str) -> String {
    match target {
        QueryBenchmarkTarget::Official => format!("{base_url}/metrics"),
        QueryBenchmarkTarget::Disk => format!("{base_url}/readyz"),
    }
}

fn sample_interval(options: &BenchOptions) -> Option<Duration> {
    options
        .sample_interval_secs
        .or(options.duration_secs.map(|_| 1))
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

fn current_unix_nanos() -> anyhow::Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before unix epoch")?
        .as_nanos()
        .try_into()
        .context("unix nanos exceed i64")?)
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{
        build_query_run_plans, build_query_url, parse_official_query_compare_options,
        OfficialQueryCompareOptions, QueryBenchmarkTarget, QueryCase,
    };
    use crate::BenchOptions;

    #[test]
    fn parse_official_query_compare_options_reads_fixture_and_query_knobs() {
        let options = parse_official_query_compare_options(
            [
                "--official-bin=/tmp/victoria-traces-prod".to_string(),
                "--rust-bin=/tmp/vtapi".to_string(),
                "--output-dir=/tmp/official-query-bench".to_string(),
                "--rounds=4".to_string(),
                "--fixture-requests=4096".to_string(),
                "--targets=official,disk".to_string(),
                "--official-port=13101".to_string(),
                "--disk-port=13103".to_string(),
                "--duration-secs=9".to_string(),
                "--warmup-secs=2".to_string(),
                "--concurrency=24".to_string(),
                "--spans-per-request=6".to_string(),
            ]
            .into_iter(),
        )
        .expect("official query compare options should parse");

        assert_eq!(options.official_bin, PathBuf::from("/tmp/victoria-traces-prod"));
        assert_eq!(options.rust_bin, PathBuf::from("/tmp/vtapi"));
        assert_eq!(options.output_dir, PathBuf::from("/tmp/official-query-bench"));
        assert_eq!(options.rounds, 4);
        assert_eq!(options.fixture_requests, 4096);
        assert_eq!(options.official_port, 13101);
        assert_eq!(options.disk_port, 13103);
        assert_eq!(
            options.targets,
            vec![QueryBenchmarkTarget::Official, QueryBenchmarkTarget::Disk]
        );
        assert_eq!(options.bench.duration_secs, Some(9));
        assert_eq!(options.bench.warmup_secs, Some(2));
        assert_eq!(options.bench.concurrency, 24);
        assert_eq!(options.bench.spans_per_request, 6);
    }

    #[test]
    fn build_query_run_plans_uses_query_reports_and_data_dirs() {
        let options = OfficialQueryCompareOptions {
            official_bin: PathBuf::from("/tmp/victoria-traces-prod"),
            rust_bin: PathBuf::from("/tmp/vtapi"),
            output_dir: PathBuf::from("/tmp/official-query-bench"),
            rounds: 2,
            host: "127.0.0.1".to_string(),
            official_port: 13081,
            disk_port: 13083,
            startup_timeout_secs: 30,
            fixture_requests: 2048,
            targets: vec![QueryBenchmarkTarget::Official, QueryBenchmarkTarget::Disk],
            bench: BenchOptions::default(),
        };

        let plans = build_query_run_plans(&options);
        assert_eq!(plans.len(), 4);
        assert_eq!(
            plans[0].ingest_url,
            "http://127.0.0.1:13081/insert/opentelemetry/v1/traces"
        );
        assert_eq!(
            plans[1].readiness_url,
            "http://127.0.0.1:13083/readyz"
        );
        assert_eq!(
            plans[0].report_file,
            Path::new("/tmp/official-query-bench/round-01/official.json")
        );
        assert_eq!(
            plans[3].data_dir,
            Path::new("/tmp/official-query-bench/round-02/disk-data")
        );
    }

    #[test]
    fn build_query_url_serializes_jaeger_search_filters() {
        let url = build_query_url(
            "http://127.0.0.1:13083",
            QueryCase {
                slug: "checkout-get",
                service: "checkout",
                operation: "GET /checkout",
                http_method: "GET",
                http_route: "/checkout",
            },
            0,
            10_000,
        )
        .expect("query url should build");

        assert!(url.contains("/select/jaeger/api/traces?"));
        assert!(url.contains("service=checkout"));
        assert!(url.contains("operation=GET"));
        assert!(url.contains("limit=20"));
        assert!(url.contains("tags="));
    }
}
