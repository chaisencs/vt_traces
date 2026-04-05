use std::{
    collections::BTreeMap,
    env, fs,
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{anyhow, bail, Context};
use axum::{
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use reqwest::Client;
use serde_json::json;
use vtapi::build_router_with_storage_and_limits;
use vtcore::{Field, TraceSearchRequest, TraceSpanRow};
use vtstorage::{BatchingStorageConfig, BatchingStorageEngine, MemoryStorageEngine, StorageEngine};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = env::args().skip(1);
    let mode = args.next().ok_or_else(|| {
        anyhow!("usage: vtbench <storage-ingest|storage-query|http-ingest> [--key=value ...]")
    })?;
    let options = parse_options(args)?;

    match mode.as_str() {
        "storage-ingest" => run_storage_ingest(options),
        "storage-query" => run_storage_query(options),
        "http-ingest" => run_http_ingest(options).await,
        _ => bail!("unknown mode: {mode}"),
    }
}

fn run_storage_ingest(options: BenchOptions) -> anyhow::Result<()> {
    let rows = options.rows;
    let batch_size = options.batch_size.max(1);
    let storage: Arc<dyn StorageEngine> = Arc::new(BatchingStorageEngine::with_config(
        Arc::new(MemoryStorageEngine::new()),
        BatchingStorageConfig::default(),
    ));
    let started = Instant::now();
    let deadline = options
        .duration_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let mut generated = 0usize;
    let mut batches = 0usize;
    let mut latencies = LatencySummary::default();
    let mut timeline = TimelineSummary::new(sample_interval(&options));
    let warmup_deadline = options
        .warmup_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let mut measured_started: Option<Instant> = None;

    while generated < rows
        || deadline
            .map(|deadline| Instant::now() < deadline)
            .unwrap_or(false)
    {
        let current_batch_size = if deadline.is_some() {
            batch_size
        } else {
            batch_size.min(rows - generated)
        };
        let batch = (0..current_batch_size)
            .map(|offset| make_row(generated + offset))
            .collect::<Result<Vec<_>, _>>()?;
        let batch_started = Instant::now();
        storage.append_rows(batch)?;
        if warmup_deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(true)
        {
            if measured_started.is_none() {
                measured_started = Some(Instant::now());
            }
            let latency = batch_started.elapsed();
            latencies.record(latency);
            if let Some(measured_started) = measured_started {
                timeline.record(
                    measured_started.elapsed(),
                    latency,
                    current_batch_size,
                    true,
                );
            }
        }
        generated += current_batch_size;
        batches += 1;
    }

    print_summary_and_report(
        &options,
        "storage-ingest",
        generated,
        measured_started
            .map(|started| started.elapsed())
            .unwrap_or_else(|| started.elapsed()),
        &[
            ("batch_size", batch_size.to_string()),
            ("batches", batches.to_string()),
        ],
        Some(&latencies),
        &timeline,
        0,
    )?;
    Ok(())
}

fn run_storage_query(options: BenchOptions) -> anyhow::Result<()> {
    let traces = options.traces.max(1);
    let spans_per_trace = options.spans_per_trace.max(1);
    let query_count = options.queries.max(1);
    let storage = Arc::new(MemoryStorageEngine::new());

    for trace_index in 0..traces {
        let batch = (0..spans_per_trace)
            .map(|offset| make_trace_row(trace_index, offset))
            .collect::<Result<Vec<_>, _>>()?;
        storage.append_rows(batch)?;
    }

    let started = Instant::now();
    let deadline = options
        .duration_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let mut latencies = LatencySummary::default();
    let mut timeline = TimelineSummary::new(sample_interval(&options));
    let mut completed_queries = 0usize;
    let warmup_deadline = options
        .warmup_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let mut measured_started = None;
    while completed_queries < query_count
        || deadline
            .map(|deadline| Instant::now() < deadline)
            .unwrap_or(false)
    {
        let query_index = completed_queries;
        let trace_id = format!("trace-{:08}", query_index % traces);
        let query_started = Instant::now();
        let window = storage
            .trace_window(&trace_id)
            .with_context(|| format!("missing trace window for {trace_id}"))?;
        let _ = storage.rows_for_trace(&trace_id, window.start_unix_nano, window.end_unix_nano);
        let _ = storage.search_traces(&TraceSearchRequest {
            start_unix_nano: 0,
            end_unix_nano: i64::MAX,
            service_name: Some("checkout".to_string()),
            operation_name: None,
            field_filters: Vec::new(),
            limit: 20,
        });
        if warmup_deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(true)
        {
            if measured_started.is_none() {
                measured_started = Some(Instant::now());
            }
            let latency = query_started.elapsed();
            latencies.record(latency);
            if let Some(measured_started) = measured_started {
                timeline.record(measured_started.elapsed(), latency, 1, true);
            }
        }
        completed_queries += 1;
    }

    print_summary_and_report(
        &options,
        "storage-query",
        completed_queries,
        measured_started
            .map(|started| started.elapsed())
            .unwrap_or_else(|| started.elapsed()),
        &[
            ("traces", traces.to_string()),
            ("spans_per_trace", spans_per_trace.to_string()),
        ],
        Some(&latencies),
        &timeline,
        0,
    )?;
    Ok(())
}

async fn run_http_ingest(options: BenchOptions) -> anyhow::Result<()> {
    let requests = options.requests.max(1);
    let spans_per_request = options.spans_per_request.max(1);
    let concurrency = options.concurrency.max(1);
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorageEngine::new());
    let fault_config = fault_injection_config(&options);
    let mut app = build_router_with_storage_and_limits(storage, Default::default());
    if let Some(fault_config) = fault_config.clone() {
        app = app.layer(middleware::from_fn(move |request, next| {
            maybe_fail_requests(fault_config.clone(), request, next)
        }));
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("vtbench server");
    });

    let client = Client::new();
    let started = Instant::now();
    let deadline = options
        .duration_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let mut latencies = LatencySummary::default();
    let mut timeline = TimelineSummary::new(sample_interval(&options));
    let mut request_index = 0usize;
    let mut error_count = 0usize;
    let warmup_deadline = options
        .warmup_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let mut measured_started = None;
    while request_index < requests
        || deadline
            .map(|deadline| Instant::now() < deadline)
            .unwrap_or(false)
    {
        let remaining = requests.saturating_sub(request_index);
        let wave = if deadline.is_some() {
            concurrency
        } else {
            concurrency.min(remaining.max(1))
        };
        let mut tasks = Vec::with_capacity(wave);
        for offset in 0..wave {
            let client = client.clone();
            let url = format!("http://{addr}/api/v1/traces/ingest");
            let payload = make_http_payload(request_index + offset, spans_per_request);
            tasks.push(tokio::spawn(async move {
                let request_started = Instant::now();
                let response = client.post(url).json(&payload).send().await;
                match response {
                    Ok(response) => (response.status().is_success(), request_started.elapsed()),
                    Err(_) => (false, request_started.elapsed()),
                }
            }));
        }
        for task in tasks {
            let (success, latency) = task.await.expect("join http ingest task");
            if warmup_deadline
                .map(|deadline| Instant::now() >= deadline)
                .unwrap_or(true)
            {
                if measured_started.is_none() {
                    measured_started = Some(Instant::now());
                }
                if success {
                    latencies.record(latency);
                } else {
                    error_count += spans_per_request;
                }
                if let Some(measured_started) = measured_started {
                    timeline.record(
                        measured_started.elapsed(),
                        latency,
                        spans_per_request,
                        success,
                    );
                }
            }
        }
        request_index += wave;
    }
    let elapsed = measured_started
        .map(|started| started.elapsed())
        .unwrap_or_else(|| started.elapsed());
    server.abort();

    print_summary_and_report(
        &options,
        "http-ingest",
        request_index * spans_per_request,
        elapsed,
        &[
            ("requests", request_index.to_string()),
            ("spans_per_request", spans_per_request.to_string()),
            ("concurrency", concurrency.to_string()),
            ("addr", format_socket_addr(addr)),
        ],
        Some(&latencies),
        &timeline,
        error_count,
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
struct BenchOptions {
    rows: usize,
    batch_size: usize,
    traces: usize,
    spans_per_trace: usize,
    queries: usize,
    requests: usize,
    spans_per_request: usize,
    concurrency: usize,
    duration_secs: Option<u64>,
    warmup_secs: Option<u64>,
    sample_interval_secs: Option<u64>,
    fault_after_secs: Option<u64>,
    fault_duration_secs: Option<u64>,
    report_file: Option<PathBuf>,
}

impl Default for BenchOptions {
    fn default() -> Self {
        Self {
            rows: 100_000,
            batch_size: 1_000,
            traces: 10_000,
            spans_per_trace: 5,
            queries: 50_000,
            requests: 2_000,
            spans_per_request: 5,
            concurrency: 32,
            duration_secs: None,
            warmup_secs: None,
            sample_interval_secs: None,
            fault_after_secs: None,
            fault_duration_secs: None,
            report_file: None,
        }
    }
}

fn parse_options(args: impl Iterator<Item = String>) -> anyhow::Result<BenchOptions> {
    let mut options = BenchOptions::default();
    for arg in args {
        let (key, value) = arg
            .strip_prefix("--")
            .and_then(|value| value.split_once('='))
            .ok_or_else(|| anyhow!("invalid argument: {arg}"))?;
        match key {
            "rows" => options.rows = value.parse()?,
            "batch-size" => options.batch_size = value.parse()?,
            "traces" => options.traces = value.parse()?,
            "spans-per-trace" => options.spans_per_trace = value.parse()?,
            "queries" => options.queries = value.parse()?,
            "requests" => options.requests = value.parse()?,
            "spans-per-request" => options.spans_per_request = value.parse()?,
            "concurrency" => options.concurrency = value.parse()?,
            "duration-secs" => options.duration_secs = Some(value.parse()?),
            "warmup-secs" => options.warmup_secs = Some(value.parse()?),
            "sample-interval-secs" => options.sample_interval_secs = Some(value.parse()?),
            "fault-after-secs" => options.fault_after_secs = Some(value.parse()?),
            "fault-duration-secs" => options.fault_duration_secs = Some(value.parse()?),
            "report-file" => options.report_file = Some(PathBuf::from(value)),
            _ => bail!("unknown option: --{key}"),
        }
    }
    Ok(options)
}

fn make_row(index: usize) -> Result<TraceSpanRow, vtcore::TraceModelError> {
    make_trace_row(index / 5, index % 5)
}

fn make_trace_row(
    trace_index: usize,
    span_index: usize,
) -> Result<TraceSpanRow, vtcore::TraceModelError> {
    let trace_id = format!("trace-{:08}", trace_index);
    let span_id = format!("span-{:08}-{:04}", trace_index, span_index);
    let start = ((trace_index * 10 + span_index) as i64) * 100;
    let end = start + 50;
    TraceSpanRow::new(
        trace_id,
        span_id,
        None,
        "GET /checkout",
        start,
        end,
        vec![
            Field::new("resource_attr:service.name", "checkout"),
            Field::new("span_attr:http.method", "GET"),
            Field::new("span_attr:http.route", "/checkout"),
        ],
    )
}

fn make_http_payload(request_index: usize, spans_per_request: usize) -> serde_json::Value {
    let spans = (0..spans_per_request)
        .map(|offset| {
            let absolute_index = request_index * spans_per_request + offset;
            json!({
                "trace_id": format!("trace-http-{:08}", request_index),
                "span_id": format!("span-http-{:08}", absolute_index),
                "parent_span_id": serde_json::Value::Null,
                "name": "GET /checkout",
                "start_time_unix_nano": (absolute_index as i64) * 100,
                "end_time_unix_nano": (absolute_index as i64) * 100 + 75,
                "attributes": [
                    { "key": "http.method", "value": { "kind": "string", "value": "GET" } },
                    { "key": "http.route", "value": { "kind": "string", "value": "/checkout" } }
                ],
                "status": { "code": 0, "message": "OK" }
            })
        })
        .collect::<Vec<_>>();

    json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": "vtbench",
                        "scope_version": "0.1.0",
                        "scope_attributes": [],
                        "spans": spans
                    }
                ]
            }
        ]
    })
}

#[derive(Debug, Default)]
struct LatencySummary {
    samples_ns: Vec<u64>,
}

impl LatencySummary {
    fn record(&mut self, duration: Duration) {
        self.samples_ns.push(duration.as_nanos() as u64);
    }

    fn quantile_ms(&self, quantile: f64) -> f64 {
        if self.samples_ns.is_empty() {
            return 0.0;
        }

        let mut sorted = self.samples_ns.clone();
        sorted.sort_unstable();
        let index = ((sorted.len() - 1) as f64 * quantile).round() as usize;
        sorted[index.min(sorted.len() - 1)] as f64 / 1_000_000.0
    }

    fn max_ms(&self) -> f64 {
        self.samples_ns.iter().copied().max().unwrap_or(0) as f64 / 1_000_000.0
    }

    fn sample_count(&self) -> usize {
        self.samples_ns.len()
    }
}

#[derive(Debug, Default)]
struct TimelineSummary {
    buckets: BTreeMap<u64, TimelineBucket>,
    interval: Option<Duration>,
}

#[derive(Debug, Default)]
struct TimelineBucket {
    operations: usize,
    errors: usize,
    latencies: LatencySummary,
}

impl TimelineSummary {
    fn new(interval: Option<Duration>) -> Self {
        Self {
            buckets: BTreeMap::new(),
            interval,
        }
    }

    fn record(&mut self, offset: Duration, latency: Duration, operations: usize, success: bool) {
        let Some(interval) = self.interval else {
            return;
        };
        let bucket = (offset.as_millis() / interval.as_millis().max(1)) as u64;
        let entry = self.buckets.entry(bucket).or_default();
        if success {
            entry.operations += operations;
            entry.latencies.record(latency);
        } else {
            entry.errors += operations;
        }
    }

    fn as_json(&self) -> Option<Vec<serde_json::Value>> {
        let interval = self.interval?;
        Some(
            self.buckets
                .iter()
                .map(|(bucket, summary)| {
                    let start_ms = interval.as_millis() as u64 * *bucket;
                    let end_ms = start_ms + interval.as_millis() as u64;
                    json!({
                        "bucket": bucket,
                        "start_ms": start_ms,
                        "end_ms": end_ms,
                        "operations": summary.operations,
                        "errors": summary.errors,
                        "latency_p50_ms": summary.latencies.quantile_ms(0.50),
                        "latency_p95_ms": summary.latencies.quantile_ms(0.95),
                        "latency_p99_ms": summary.latencies.quantile_ms(0.99),
                        "latency_p999_ms": summary.latencies.quantile_ms(0.999),
                        "latency_max_ms": summary.latencies.max_ms()
                    })
                })
                .collect(),
        )
    }
}

#[derive(Debug, Clone)]
struct FaultInjectionConfig {
    after: Duration,
    duration: Duration,
    started_at: Instant,
}

fn print_summary_and_report(
    options: &BenchOptions,
    mode: &str,
    operations: usize,
    elapsed: Duration,
    fields: &[(&str, String)],
    latencies: Option<&LatencySummary>,
    timeline: &TimelineSummary,
    error_count: usize,
) -> anyhow::Result<()> {
    let elapsed_secs = elapsed.as_secs_f64().max(f64::EPSILON);
    let ops_per_sec = operations as f64 / elapsed_secs;
    println!("mode={mode}");
    println!("operations={operations}");
    println!("errors={error_count}");
    println!("elapsed_ms={:.3}", elapsed.as_secs_f64() * 1000.0);
    println!("ops_per_sec={:.3}", ops_per_sec);
    if let Some(latencies) = latencies {
        println!("latency_samples={}", latencies.sample_count());
        println!("latency_p50_ms={:.3}", latencies.quantile_ms(0.50));
        println!("latency_p95_ms={:.3}", latencies.quantile_ms(0.95));
        println!("latency_p99_ms={:.3}", latencies.quantile_ms(0.99));
        println!("latency_p999_ms={:.3}", latencies.quantile_ms(0.999));
        println!("latency_max_ms={:.3}", latencies.max_ms());
    }
    for (key, value) in fields {
        println!("{key}={value}");
    }

    if let Some(report_file) = &options.report_file {
        if let Some(parent) = report_file.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent)?;
            }
        }
        let mut report = serde_json::Map::new();
        report.insert("mode".to_string(), json!(mode));
        report.insert("operations".to_string(), json!(operations));
        report.insert("errors".to_string(), json!(error_count));
        report.insert(
            "elapsed_ms".to_string(),
            json!(elapsed.as_secs_f64() * 1000.0),
        );
        report.insert("ops_per_sec".to_string(), json!(ops_per_sec));
        report.insert("duration_secs".to_string(), json!(options.duration_secs));
        report.insert("warmup_secs".to_string(), json!(options.warmup_secs));
        for (key, value) in fields {
            report.insert((*key).to_string(), json!(value));
        }
        if let Some(latencies) = latencies {
            report.insert(
                "latency_samples".to_string(),
                json!(latencies.sample_count()),
            );
            report.insert(
                "latency_p50_ms".to_string(),
                json!(latencies.quantile_ms(0.50)),
            );
            report.insert(
                "latency_p95_ms".to_string(),
                json!(latencies.quantile_ms(0.95)),
            );
            report.insert(
                "latency_p99_ms".to_string(),
                json!(latencies.quantile_ms(0.99)),
            );
            report.insert(
                "latency_p999_ms".to_string(),
                json!(latencies.quantile_ms(0.999)),
            );
            report.insert("latency_max_ms".to_string(), json!(latencies.max_ms()));
        }
        if let Some(series) = timeline.as_json() {
            report.insert("timeline".to_string(), json!(series));
        }
        let payload = serde_json::to_vec_pretty(&serde_json::Value::Object(report))?;
        fs::write(report_file, payload)?;
    }
    Ok(())
}

fn sample_interval(options: &BenchOptions) -> Option<Duration> {
    options
        .sample_interval_secs
        .or(options.duration_secs.map(|_| 1))
        .filter(|seconds| *seconds > 0)
        .map(Duration::from_secs)
}

fn fault_injection_config(options: &BenchOptions) -> Option<FaultInjectionConfig> {
    match (options.fault_after_secs, options.fault_duration_secs) {
        (Some(after), Some(duration)) if duration > 0 => Some(FaultInjectionConfig {
            after: Duration::from_secs(after),
            duration: Duration::from_secs(duration),
            started_at: Instant::now(),
        }),
        _ => None,
    }
}

async fn maybe_fail_requests(
    fault_config: FaultInjectionConfig,
    request: Request,
    next: Next,
) -> Response {
    let elapsed = fault_config.started_at.elapsed();
    if elapsed >= fault_config.after
        && elapsed < fault_config.after.saturating_add(fault_config.duration)
    {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    next.run(request).await
}

fn format_socket_addr(addr: SocketAddr) -> String {
    format!("{}:{}", addr.ip(), addr.port())
}

#[cfg(test)]
mod tests {
    use super::{parse_options, LatencySummary};
    use std::time::Duration;

    #[test]
    fn parse_options_reads_duration_secs() {
        let options = parse_options(["--duration-secs=15".to_string()].into_iter())
            .expect("options should parse");

        assert_eq!(options.duration_secs, Some(15));
    }

    #[test]
    fn parse_options_reads_report_and_warmup() {
        let options = parse_options(
            [
                "--warmup-secs=3".to_string(),
                "--sample-interval-secs=2".to_string(),
                "--fault-after-secs=5".to_string(),
                "--fault-duration-secs=4".to_string(),
                "--report-file=/tmp/vtbench.json".to_string(),
            ]
            .into_iter(),
        )
        .expect("options should parse");

        assert_eq!(options.warmup_secs, Some(3));
        assert_eq!(options.sample_interval_secs, Some(2));
        assert_eq!(options.fault_after_secs, Some(5));
        assert_eq!(options.fault_duration_secs, Some(4));
        assert_eq!(
            options.report_file.as_deref(),
            Some(std::path::Path::new("/tmp/vtbench.json"))
        );
    }

    #[test]
    fn latency_summary_reports_quantiles() {
        let mut summary = LatencySummary::default();
        for millis in [10, 20, 30, 40, 50] {
            summary.record(Duration::from_millis(millis));
        }

        assert_eq!(summary.sample_count(), 5);
        assert!((summary.quantile_ms(0.50) - 30.0).abs() < 0.001);
        assert!((summary.quantile_ms(0.95) - 50.0).abs() < 0.001);
        assert!((summary.quantile_ms(0.99) - 50.0).abs() < 0.001);
        assert!((summary.max_ms() - 50.0).abs() < 0.001);
    }
}
