mod compare;
mod preflight;

use std::{
    collections::BTreeMap,
    env, fs,
    hash::{Hash, Hasher},
    net::SocketAddr,
    path::PathBuf,
    process::Command,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use anyhow::{anyhow, bail, Context};
use axum::{
    extract::Request,
    http::StatusCode,
    middleware::{self, Next},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use compare::{default_report_schema_version, run_compare, CompareOptions};
use preflight::{validate_benchmark_preflight, validate_otlp_benchmark_target};
use reqwest::Client;
use rustc_hash::FxHasher;
use serde_json::json;
use vtapi::build_router_with_storage_and_limits;
use vtcore::{Field, TraceBlock, TraceSearchRequest, TraceSpanRow};
use vtingest::{
    encode_export_trace_service_request_protobuf, AttributeValue, ExportTraceServiceRequest,
    KeyValue, ResourceSpans, ScopeSpans, SpanRecord, Status,
};
use vtstorage::{
    BatchingStorageConfig, BatchingStorageEngine, DiskStorageConfig, DiskStorageEngine,
    DiskSyncPolicy, MemoryStorageEngine, StorageEngine,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut args = env::args().skip(1);
    let mode = args.next().ok_or_else(|| {
        anyhow!("usage: vtbench <storage-ingest|storage-query|http-ingest|otlp-protobuf-load|disk-trace-block-append|compare> [--key=value ...]")
    })?;
    validate_benchmark_preflight(&mode)?;
    match mode.as_str() {
        "compare" => run_compare(parse_compare_options(args)?),
        _ => {
            let options = parse_options(args)?;
            match mode.as_str() {
                "storage-ingest" => run_storage_ingest(options),
                "storage-query" => run_storage_query(options),
                "http-ingest" => run_http_ingest(options).await,
                "otlp-protobuf-load" => run_otlp_protobuf_load(options).await,
                "disk-trace-block-append" => run_disk_trace_block_append(options),
                _ => bail!("unknown mode: {mode}"),
            }
        }
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

    let client = Client::builder()
        .no_proxy()
        .build()
        .context("build reqwest client for http-ingest")?;
    let started = Instant::now();
    let deadline = options
        .duration_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let latencies = Arc::new(Mutex::new(LatencySummary::default()));
    let timeline = Arc::new(Mutex::new(TimelineSummary::new(sample_interval(&options))));
    let completed_requests = Arc::new(AtomicUsize::new(0));
    let next_request = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let warmup_deadline = options
        .warmup_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let measured_started = Arc::new(Mutex::new(None::<Instant>));
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let latencies = latencies.clone();
        let timeline = timeline.clone();
        let completed_requests = completed_requests.clone();
        let next_request = next_request.clone();
        let error_count = error_count.clone();
        let measured_started = measured_started.clone();
        let deadline = deadline;
        let warmup_deadline = warmup_deadline;
        workers.push(tokio::spawn(async move {
            loop {
                let request_index = next_request.fetch_add(1, Ordering::Relaxed);
                if deadline.is_none() && request_index >= requests {
                    break;
                }
                if let Some(deadline) = deadline {
                    if Instant::now() >= deadline {
                        break;
                    }
                }

                let url = format!("http://{addr}/api/v1/traces/ingest");
                let payload = make_http_payload(request_index, spans_per_request);
                let request_started = Instant::now();
                let response = client.post(url).json(&payload).send().await;
                let success = match response {
                    Ok(response) => {
                        let success = response.status().is_success();
                        let _ = response.bytes().await;
                        success
                    }
                    Err(_) => false,
                };
                let latency = request_started.elapsed();
                completed_requests.fetch_add(1, Ordering::Relaxed);

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
                        error_count.fetch_add(spans_per_request, Ordering::Relaxed);
                    }
                    timeline.lock().expect("timeline lock").record(
                        measured_origin.elapsed(),
                        latency,
                        spans_per_request,
                        success,
                    );
                }
            }
        }));
    }
    for worker in workers {
        worker.await.expect("join http ingest worker");
    }
    let elapsed = measured_started
        .lock()
        .expect("measured start lock")
        .map(|started| started.elapsed())
        .unwrap_or_else(|| started.elapsed());
    let request_index = completed_requests.load(Ordering::Relaxed);
    server.abort();
    let latencies = latencies.lock().expect("latency lock");
    let timeline = timeline.lock().expect("timeline lock");

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
        error_count.load(Ordering::Relaxed),
    )?;
    Ok(())
}

async fn run_otlp_protobuf_load(options: BenchOptions) -> anyhow::Result<()> {
    let url = options
        .url
        .clone()
        .ok_or_else(|| anyhow!("--url is required for otlp-protobuf-load"))?;
    let requests = options.requests.max(1);
    let spans_per_request = options.spans_per_request.max(1);
    let concurrency = options.concurrency.max(1);
    let payload_variants = options.payload_variants.max(1);
    let payloads = make_otlp_protobuf_payload_variants(payload_variants, spans_per_request)?;
    let client = Client::builder()
        .no_proxy()
        .build()
        .context("build reqwest client for otlp-protobuf-load")?;
    validate_otlp_benchmark_target(&client, &url).await?;
    let started = Instant::now();
    let deadline = options
        .duration_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let latencies = Arc::new(Mutex::new(LatencySummary::default()));
    let timeline = Arc::new(Mutex::new(TimelineSummary::new(sample_interval(&options))));
    let completed_requests = Arc::new(AtomicUsize::new(0));
    let next_request = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));
    let warmup_deadline = options
        .warmup_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let measured_started = Arc::new(Mutex::new(None::<Instant>));
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let client = client.clone();
        let url = url.clone();
        let payloads = payloads.clone();
        let latencies = latencies.clone();
        let timeline = timeline.clone();
        let completed_requests = completed_requests.clone();
        let next_request = next_request.clone();
        let error_count = error_count.clone();
        let measured_started = measured_started.clone();
        let deadline = deadline;
        let warmup_deadline = warmup_deadline;
        workers.push(tokio::spawn(async move {
            loop {
                let request_index = next_request.fetch_add(1, Ordering::Relaxed);
                if deadline.is_none() && request_index >= requests {
                    break;
                }
                if let Some(deadline) = deadline {
                    if Instant::now() >= deadline {
                        break;
                    }
                }

                let payload = payloads[request_index % payloads.len()].clone();
                let request_started = Instant::now();
                let response = client
                    .post(url.clone())
                    .header("content-type", "application/x-protobuf")
                    .body(payload)
                    .send()
                    .await;
                let success = match response {
                    Ok(response) => {
                        let success = response.status().is_success();
                        let _ = response.bytes().await;
                        success
                    }
                    Err(_) => false,
                };
                let latency = request_started.elapsed();
                completed_requests.fetch_add(1, Ordering::Relaxed);

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
                        error_count.fetch_add(spans_per_request, Ordering::Relaxed);
                    }
                    timeline.lock().expect("timeline lock").record(
                        measured_origin.elapsed(),
                        latency,
                        spans_per_request,
                        success,
                    );
                }
            }
        }));
    }
    for worker in workers {
        worker.await.expect("join protobuf ingest worker");
    }
    let elapsed = measured_started
        .lock()
        .expect("measured start lock")
        .map(|started| started.elapsed())
        .unwrap_or_else(|| started.elapsed());
    let request_index = completed_requests.load(Ordering::Relaxed);
    let latencies = latencies.lock().expect("latency lock");
    let timeline = timeline.lock().expect("timeline lock");

    print_summary_and_report(
        &options,
        "otlp-protobuf-load",
        request_index * spans_per_request,
        elapsed,
        &[
            ("requests", request_index.to_string()),
            ("spans_per_request", spans_per_request.to_string()),
            ("concurrency", concurrency.to_string()),
            ("payload_variants", payload_variants.to_string()),
            ("url", url),
        ],
        Some(&latencies),
        &timeline,
        error_count.load(Ordering::Relaxed),
    )?;
    Ok(())
}

fn run_disk_trace_block_append(options: BenchOptions) -> anyhow::Result<()> {
    let requests = options.requests.max(1);
    let blocks_per_append = options.blocks_per_append.max(1);
    let spans_per_block = options.spans_per_block.max(1);
    let trace_shards = options.trace_shards.max(1);
    let payload_variants = options.payload_variants.max(1);
    let data_path = temp_bench_dir("disk-trace-block-append");
    let storage = DiskStorageEngine::open_with_config(
        &data_path,
        DiskStorageConfig::default()
            .with_sync_policy(DiskSyncPolicy::None)
            .with_trace_shards(trace_shards),
    )?;
    let payloads = make_same_shard_trace_block_payload_variants(
        payload_variants,
        blocks_per_append,
        spans_per_block,
        trace_shards,
        0,
    )?;
    let started = Instant::now();
    let deadline = options
        .duration_secs
        .map(Duration::from_secs)
        .map(|duration| started + duration);
    let mut latencies = LatencySummary::default();
    let mut timeline = TimelineSummary::new(sample_interval(&options));
    let mut request_index = 0usize;
    let operations_per_append = blocks_per_append * spans_per_block;
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
        let batch = payloads[request_index % payloads.len()].clone();
        let append_started = Instant::now();
        storage.append_trace_blocks(batch)?;
        if warmup_deadline
            .map(|deadline| Instant::now() >= deadline)
            .unwrap_or(true)
        {
            if measured_started.is_none() {
                measured_started = Some(Instant::now());
            }
            let latency = append_started.elapsed();
            latencies.record(latency);
            if let Some(measured_started) = measured_started {
                timeline.record(
                    measured_started.elapsed(),
                    latency,
                    operations_per_append,
                    true,
                );
            }
        }
        request_index += 1;
    }
    let elapsed = measured_started
        .map(|started| started.elapsed())
        .unwrap_or_else(|| started.elapsed());
    let operations = request_index * operations_per_append;
    drop(storage);
    fs::remove_dir_all(&data_path).with_context(|| format!("remove {}", data_path.display()))?;

    print_summary_and_report(
        &options,
        "disk-trace-block-append",
        operations,
        elapsed,
        &[
            ("requests", request_index.to_string()),
            ("blocks_per_append", blocks_per_append.to_string()),
            ("spans_per_block", spans_per_block.to_string()),
            ("trace_shards", trace_shards.to_string()),
            ("payload_variants", payload_variants.to_string()),
        ],
        Some(&latencies),
        &timeline,
        0,
    )?;
    Ok(())
}

#[derive(Debug, Clone)]
struct BenchOptions {
    rows: usize,
    batch_size: usize,
    blocks_per_append: usize,
    traces: usize,
    spans_per_trace: usize,
    queries: usize,
    requests: usize,
    spans_per_request: usize,
    spans_per_block: usize,
    payload_variants: usize,
    trace_shards: usize,
    concurrency: usize,
    url: Option<String>,
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
            blocks_per_append: 1,
            traces: 10_000,
            spans_per_trace: 5,
            queries: 50_000,
            requests: 2_000,
            spans_per_request: 5,
            spans_per_block: 5,
            payload_variants: 1024,
            trace_shards: 1,
            concurrency: 32,
            url: None,
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
            "blocks-per-append" => options.blocks_per_append = value.parse()?,
            "traces" => options.traces = value.parse()?,
            "spans-per-trace" => options.spans_per_trace = value.parse()?,
            "queries" => options.queries = value.parse()?,
            "requests" => options.requests = value.parse()?,
            "spans-per-request" => options.spans_per_request = value.parse()?,
            "spans-per-block" => options.spans_per_block = value.parse()?,
            "payload-variants" => options.payload_variants = value.parse()?,
            "trace-shards" => options.trace_shards = value.parse()?,
            "concurrency" => options.concurrency = value.parse()?,
            "url" => options.url = Some(value.to_string()),
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

fn parse_compare_options(args: impl Iterator<Item = String>) -> anyhow::Result<CompareOptions> {
    let mut baseline_file = None;
    let mut candidate_file = None;
    let mut min_throughput_ratio = 1.0;
    let mut max_p99_ratio = 1.0;

    for arg in args {
        let (key, value) = arg
            .strip_prefix("--")
            .and_then(|value| value.split_once('='))
            .ok_or_else(|| anyhow!("invalid argument: {arg}"))?;
        match key {
            "baseline-file" => baseline_file = Some(PathBuf::from(value)),
            "candidate-file" => candidate_file = Some(PathBuf::from(value)),
            "min-throughput-ratio" => min_throughput_ratio = value.parse()?,
            "max-p99-ratio" => max_p99_ratio = value.parse()?,
            _ => bail!("unknown option: --{key}"),
        }
    }

    Ok(CompareOptions {
        baseline_file: baseline_file
            .ok_or_else(|| anyhow!("--baseline-file is required for compare"))?,
        candidate_file: candidate_file
            .ok_or_else(|| anyhow!("--candidate-file is required for compare"))?,
        min_throughput_ratio,
        max_p99_ratio,
    })
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

fn make_otlp_protobuf_payload_variants(
    payload_variants: usize,
    spans_per_request: usize,
) -> anyhow::Result<Vec<Bytes>> {
    (0..payload_variants.max(1))
        .map(|request_index| make_otlp_protobuf_payload(request_index, spans_per_request))
        .collect()
}

fn make_otlp_protobuf_payload(
    request_index: usize,
    spans_per_request: usize,
) -> anyhow::Result<Bytes> {
    let spans = (0..spans_per_request)
        .map(|offset| {
            let absolute_index = request_index * spans_per_request + offset;
            SpanRecord {
                trace_id: format!("{:032x}", request_index + 1),
                span_id: format!("{:016x}", absolute_index + 1),
                parent_span_id: None,
                name: "GET /checkout".to_string(),
                start_time_unix_nano: (absolute_index as i64) * 100,
                end_time_unix_nano: (absolute_index as i64) * 100 + 75,
                attributes: vec![
                    KeyValue::new("http.method", AttributeValue::String("GET".to_string())),
                    KeyValue::new(
                        "http.route",
                        AttributeValue::String("/checkout".to_string()),
                    ),
                ],
                status: Some(Status {
                    code: 0,
                    message: "OK".to_string(),
                }),
            }
        })
        .collect();
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("vtbench".to_string()),
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

fn make_same_shard_trace_block_payload_variants(
    payload_variants: usize,
    blocks_per_append: usize,
    spans_per_block: usize,
    trace_shards: usize,
    shard_index: usize,
) -> anyhow::Result<Vec<Vec<TraceBlock>>> {
    let trace_ids = trace_ids_for_shard(
        trace_shards,
        shard_index,
        payload_variants * blocks_per_append,
    );
    let mut next_trace = 0usize;
    let mut payloads = Vec::with_capacity(payload_variants);
    for request_index in 0..payload_variants {
        let mut blocks = Vec::with_capacity(blocks_per_append);
        for block_offset in 0..blocks_per_append {
            blocks.push(make_same_shard_trace_block(
                request_index,
                block_offset,
                blocks_per_append,
                &trace_ids[next_trace],
                spans_per_block,
            )?);
            next_trace += 1;
        }
        payloads.push(blocks);
    }
    Ok(payloads)
}

fn make_same_shard_trace_block(
    request_index: usize,
    block_offset: usize,
    blocks_per_append: usize,
    trace_id: &str,
    spans_per_block: usize,
) -> anyhow::Result<TraceBlock> {
    let mut rows = Vec::with_capacity(spans_per_block);
    let absolute_block_index = request_index * blocks_per_append + block_offset;
    for span_offset in 0..spans_per_block {
        let absolute_index = absolute_block_index * spans_per_block + span_offset;
        rows.push(TraceSpanRow::new(
            trace_id.to_string(),
            format!("span-block-{request_index:08}-{block_offset:04}-{span_offset:04}"),
            None,
            "GET /checkout",
            (absolute_index as i64) * 100,
            (absolute_index as i64) * 100 + 75,
            vec![
                Field::new("resource_attr:service.name", "checkout"),
                Field::new("span_attr:http.method", "GET"),
                Field::new("span_attr:http.route", "/checkout"),
            ],
        )?);
    }
    Ok(TraceBlock::from_rows(rows))
}

fn trace_ids_for_shard(trace_shards: usize, shard_index: usize, count: usize) -> Vec<String> {
    let mut trace_ids = Vec::with_capacity(count);
    let mut candidate = 0usize;
    while trace_ids.len() < count {
        let trace_id = format!("trace-bench-shard-{candidate}");
        if trace_shard_index(&trace_id, trace_shards) == shard_index {
            trace_ids.push(trace_id);
        }
        candidate += 1;
    }
    trace_ids
}

fn trace_shard_index(trace_id: &str, trace_shards: usize) -> usize {
    if trace_shards <= 1 {
        return 0;
    }
    let mut hasher = FxHasher::default();
    trace_id.hash(&mut hasher);
    (hasher.finish() as usize) % trace_shards
}

fn temp_bench_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rust-vt-bench-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
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
        report.insert(
            "report_schema_version".to_string(),
            json!(default_report_schema_version()),
        );
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
        report.insert(
            "sample_interval_secs".to_string(),
            json!(options.sample_interval_secs),
        );
        report.insert(
            "fault_after_secs".to_string(),
            json!(options.fault_after_secs),
        );
        report.insert(
            "fault_duration_secs".to_string(),
            json!(options.fault_duration_secs),
        );
        report.insert(
            "binary_target_arch".to_string(),
            json!(std::env::consts::ARCH),
        );
        report.insert("binary_target_os".to_string(), json!(std::env::consts::OS));
        if let Some(git_sha) = current_git_sha() {
            report.insert("git_sha".to_string(), json!(git_sha));
        }
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

fn current_git_sha() -> Option<String> {
    let output = Command::new("git")
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?;
    let sha = sha.trim();
    if sha.is_empty() {
        None
    } else {
        Some(sha.to_string())
    }
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
    use super::{
        current_git_sha, make_otlp_protobuf_payload_variants,
        make_same_shard_trace_block_payload_variants, parse_compare_options, parse_options,
        trace_shard_index, LatencySummary,
    };
    use std::path::PathBuf;
    use std::time::Duration;
    use vtingest::decode_export_trace_service_request_protobuf;

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
    fn parse_options_reads_payload_variants_and_url() {
        let options = parse_options(
            [
                "--payload-variants=2048".to_string(),
                "--url=http://127.0.0.1:13000/v1/traces".to_string(),
            ]
            .into_iter(),
        )
        .expect("options should parse");

        assert_eq!(options.payload_variants, 2048);
        assert_eq!(
            options.url.as_deref(),
            Some("http://127.0.0.1:13000/v1/traces")
        );
    }

    #[test]
    fn parse_options_reads_direct_disk_trace_block_knobs() {
        let options = parse_options(
            [
                "--blocks-per-append=8".to_string(),
                "--spans-per-block=5".to_string(),
                "--trace-shards=4".to_string(),
            ]
            .into_iter(),
        )
        .expect("options should parse");

        assert_eq!(options.blocks_per_append, 8);
        assert_eq!(options.spans_per_block, 5);
        assert_eq!(options.trace_shards, 4);
    }

    #[test]
    fn parse_compare_options_reads_thresholds() {
        let options = parse_compare_options(
            [
                "--baseline-file=/tmp/base.json".to_string(),
                "--candidate-file=/tmp/candidate.json".to_string(),
                "--min-throughput-ratio=0.97".to_string(),
                "--max-p99-ratio=1.10".to_string(),
            ]
            .into_iter(),
        )
        .expect("compare options should parse");

        assert_eq!(options.baseline_file, PathBuf::from("/tmp/base.json"));
        assert_eq!(options.candidate_file, PathBuf::from("/tmp/candidate.json"));
        assert_eq!(options.min_throughput_ratio, 0.97);
        assert_eq!(options.max_p99_ratio, 1.10);
    }

    #[test]
    fn current_git_sha_is_available_inside_repo() {
        let sha = current_git_sha();
        assert!(sha
            .as_deref()
            .map(|value| !value.is_empty())
            .unwrap_or(false));
    }

    #[test]
    fn direct_trace_block_payload_variants_stay_on_requested_shard() {
        let payloads = make_same_shard_trace_block_payload_variants(2, 3, 4, 4, 2)
            .expect("trace block payloads should build");

        assert_eq!(payloads.len(), 2);
        assert!(payloads.iter().all(|payload| payload.len() == 3));
        assert!(payloads
            .iter()
            .flat_map(|payload| payload.iter())
            .all(|block| block.row_count() == 4));
        assert!(payloads
            .iter()
            .flat_map(|payload| payload.iter())
            .all(|block| {
                block
                    .trace_ids
                    .iter()
                    .all(|trace_id| trace_shard_index(trace_id.as_ref(), 4) == 2)
            }));
    }

    #[test]
    fn protobuf_payload_variants_use_distinct_trace_ids() {
        let payloads =
            make_otlp_protobuf_payload_variants(2, 3).expect("protobuf payloads should build");
        let first =
            decode_export_trace_service_request_protobuf(&payloads[0]).expect("first payload");
        let second =
            decode_export_trace_service_request_protobuf(&payloads[1]).expect("second payload");

        let first_trace_id = &first.resource_spans[0].scope_spans[0].spans[0].trace_id;
        let second_trace_id = &second.resource_spans[0].scope_spans[0].spans[0].trace_id;
        assert_ne!(first_trace_id, second_trace_id);
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
