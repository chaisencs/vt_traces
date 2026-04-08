use axum::{
    body::Body,
    http::{Request, StatusCode},
    routing::{get, post},
    Router,
};
use rcgen::{BasicConstraints, CertificateParams, DistinguishedName, DnType, IsCa, SanType};
use reqwest::{Client, StatusCode as ReqwestStatusCode};
use serde_json::{json, Value};
use std::{
    fs,
    net::IpAddr,
    net::SocketAddr,
    net::TcpListener as StdTcpListener,
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread,
    thread::JoinHandle as ThreadJoinHandle,
    time::Duration,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Barrier},
    task::JoinHandle,
};
use tower::ServiceExt;
use vtapi::{
    build_insert_router, build_router, build_router_with_limits, build_router_with_storage,
    build_router_with_storage_and_trace_ingest_profile, build_router_with_storage_auth_and_limits,
    build_select_router, build_select_router_with_auth_and_limits,
    build_select_router_with_client_auth_and_limits, build_storage_router,
    build_storage_router_with_auth_and_limits, build_storage_router_with_limits,
    build_storage_router_with_startup_auth_and_limits, serve_app, spawn_background_rebalance_task,
    ApiLimitsConfig, AuthConfig, ClusterConfig, ClusterHttpClient, ClusterHttpClientConfig,
    ServerTlsConfig, StorageStartupState, TraceIngestProfile,
};
use vtcore::{
    encode_trace_rows, LogRow, LogSearchRequest, TraceSearchHit, TraceSearchRequest, TraceSpanRow,
    TraceWindow,
};
use vtingest::{
    decode_export_trace_service_request_protobuf, encode_export_logs_service_request_protobuf,
    encode_export_trace_service_request_protobuf, AttributeValue, ExportLogsServiceRequest,
    ExportTraceServiceRequest, KeyValue, LogRecord, ResourceLogs, ResourceSpans, ScopeLogs,
    ScopeSpans, SpanRecord, Status,
};
use vtstorage::{
    DiskStorageConfig, DiskStorageEngine, DiskSyncPolicy, MemoryStorageEngine, StorageEngine,
    StorageError, StorageStatsSnapshot,
};

fn temp_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rust-vt-api-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn grpc_frame(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload);
    framed
}

struct TestServer {
    base_url: String,
    handle: JoinHandle<()>,
}

impl TestServer {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

struct ThreadRuntimeServer {
    base_url: String,
    shutdown: Option<oneshot::Sender<()>>,
    handle: Option<ThreadJoinHandle<()>>,
}

impl ThreadRuntimeServer {
    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

impl Drop for ThreadRuntimeServer {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

struct GeneratedTlsMaterials {
    ca_cert_pem: String,
    server_cert_pem: String,
    server_key_pem: String,
    client_cert_pem: String,
    client_key_pem: String,
}

async fn spawn_server(app: Router) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    spawn_server_from_listener(listener, app).await
}

async fn spawn_server_at(addr: SocketAddr, app: Router) -> TestServer {
    let listener = TcpListener::bind(addr).await.expect("bind listener");
    spawn_server_from_listener(listener, app).await
}

async fn spawn_server_from_listener(listener: TcpListener, app: Router) -> TestServer {
    let addr = listener.local_addr().expect("listener addr");
    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server should run");
    });

    TestServer {
        base_url: format!("http://{addr}"),
        handle,
    }
}

fn spawn_server_on_single_worker_runtime(app: Router) -> ThreadRuntimeServer {
    let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind listener");
    let addr = listener.local_addr().expect("listener addr");
    listener
        .set_nonblocking(true)
        .expect("set listener nonblocking");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handle = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("build single-worker runtime");
        runtime.block_on(async move {
            let listener = TcpListener::from_std(listener).expect("tokio listener");
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = shutdown_rx.await;
                })
                .await
                .expect("server should run");
        });
    });

    ThreadRuntimeServer {
        base_url: format!("http://{addr}"),
        shutdown: Some(shutdown_tx),
        handle: Some(handle),
    }
}

async fn spawn_tls_server(app: Router, tls_config: ServerTlsConfig) -> TestServer {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind TLS listener");
    let addr = listener.local_addr().expect("listener addr");
    let handle = tokio::spawn(async move {
        serve_app(listener, app, Some(tls_config))
            .await
            .expect("TLS server should run");
    });

    TestServer {
        base_url: format!("https://localhost:{}", addr.port()),
        handle,
    }
}

fn reserve_addr() -> SocketAddr {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("reserve listener")
        .local_addr()
        .expect("listener addr")
}

fn generate_tls_materials() -> GeneratedTlsMaterials {
    let mut ca_params = CertificateParams::default();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.distinguished_name = DistinguishedName::new();
    ca_params
        .distinguished_name
        .push(DnType::CommonName, "rust-vt-test-ca");
    let ca_cert = rcgen::Certificate::from_params(ca_params).expect("create CA cert");

    let mut server_params = CertificateParams::new(vec!["localhost".to_string()]);
    server_params
        .subject_alt_names
        .push(SanType::IpAddress(IpAddr::from([127, 0, 0, 1])));
    server_params.distinguished_name = DistinguishedName::new();
    server_params
        .distinguished_name
        .push(DnType::CommonName, "localhost");
    let server_cert =
        rcgen::Certificate::from_params(server_params).expect("create server certificate");

    let mut client_params = CertificateParams::default();
    client_params.distinguished_name = DistinguishedName::new();
    client_params
        .distinguished_name
        .push(DnType::CommonName, "rust-vt-test-client");
    let client_cert =
        rcgen::Certificate::from_params(client_params).expect("create client certificate");

    GeneratedTlsMaterials {
        ca_cert_pem: ca_cert.serialize_pem().expect("serialize CA"),
        server_cert_pem: server_cert
            .serialize_pem_with_signer(&ca_cert)
            .expect("serialize server cert"),
        server_key_pem: server_cert.serialize_private_key_pem(),
        client_cert_pem: client_cert
            .serialize_pem_with_signer(&ca_cert)
            .expect("serialize client cert"),
        client_key_pem: client_cert.serialize_private_key_pem(),
    }
}

struct SlowStorageEngine {
    delay: Duration,
    started: AtomicBool,
}

impl SlowStorageEngine {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            started: AtomicBool::new(false),
        }
    }

    fn started(&self) -> bool {
        self.started.load(Ordering::Relaxed)
    }
}

impl StorageEngine for SlowStorageEngine {
    fn append_rows(&self, _rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        self.started.store(true, Ordering::Relaxed);
        thread::sleep(self.delay);
        Ok(())
    }

    fn append_logs(&self, _rows: Vec<LogRow>) -> Result<(), StorageError> {
        self.started.store(true, Ordering::Relaxed);
        thread::sleep(self.delay);
        Ok(())
    }

    fn trace_window(&self, _trace_id: &str) -> Option<TraceWindow> {
        None
    }

    fn list_trace_ids(&self) -> Vec<String> {
        Vec::new()
    }

    fn list_services(&self) -> Vec<String> {
        Vec::new()
    }

    fn list_field_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn list_field_values(&self, _field_name: &str) -> Vec<String> {
        Vec::new()
    }

    fn search_traces(&self, _request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        Vec::new()
    }

    fn search_logs(&self, _request: &LogSearchRequest) -> Vec<LogRow> {
        Vec::new()
    }

    fn rows_for_trace(
        &self,
        _trace_id: &str,
        _start_unix_nano: i64,
        _end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        Vec::new()
    }

    fn stats(&self) -> StorageStatsSnapshot {
        StorageStatsSnapshot::default()
    }
}

#[tokio::test]
async fn healthz_returns_ok() {
    let app = build_router(MemoryStorageEngine::new());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test]
async fn storage_router_binds_and_returns_service_unavailable_until_startup_finishes() {
    let startup = StorageStartupState::starting();
    let server = spawn_server(build_storage_router_with_startup_auth_and_limits(
        Arc::new(MemoryStorageEngine::new()),
        startup.clone(),
        AuthConfig::default(),
        ApiLimitsConfig::default(),
    ))
    .await;
    let client = Client::builder()
        .no_proxy()
        .build()
        .expect("build reqwest client");

    let health_response = client
        .get(server.url("/healthz"))
        .send()
        .await
        .expect("startup healthz response");
    assert_eq!(health_response.status(), ReqwestStatusCode::SERVICE_UNAVAILABLE);
    let health_body: Value = health_response
        .json()
        .await
        .expect("startup healthz body");
    assert_eq!(health_body["status"], "starting");

    let services_response = client
        .get(server.url("/internal/v1/services"))
        .send()
        .await
        .expect("startup services response");
    assert_eq!(services_response.status(), ReqwestStatusCode::SERVICE_UNAVAILABLE);

    startup.mark_ready();

    let ready_health_response = client
        .get(server.url("/healthz"))
        .send()
        .await
        .expect("ready healthz response");
    assert_eq!(ready_health_response.status(), ReqwestStatusCode::OK);

    let ready_services_response = client
        .get(server.url("/internal/v1/services"))
        .send()
        .await
        .expect("ready services response");
    assert_eq!(ready_services_response.status(), ReqwestStatusCode::OK);
}

#[tokio::test]
async fn trace_ingest_keeps_metrics_responsive_while_storage_runs() {
    let storage = Arc::new(SlowStorageEngine::new(Duration::from_millis(250)));
    let server = spawn_server_on_single_worker_runtime(build_router_with_storage(storage.clone()));
    let client = Client::builder()
        .no_proxy()
        .build()
        .expect("build reqwest client");
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": "io.opentelemetry.auto",
                        "scope_version": "1.0.0",
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-blocking-boundary-1",
                                "span_id": "span-blocking-boundary-1",
                                "parent_span_id": null,
                                "name": "GET /blocking-boundary",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": { "code": 0, "message": "OK" }
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_request = {
        let client = client.clone();
        let url = server.url("/api/v1/traces/ingest");
        let body = ingest_payload.to_string();
        tokio::spawn(async move {
            client
                .post(url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .expect("trace ingest request")
        })
    };

    let wait_started = Instant::now();
    while !storage.started() {
        assert!(
            wait_started.elapsed() < Duration::from_secs(2),
            "trace ingest never reached storage append",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let metrics_response = tokio::time::timeout(
        Duration::from_millis(100),
        client.get(server.url("/metrics")).send(),
    )
    .await
    .expect("metrics request should stay responsive while trace ingest is in flight")
    .expect("metrics request");
    assert_eq!(metrics_response.status(), ReqwestStatusCode::OK);

    let ingest_response = ingest_request.await.expect("join ingest request");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);
}

#[tokio::test]
async fn trace_ingest_throughput_profile_can_block_metrics_on_single_worker_runtime() {
    let storage = Arc::new(SlowStorageEngine::new(Duration::from_millis(250)));
    let server =
        spawn_server_on_single_worker_runtime(build_router_with_storage_and_trace_ingest_profile(
            storage.clone(),
            TraceIngestProfile::Throughput,
        ));
    let client = Client::builder()
        .no_proxy()
        .build()
        .expect("build reqwest client");
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": "io.opentelemetry.auto",
                        "scope_version": "1.0.0",
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-throughput-profile-1",
                                "span_id": "span-throughput-profile-1",
                                "parent_span_id": null,
                                "name": "GET /throughput-profile",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": { "code": 0, "message": "OK" }
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_request = {
        let client = client.clone();
        let url = server.url("/api/v1/traces/ingest");
        let body = ingest_payload.to_string();
        tokio::spawn(async move {
            client
                .post(url)
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .expect("trace ingest request")
        })
    };

    let wait_started = Instant::now();
    while !storage.started() {
        assert!(
            wait_started.elapsed() < Duration::from_secs(2),
            "trace ingest never reached storage append",
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let metrics_result = tokio::time::timeout(
        Duration::from_millis(100),
        client.get(server.url("/metrics")).send(),
    )
    .await;
    assert!(
        metrics_result.is_err(),
        "throughput profile should allow metrics to block while trace ingest is in flight",
    );

    let ingest_response = ingest_request.await.expect("join ingest request");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);
}

#[tokio::test]
async fn ingest_then_query_trace_round_trip() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": "io.opentelemetry.auto",
                        "scope_version": "1.0.0",
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-http-1",
                                "span_id": "span-http-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "GET" } }
                                ],
                                "status": { "code": 0, "message": "OK" }
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request should succeed");

    assert_eq!(ingest_response.status(), StatusCode::OK);

    let query_response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/traces/trace-http-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("query request should succeed");

    assert_eq!(query_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(query_response.into_body(), usize::MAX)
        .await
        .expect("body should decode");
    let decoded: Value = serde_json::from_slice(&body).expect("json body");

    assert_eq!(decoded["trace_id"], "trace-http-1");
    assert_eq!(decoded["rows"].as_array().unwrap().len(), 1);
    assert_eq!(decoded["rows"][0]["trace_id"], "trace-http-1");
    assert_eq!(
        decoded["rows"][0]["fields"]["resource_attr:service.name"],
        "checkout"
    );
}

#[tokio::test]
async fn ingest_then_search_logs_round_trip() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_logs": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_logs": [
                    {
                        "scope_name": "io.opentelemetry.logs",
                        "scope_version": "1.0.0",
                        "scope_attributes": [],
                        "log_records": [
                            {
                                "time_unix_nano": 100,
                                "observed_time_unix_nano": 110,
                                "severity_number": 9,
                                "severity_text": "INFO",
                                "body": { "kind": "string", "value": "checkout completed" },
                                "trace_id": "00112233445566778899aabbccddeeff",
                                "span_id": "0123456789abcdef",
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "POST" } }
                                ]
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/logs")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("logs ingest request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/logs/search?start_unix_nano=0&end_unix_nano=1000&service_name=checkout&severity_text=INFO")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("logs search should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("logs search body");
    let payload: Value = serde_json::from_slice(&body).expect("logs search json");
    let rows = payload["rows"].as_array().cloned().unwrap_or_default();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["body"], "checkout completed");
    assert_eq!(rows[0]["severity_text"], "INFO");
}

#[tokio::test]
async fn ingest_logs_over_grpc_then_search() {
    let app = build_router(MemoryStorageEngine::new());
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource_attributes: vec![KeyValue::string("service.name", "payments")],
            scope_logs: vec![ScopeLogs {
                scope_name: Some("io.opentelemetry.logs".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: Vec::new(),
                log_records: vec![LogRecord {
                    time_unix_nano: 200,
                    observed_time_unix_nano: Some(220),
                    severity_number: Some(17),
                    severity_text: Some("ERROR".to_string()),
                    body: AttributeValue::String("payment declined".to_string()),
                    trace_id: Some("00112233445566778899aabbccddeeff".to_string()),
                    span_id: Some("0123456789abcdef".to_string()),
                    attributes: vec![KeyValue::string("error.type", "declined")],
                }],
            }],
        }],
    };
    let payload =
        encode_export_logs_service_request_protobuf(&request).expect("encode OTLP logs protobuf");
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                .header("content-type", "application/grpc")
                .body(Body::from(grpc_frame(&payload)))
                .unwrap(),
        )
        .await
        .expect("gRPC logs ingest should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok()),
        Some("0")
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/logs/search?start_unix_nano=0&end_unix_nano=1000&service_name=payments&severity_text=ERROR")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("logs search should succeed");
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("logs search body");
    let payload: Value = serde_json::from_slice(&body).expect("logs search json");
    let rows = payload["rows"].as_array().cloned().unwrap_or_default();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["body"], "payment declined");
}

#[tokio::test]
async fn cluster_ingest_then_search_logs_round_trip() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let storage_b = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), storage_b.base_url.clone()],
        2,
    )
    .expect("cluster config");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;

    let ingest_payload = json!({
        "resource_logs": [
            {
                "resource_attributes": [{"key": "service.name", "value": { "kind": "string", "value": "payments" }}],
                "scope_logs": [
                    {
                        "scope_name": "checkout",
                        "log_records": [
                            {
                                "time_unix_nano": 200,
                                "observed_time_unix_nano": 210,
                                "severity_number": 17,
                                "severity_text": "ERROR",
                                "body": { "kind": "string", "value": "payment declined" },
                                "attributes": [{"key": "gateway", "value": { "kind": "string", "value": "adyen" }}]
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let client = Client::new();
    let ingest_response = client
        .post(insert.url("/v1/logs"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("ingest request should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    let search_response = client
        .get(select.url(
            "/api/v1/logs/search?start_unix_nano=0&end_unix_nano=1000&service_name=payments&severity_text=ERROR",
        ))
        .send()
        .await
        .expect("search request should succeed");
    assert_eq!(search_response.status(), ReqwestStatusCode::OK);
    let body: Value = search_response.json().await.expect("json body");
    let rows = body["rows"].as_array().expect("rows array");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["body"], "payment declined");
}

#[tokio::test]
async fn otlp_http_json_ingest_works_on_standard_v1_traces_path() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": "io.opentelemetry.auto",
                        "scope_version": "1.0.0",
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-otlp-http-1",
                                "span_id": "span-otlp-http-1",
                                "parent_span_id": null,
                                "name": "GET /v1/traces",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": { "code": 0, "message": "OK" }
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("otlp ingest request should succeed");

    assert_eq!(ingest_response.status(), StatusCode::OK);

    let query_response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/traces/trace-otlp-http-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("query request should succeed");

    assert_eq!(query_response.status(), StatusCode::OK);
}

#[tokio::test]
async fn otlp_http_protobuf_ingest_works_on_standard_v1_traces_path() {
    let app = build_router(MemoryStorageEngine::new());
    let trace_id = "00112233445566778899aabbccddeeff";
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.rust".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: Vec::new(),
                spans: vec![SpanRecord {
                    trace_id: trace_id.to_string(),
                    span_id: "0123456789abcdef".to_string(),
                    parent_span_id: None,
                    name: "POST /protobuf".to_string(),
                    start_time_unix_nano: 100,
                    end_time_unix_nano: 180,
                    attributes: vec![KeyValue::new(
                        "http.method",
                        AttributeValue::String("POST".to_string()),
                    )],
                    status: Some(Status {
                        code: 0,
                        message: "OK".to_string(),
                    }),
                }],
            }],
        }],
    };
    let payload =
        encode_export_trace_service_request_protobuf(&request).expect("encode protobuf payload");

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/traces")
                .header("content-type", "application/x-protobuf")
                .body(Body::from(payload))
                .unwrap(),
        )
        .await
        .expect("protobuf ingest request should succeed");

    assert_eq!(ingest_response.status(), StatusCode::OK);

    let query_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/traces/{trace_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("query request should succeed");

    assert_eq!(query_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(query_response.into_body(), usize::MAX)
        .await
        .expect("body should decode");
    let decoded: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(decoded["trace_id"], trace_id);
    assert_eq!(
        decoded["rows"][0]["fields"]["resource_attr:service.name"],
        "checkout"
    );
    assert_eq!(
        decoded["rows"][0]["fields"]["span_attr:http.method"],
        "POST"
    );
}

#[tokio::test]
async fn otlp_grpc_ingest_works_on_trace_service_export_path() {
    let app = build_router(MemoryStorageEngine::new());
    let trace_id = "00112233445566778899aabbccddeeff";
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.rust".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: Vec::new(),
                spans: vec![SpanRecord {
                    trace_id: trace_id.to_string(),
                    span_id: "0123456789abcdef".to_string(),
                    parent_span_id: None,
                    name: "POST /grpc".to_string(),
                    start_time_unix_nano: 100,
                    end_time_unix_nano: 180,
                    attributes: vec![KeyValue::new(
                        "http.method",
                        AttributeValue::String("POST".to_string()),
                    )],
                    status: Some(Status {
                        code: 0,
                        message: "OK".to_string(),
                    }),
                }],
            }],
        }],
    };
    let payload =
        encode_export_trace_service_request_protobuf(&request).expect("encode protobuf payload");

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .header("content-type", "application/grpc")
                .body(Body::from(grpc_frame(&payload)))
                .unwrap(),
        )
        .await
        .expect("grpc ingest request should succeed");

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/grpc")
    );
    assert_eq!(
        response
            .headers()
            .get("grpc-status")
            .and_then(|value| value.to_str().ok()),
        Some("0")
    );
    let response_body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("grpc response body");
    assert_eq!(response_body.as_ref(), &[0, 0, 0, 0, 0]);

    let query_response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/traces/{trace_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("query request should succeed");

    assert_eq!(query_response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(query_response.into_body(), usize::MAX)
        .await
        .expect("body should decode");
    let decoded: Value = serde_json::from_slice(&body).expect("json body");
    assert_eq!(decoded["trace_id"], trace_id);
    assert_eq!(decoded["rows"][0]["name"], "POST /grpc");
}

#[tokio::test]
async fn ingest_returns_bad_request_for_invalid_time_range() {
    let app = build_router(MemoryStorageEngine::new());

    let invalid_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-bad-1",
                                "span_id": "span-bad-1",
                                "parent_span_id": null,
                                "name": "bad-span",
                                "start_time_unix_nano": 200,
                                "end_time_unix_nano": 100,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(invalid_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("request should complete");

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn public_routes_require_bearer_token_when_configured() {
    let app = build_router_with_storage_auth_and_limits(
        Arc::new(MemoryStorageEngine::new()),
        AuthConfig::default().with_public_bearer_token("public-secret"),
        ApiLimitsConfig::default(),
    );
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-auth-public-1",
                                "span_id": "span-auth-public-1",
                                "parent_span_id": null,
                                "name": "GET /secure",
                                "start_time_unix_nano": 1,
                                "end_time_unix_nano": 2,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("unauthorized request");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .header("authorization", "Bearer public-secret")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("authorized request");
    assert_eq!(authorized.status(), StatusCode::OK);
}

#[tokio::test]
async fn storage_internal_routes_require_internal_bearer_token_when_configured() {
    let app = build_storage_router_with_auth_and_limits(
        Arc::new(MemoryStorageEngine::new()),
        AuthConfig::default().with_internal_bearer_token("internal-secret"),
        ApiLimitsConfig::default(),
    );
    let rows_payload = json!({
        "rows": [
            {
                "trace_id": "trace-auth-internal-1",
                "span_id": "span-auth-internal-1",
                "parent_span_id": null,
                "name": "GET /internal",
                "start_unix_nano": 1,
                "end_unix_nano": 2,
                "time_unix_nano": 2,
                "fields": []
            }
        ]
    });

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/v1/rows")
                .header("content-type", "application/json")
                .body(Body::from(rows_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("unauthorized request");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/v1/rows")
                .header("content-type", "application/json")
                .header("authorization", "Bearer internal-secret")
                .body(Body::from(rows_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("authorized request");
    assert_eq!(authorized.status(), StatusCode::OK);

    let binary_rows = encode_trace_rows(&[TraceSpanRow {
        trace_id: "trace-auth-internal-bin-1".to_string(),
        span_id: "span-auth-internal-bin-1".to_string(),
        parent_span_id: None,
        name: "GET /internal-bin".to_string(),
        start_unix_nano: 3,
        end_unix_nano: 4,
        time_unix_nano: 4,
        fields: Vec::new(),
    }]);

    let authorized_binary = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/v1/rows")
                .header("content-type", "application/x-vt-row-batch")
                .header("authorization", "Bearer internal-secret")
                .body(Body::from(binary_rows))
                .unwrap(),
        )
        .await
        .expect("authorized binary request");
    assert_eq!(authorized_binary.status(), StatusCode::OK);
}

#[tokio::test]
async fn select_admin_routes_require_admin_bearer_token_when_configured() {
    let cluster = ClusterConfig::new(vec!["http://127.0.0.1:1".to_string()], 1).expect("cluster");
    let app = build_select_router_with_auth_and_limits(
        cluster,
        AuthConfig::default().with_admin_bearer_token("admin-secret"),
        ApiLimitsConfig::default(),
    );

    let unauthorized = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/v1/cluster/rebalance")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("unauthorized request");
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let authorized = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/admin/v1/cluster/rebalance")
                .header("authorization", "Bearer admin-secret")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("authorized request");
    assert_ne!(authorized.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn ingest_rejects_payloads_over_body_limit() {
    let app = build_router_with_limits(
        MemoryStorageEngine::new(),
        ApiLimitsConfig::default().with_max_request_body_bytes(128),
    );

    let oversized_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout-checkout-checkout-checkout" } }
                ],
                "scope_spans": []
            }
        ]
    });

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(oversized_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("oversized ingest request");

    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[serial_test::serial]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn storage_router_rejects_requests_when_concurrency_limit_is_reached() {
    let storage = Arc::new(SlowStorageEngine::new(Duration::from_secs(1)));
    let app = build_storage_router_with_limits(
        storage.clone(),
        ApiLimitsConfig::default().with_concurrency_limit(1),
    );
    let rows_payload = json!({
        "rows": [
            {
                "trace_id": "trace-overload-1",
                "span_id": "span-1",
                "parent_span_id": null,
                "name": "GET /overload",
                "start_unix_nano": 1,
                "end_unix_nano": 2,
                "time_unix_nano": 2,
                "fields": []
            }
        ]
    });

    let first_request = {
        let payload = rows_payload.clone();
        let app = app.clone();
        tokio::spawn(async move {
            app.oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/internal/v1/rows")
                    .header("content-type", "application/json")
                    .body(Body::from(payload.to_string()))
                    .unwrap(),
            )
            .await
            .expect("first request")
        })
    };

    let wait_started = Instant::now();
    while !storage.started() {
        assert!(
            wait_started.elapsed() < Duration::from_secs(2),
            "first request never occupied the concurrency slot"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let second_response = tokio::time::timeout(
        Duration::from_secs(5),
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/internal/v1/rows")
                .header("content-type", "application/json")
                .body(Body::from(rows_payload.to_string()))
                .unwrap(),
        ),
    )
    .await
    .expect("second request timed out")
    .expect("second request");
    assert_eq!(second_response.status(), StatusCode::SERVICE_UNAVAILABLE);

    let first_response = first_request.await.expect("join first request");
    assert_eq!(first_response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn background_rebalance_task_polls_select_admin_endpoint() {
    let rebalance_calls = Arc::new(AtomicU64::new(0));
    let app = {
        let rebalance_calls = rebalance_calls.clone();
        Router::new()
            .route(
                "/admin/v1/cluster/leader",
                get(|| async {
                    axum::Json(json!({
                        "leader": serde_json::Value::Null,
                        "local": true,
                        "epoch": 0,
                        "peers": []
                    }))
                }),
            )
            .route(
                "/admin/v1/cluster/rebalance",
                post(move || {
                    let rebalance_calls = rebalance_calls.clone();
                    async move {
                        rebalance_calls.fetch_add(1, Ordering::Relaxed);
                        StatusCode::OK
                    }
                }),
            )
    };
    let server = spawn_server(app).await;
    let task = spawn_background_rebalance_task(server.base_url.clone(), Duration::from_millis(50));
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if rebalance_calls.load(Ordering::Relaxed) >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("background rebalance never polled admin endpoint");
    task.abort();

    assert!(rebalance_calls.load(Ordering::Relaxed) >= 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_insert_fanout_runs_remote_writes_in_parallel() {
    let barrier = Arc::new(Barrier::new(2));
    let storage_a = spawn_server(Router::new().route(
        "/internal/v1/rows",
        post({
            let barrier = barrier.clone();
            move || {
                let barrier = barrier.clone();
                async move {
                    barrier.wait().await;
                    (StatusCode::OK, axum::Json(json!({ "ingested_rows": 1 })))
                }
            }
        }),
    ))
    .await;
    let storage_b = spawn_server(Router::new().route(
        "/internal/v1/rows",
        post({
            let barrier = barrier.clone();
            move || {
                let barrier = barrier.clone();
                async move {
                    barrier.wait().await;
                    (StatusCode::OK, axum::Json(json!({ "ingested_rows": 1 })))
                }
            }
        }),
    ))
    .await;
    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), storage_b.base_url.clone()],
        2,
    )
    .expect("cluster config");
    let app = build_insert_router(cluster);

    let response = tokio::time::timeout(
        Duration::from_secs(2),
        app.oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(
                    json!({
                        "resource_spans": [
                            {
                                "resource_attributes": [
                                    { "key": "service.name", "value": { "kind": "string", "value": "parallel-write" } }
                                ],
                                "scope_spans": [
                                    {
                                        "scope_name": null,
                                        "scope_version": null,
                                        "scope_attributes": [],
                                        "spans": [
                                            {
                                                "trace_id": "trace-cluster-parallel-write-1",
                                                "span_id": "span-cluster-parallel-write-1",
                                                "parent_span_id": null,
                                                "name": "POST /parallel-write",
                                                "start_time_unix_nano": 1,
                                                "end_time_unix_nano": 2,
                                                "attributes": [],
                                                "status": null
                                            }
                                        ]
                                    }
                                ]
                            }
                        ]
                    })
                    .to_string(),
                ))
                .unwrap(),
        ),
    )
    .await
    .expect("parallel replica writes never overlapped")
    .expect("parallel write request");

    assert_eq!(response.status(), StatusCode::OK);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cluster_select_returns_from_fast_replica_without_waiting_for_slow_one() {
    let slow_delay = Duration::from_millis(250);
    let fast_trace_id = "trace-cluster-fast-read-0".to_string();
    let trace_rows = json!({
        "trace_id": fast_trace_id,
        "rows": [
            {
                "trace_id": fast_trace_id,
                "span_id": "span-cluster-fast-read-1",
                "parent_span_id": null,
                "name": "GET /fast-read",
                "start_unix_nano": 1,
                "end_unix_nano": 2,
                "time_unix_nano": 2,
                "fields": {
                    "resource_attr:service.name": "fast-read"
                }
            }
        ]
    });
    let storage_slow = spawn_server(Router::new().route(
        "/internal/v1/traces/:trace_id",
        get({
            let trace_rows = trace_rows.clone();
            move || {
                let trace_rows = trace_rows.clone();
                async move {
                    tokio::time::sleep(slow_delay).await;
                    (StatusCode::OK, axum::Json(trace_rows))
                }
            }
        }),
    ))
    .await;
    let storage_fast = spawn_server(Router::new().route(
        "/internal/v1/traces/:trace_id",
        get({
            let trace_rows = trace_rows.clone();
            move || {
                let trace_rows = trace_rows.clone();
                async move { (StatusCode::OK, axum::Json(trace_rows)) }
            }
        }),
    ))
    .await;
    let cluster = ClusterConfig::new(
        vec![storage_slow.base_url.clone(), storage_fast.base_url.clone()],
        2,
    )
    .expect("cluster config")
    .with_read_quorum(1)
    .expect("read quorum");
    let trace_id = (0..4_096)
        .map(|index| format!("trace-cluster-fast-read-{index}"))
        .find(|trace_id| cluster.placements(trace_id)[0] == storage_slow.base_url.as_str())
        .expect("trace id that routes to slow node first");
    let app = build_select_router(cluster);

    let started = Instant::now();
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/traces/{trace_id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("fast read request");
    let elapsed = started.elapsed();

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        elapsed < Duration::from_millis(180),
        "trace read waited on slow replica: {elapsed:?}"
    );
}

#[tokio::test]
async fn metrics_endpoint_exposes_storage_counters() {
    let app = build_router(MemoryStorageEngine::new());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("metrics request should complete");

    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body should decode");
    let body = String::from_utf8(body.to_vec()).expect("utf8 body");

    assert!(body.contains("vt_build_info{"));
    assert!(body.contains("target_arch=\""));
    assert!(body.contains("vt_rows_ingested_total"));
    assert!(body.contains("vt_traces_tracked"));
    assert!(body.contains("vt_storage_retained_trace_blocks"));
    assert!(body.contains("vt_storage_segments"));
    assert!(body.contains("vt_storage_trace_head_segments"));
    assert!(body.contains("vt_storage_trace_head_rows"));
    assert!(body.contains("vt_storage_segment_read_batches_total"));
    assert!(body.contains("vt_storage_part_selective_decodes_total"));
    assert!(body.contains("vt_storage_trace_group_commit_flushes_total"));
    assert!(body.contains("vt_storage_trace_group_commit_rows_total"));
    assert!(body.contains("vt_storage_trace_group_commit_bytes_total"));
    assert!(body.contains("vt_storage_trace_batch_flushes_total"));
    assert!(body.contains("vt_storage_trace_batch_input_blocks_total"));
    assert!(body.contains("vt_storage_trace_live_update_queue_depth"));
    assert!(body.contains("vt_storage_trace_seal_queue_depth"));
    assert!(body.contains("vt_storage_trace_seal_completed_total"));
    assert!(body.contains("vt_storage_trace_seal_rows_total"));
    assert!(body.contains("vt_storage_trace_seal_bytes_total"));
}

#[tokio::test]
async fn services_and_search_endpoints_return_trace_metadata() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-search-http-1",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request");

    let services_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("services request");
    assert_eq!(services_response.status(), StatusCode::OK);

    let services_body = axum::body::to_bytes(services_response.into_body(), usize::MAX)
        .await
        .expect("services body");
    let services: Value = serde_json::from_slice(&services_body).expect("services json");
    assert_eq!(services["services"][0], "checkout");

    let search_response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/traces/search?start_unix_nano=50&end_unix_nano=200&service_name=checkout&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("search request");
    assert_eq!(search_response.status(), StatusCode::OK);

    let search_body = axum::body::to_bytes(search_response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let hits: Value = serde_json::from_slice(&search_body).expect("search json");
    assert_eq!(hits["hits"].as_array().unwrap().len(), 1);
    assert_eq!(hits["hits"][0]["trace_id"], "trace-search-http-1");
}

#[tokio::test]
async fn search_endpoint_filters_by_operation_name() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-search-operation-1",
                                "span_id": "span-op-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            },
                            {
                                "trace_id": "trace-search-operation-2",
                                "span_id": "span-op-2",
                                "parent_span_id": null,
                                "name": "POST /checkout",
                                "start_time_unix_nano": 120,
                                "end_time_unix_nano": 220,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request");

    let search_response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/traces/search?start_unix_nano=50&end_unix_nano=300&service_name=checkout&operation_name=POST%20/checkout&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("search request");
    assert_eq!(search_response.status(), StatusCode::OK);

    let search_body = axum::body::to_bytes(search_response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let hits: Value = serde_json::from_slice(&search_body).expect("search json");
    assert_eq!(hits["hits"].as_array().unwrap().len(), 1);
    assert_eq!(hits["hits"][0]["trace_id"], "trace-search-operation-2");
}

#[tokio::test]
async fn search_endpoint_filters_by_generic_field_filter() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-search-field-1",
                                "span_id": "span-field-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "GET" } }
                                ],
                                "status": null
                            },
                            {
                                "trace_id": "trace-search-field-2",
                                "span_id": "span-field-2",
                                "parent_span_id": null,
                                "name": "POST /checkout",
                                "start_time_unix_nano": 120,
                                "end_time_unix_nano": 220,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "POST" } }
                                ],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request");

    let search_response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/traces/search?start_unix_nano=50&end_unix_nano=300&service_name=checkout&field_filter=span_attr:http.method=POST&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("search request");
    assert_eq!(search_response.status(), StatusCode::OK);

    let search_body = axum::body::to_bytes(search_response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let hits: Value = serde_json::from_slice(&search_body).expect("search json");
    assert_eq!(hits["hits"].as_array().unwrap().len(), 1);
    assert_eq!(hits["hits"][0]["trace_id"], "trace-search-field-2");
}

#[tokio::test]
async fn tempo_endpoints_expose_search_tags_and_values() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } },
                    { "key": "deployment.environment", "value": { "kind": "string", "value": "prod" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-tempo-1",
                                "span_id": "span-tempo-1",
                                "parent_span_id": null,
                                "name": "POST /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 250,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "POST" } },
                                    { "key": "http.route", "value": { "kind": "string", "value": "/checkout" } }
                                ],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request");

    let search_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/search?q=%7Bresource.service.name%3D%22checkout%22%20%26%26%20span.http.method%3D%22POST%22%7D&start=0&end=1&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo search request");
    assert_eq!(search_response.status(), StatusCode::OK);
    let search_body = axum::body::to_bytes(search_response.into_body(), usize::MAX)
        .await
        .expect("tempo search body");
    let traces: Value = serde_json::from_slice(&search_body).expect("tempo search json");
    assert_eq!(traces["traces"].as_array().unwrap().len(), 1);
    assert_eq!(traces["traces"][0]["traceID"], "trace-tempo-1");
    assert_eq!(traces["traces"][0]["rootServiceName"], "checkout");
    assert_eq!(traces["traces"][0]["rootTraceName"], "POST /checkout");

    let tags_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/search/tags?scope=resource&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo tags request");
    assert_eq!(tags_response.status(), StatusCode::OK);
    let tags_body = axum::body::to_bytes(tags_response.into_body(), usize::MAX)
        .await
        .expect("tempo tags body");
    let tags: Value = serde_json::from_slice(&tags_body).expect("tempo tags json");
    assert!(tags["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "deployment.environment"));
    assert!(tags["tagNames"]
        .as_array()
        .unwrap()
        .iter()
        .any(|value| value == "service.name"));

    let scoped_tags_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v2/search/tags?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo scoped tags request");
    assert_eq!(scoped_tags_response.status(), StatusCode::OK);
    let scoped_tags_body = axum::body::to_bytes(scoped_tags_response.into_body(), usize::MAX)
        .await
        .expect("tempo scoped tags body");
    let scoped_tags: Value =
        serde_json::from_slice(&scoped_tags_body).expect("tempo scoped tags json");
    assert!(scoped_tags["scopes"]
        .as_array()
        .unwrap()
        .iter()
        .any(|scope| scope["name"] == "span"
            && scope["tags"]
                .as_array()
                .unwrap()
                .iter()
                .any(|tag| tag == "http.method")));

    let values_response = app
        .oneshot(
            Request::builder()
                .uri("/api/search/tag/span.http.method/values?limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo tag values request");
    assert_eq!(values_response.status(), StatusCode::OK);
    let values_body = axum::body::to_bytes(values_response.into_body(), usize::MAX)
        .await
        .expect("tempo tag values body");
    let values: Value = serde_json::from_slice(&values_body).expect("tempo tag values json");
    assert_eq!(
        values["tagValues"].as_array().unwrap(),
        &vec![json!("POST")]
    );
}

#[tokio::test]
async fn tempo_trace_endpoint_returns_json_and_protobuf() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": "checkout-scope",
                        "scope_version": "1.0.0",
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "1234567890abcdef1234567890abcdef",
                                "span_id": "0123456789abcdef",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 200,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "GET" } }
                                ],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request");

    let json_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/traces/1234567890abcdef1234567890abcdef")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo trace json request");
    assert_eq!(json_response.status(), StatusCode::OK);
    let json_body = axum::body::to_bytes(json_response.into_body(), usize::MAX)
        .await
        .expect("tempo trace json body");
    let trace_payload: Value = serde_json::from_slice(&json_body).expect("tempo trace json");
    assert_eq!(
        trace_payload["resource_spans"][0]["scope_spans"][0]["spans"][0]["trace_id"],
        "1234567890abcdef1234567890abcdef"
    );

    let protobuf_response = app
        .oneshot(
            Request::builder()
                .uri("/api/traces/1234567890abcdef1234567890abcdef")
                .header("accept", "application/protobuf")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("tempo trace protobuf request");
    assert_eq!(protobuf_response.status(), StatusCode::OK);
    assert_eq!(
        protobuf_response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok()),
        Some("application/protobuf")
    );
    let protobuf_body = axum::body::to_bytes(protobuf_response.into_body(), usize::MAX)
        .await
        .expect("tempo trace protobuf body");
    let export_request =
        decode_export_trace_service_request_protobuf(&protobuf_body).expect("decode protobuf");
    assert_eq!(export_request.resource_spans.len(), 1);
    assert_eq!(
        export_request.resource_spans[0].scope_spans[0].spans[0].trace_id,
        "1234567890abcdef1234567890abcdef"
    );
}

#[tokio::test]
async fn disk_backend_survives_router_restart() {
    let path = temp_test_dir("disk-router");
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-disk-http-1",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    {
        let storage = Arc::new(DiskStorageEngine::open(&path).expect("open disk engine"));
        let app = build_router_with_storage(storage);
        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/traces/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(ingest_payload.to_string()))
                    .unwrap(),
            )
            .await
            .expect("ingest request");
        assert_eq!(response.status(), StatusCode::OK);
    }

    {
        let storage = Arc::new(DiskStorageEngine::open(&path).expect("reopen disk engine"));
        let app = build_router_with_storage(storage);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/traces/trace-disk-http-1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("query request");
        assert_eq!(response.status(), StatusCode::OK);

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("body");
        let decoded: Value = serde_json::from_slice(&body).expect("json");
        assert_eq!(decoded["trace_id"], "trace-disk-http-1");
        assert_eq!(decoded["rows"].as_array().unwrap().len(), 1);
    }

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[tokio::test]
async fn disk_backend_query_endpoints_are_immediately_visible_for_stable_and_throughput_profiles() {
    for (label, profile, config) in [
        (
            "stable",
            TraceIngestProfile::Default,
            DiskStorageConfig::default()
                .with_sync_policy(DiskSyncPolicy::Data)
                .with_trace_shards(1)
                .with_target_segment_size_bytes(1 << 30),
        ),
        (
            "throughput",
            TraceIngestProfile::Throughput,
            DiskStorageConfig::default()
                .with_sync_policy(DiskSyncPolicy::None)
                .with_trace_shards(1)
                .with_target_segment_size_bytes(256 * 1024 * 1024)
                .with_trace_deferred_wal_writes(true),
        ),
    ] {
        let path = temp_test_dir(&format!("disk-query-profile-{label}"));
        let storage: Arc<dyn StorageEngine> =
            Arc::new(DiskStorageEngine::open_with_config(&path, config).expect("open disk"));
        let app = build_router_with_storage_and_trace_ingest_profile(storage, profile);

        let ingest_payload = json!({
            "resource_spans": [
                {
                    "resource_attributes": [
                        { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                    ],
                    "scope_spans": [
                        {
                            "scope_name": null,
                            "scope_version": null,
                            "scope_attributes": [],
                            "spans": [
                                {
                                    "trace_id": format!("trace-query-profile-{label}-1"),
                                    "span_id": "span-1",
                                    "parent_span_id": null,
                                    "name": "GET /checkout",
                                    "start_time_unix_nano": 100,
                                    "end_time_unix_nano": 180,
                                    "attributes": [
                                        { "key": "http.method", "value": { "kind": "string", "value": "GET" } }
                                    ],
                                    "status": null
                                },
                                {
                                    "trace_id": format!("trace-query-profile-{label}-2"),
                                    "span_id": "span-2",
                                    "parent_span_id": null,
                                    "name": "POST /checkout",
                                    "start_time_unix_nano": 200,
                                    "end_time_unix_nano": 320,
                                    "attributes": [
                                        { "key": "http.method", "value": { "kind": "string", "value": "POST" } }
                                    ],
                                    "status": null
                                }
                            ]
                        }
                    ]
                }
            ]
        });

        let ingest_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/traces/ingest")
                    .header("content-type", "application/json")
                    .body(Body::from(ingest_payload.to_string()))
                    .unwrap(),
            )
            .await
            .expect("ingest request");
        assert_eq!(ingest_response.status(), StatusCode::OK);

        let trace_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/api/v1/traces/trace-query-profile-{label}-2"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("trace request");
        assert_eq!(trace_response.status(), StatusCode::OK);
        let trace_body = axum::body::to_bytes(trace_response.into_body(), usize::MAX)
            .await
            .expect("trace body");
        let trace: Value = serde_json::from_slice(&trace_body).expect("trace json");
        assert_eq!(trace["trace_id"], format!("trace-query-profile-{label}-2"));
        assert_eq!(trace["rows"].as_array().unwrap().len(), 1);

        let services_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/services")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("services request");
        assert_eq!(services_response.status(), StatusCode::OK);
        let services_body = axum::body::to_bytes(services_response.into_body(), usize::MAX)
            .await
            .expect("services body");
        let services: Value = serde_json::from_slice(&services_body).expect("services json");
        assert_eq!(
            services["services"].as_array().unwrap(),
            &vec![json!("checkout")]
        );

        let search_response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/traces/search?start_unix_nano=0&end_unix_nano=500&service_name=checkout&field_filter=span_attr:http.method=POST&limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("search request");
        assert_eq!(search_response.status(), StatusCode::OK);
        let search_body = axum::body::to_bytes(search_response.into_body(), usize::MAX)
            .await
            .expect("search body");
        let search_hits: Value = serde_json::from_slice(&search_body).expect("search json");
        assert_eq!(search_hits["hits"].as_array().unwrap().len(), 1);
        assert_eq!(
            search_hits["hits"][0]["trace_id"],
            format!("trace-query-profile-{label}-2")
        );

        let values_response = app
            .oneshot(
                Request::builder()
                    .uri("/api/search/tag/span.http.method/values?limit=10")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .expect("tempo tag values request");
        assert_eq!(values_response.status(), StatusCode::OK);
        let values_body = axum::body::to_bytes(values_response.into_body(), usize::MAX)
            .await
            .expect("tempo tag values body");
        let values: Value = serde_json::from_slice(&values_body).expect("tempo tag values json");
        assert_eq!(
            values["tagValues"].as_array().unwrap(),
            &vec![json!("GET"), json!("POST")]
        );

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }
}

#[tokio::test]
async fn cluster_insert_and_select_round_trip_with_replication() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let storage_b = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), storage_b.base_url.clone()],
        2,
    )
    .expect("cluster config");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-cluster-1",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    let query_response = client
        .get(select.url("/api/v1/traces/trace-cluster-1"))
        .send()
        .await
        .expect("cluster query should succeed");
    assert_eq!(query_response.status(), ReqwestStatusCode::OK);

    let query_body: Value = query_response.json().await.expect("query json");
    assert_eq!(query_body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(query_body["rows"][0]["trace_id"], "trace-cluster-1");

    for storage in [&storage_a, &storage_b] {
        let replica_response = client
            .get(storage.url("/internal/v1/traces/trace-cluster-1"))
            .send()
            .await
            .expect("replica query");
        assert_eq!(replica_response.status(), ReqwestStatusCode::OK);
        let replica_body: Value = replica_response.json().await.expect("replica json");
        assert_eq!(replica_body["rows"].as_array().unwrap().len(), 1);
    }

    let services_response = client
        .get(select.url("/api/v1/services"))
        .send()
        .await
        .expect("services request");
    assert_eq!(services_response.status(), ReqwestStatusCode::OK);
    let services_body: Value = services_response.json().await.expect("services json");
    assert_eq!(services_body["services"][0], "checkout");

    let search_response = client
        .get(select.url(
            "/api/v1/traces/search?start_unix_nano=50&end_unix_nano=200&service_name=checkout&limit=10",
        ))
        .send()
        .await
        .expect("search request");
    assert_eq!(search_response.status(), ReqwestStatusCode::OK);
    let search_body: Value = search_response.json().await.expect("search json");
    assert_eq!(search_body["hits"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn cluster_select_reads_from_replica_when_primary_is_down() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let storage_b = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), storage_b.base_url.clone()],
        2,
    )
    .expect("cluster config");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "payments" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-cluster-failover-1",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "POST /pay",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    storage_a.handle.abort();

    let query_response = client
        .get(select.url("/api/v1/traces/trace-cluster-failover-1"))
        .send()
        .await
        .expect("cluster query should succeed");
    assert_eq!(query_response.status(), ReqwestStatusCode::OK);

    let query_body: Value = query_response.json().await.expect("query json");
    assert_eq!(query_body["rows"].as_array().unwrap().len(), 1);
    assert_eq!(
        query_body["rows"][0]["trace_id"],
        "trace-cluster-failover-1"
    );
}

#[tokio::test]
async fn cluster_metrics_report_remote_io_counters() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let storage_b = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), storage_b.base_url.clone()],
        2,
    )
    .expect("cluster config");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "catalog" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-cluster-metrics-1",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /catalog",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    let query_response = client
        .get(select.url("/api/v1/traces/trace-cluster-metrics-1"))
        .send()
        .await
        .expect("cluster query should succeed");
    assert_eq!(query_response.status(), ReqwestStatusCode::OK);

    let insert_metrics = client
        .get(insert.url("/metrics"))
        .send()
        .await
        .expect("insert metrics request");
    assert_eq!(insert_metrics.status(), ReqwestStatusCode::OK);
    let insert_metrics_body = insert_metrics.text().await.expect("insert metrics body");
    assert!(insert_metrics_body.contains("vt_cluster_replication_factor 2"));
    assert!(insert_metrics_body.contains("vt_remote_write_batches_total 2"));
    assert!(insert_metrics_body.contains("vt_remote_write_rows_total 2"));

    let select_metrics = client
        .get(select.url("/metrics"))
        .send()
        .await
        .expect("select metrics request");
    assert_eq!(select_metrics.status(), ReqwestStatusCode::OK);
    let select_metrics_body = select_metrics.text().await.expect("select metrics body");
    assert!(select_metrics_body.contains("vt_remote_read_requests_total 2"));
}

#[tokio::test]
async fn cluster_insert_accepts_partial_replica_failure_when_quorum_is_met() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let unavailable_node = "http://127.0.0.1:9".to_string();
    let cluster = ClusterConfig::new(vec![storage_a.base_url.clone(), unavailable_node], 2)
        .expect("cluster config")
        .with_write_quorum(1)
        .expect("write quorum");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "quorum" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-cluster-quorum-1",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /quorum",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should complete");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    let query_response = client
        .get(select.url("/api/v1/traces/trace-cluster-quorum-1"))
        .send()
        .await
        .expect("cluster query should succeed");
    assert_eq!(query_response.status(), ReqwestStatusCode::OK);

    let insert_metrics = client
        .get(insert.url("/metrics"))
        .send()
        .await
        .expect("insert metrics request");
    let insert_metrics_body = insert_metrics.text().await.expect("insert metrics body");
    assert!(insert_metrics_body.contains("vt_cluster_write_quorum 1"));
    assert!(insert_metrics_body.contains("vt_remote_write_errors_total 1"));
}

#[tokio::test]
async fn cluster_insert_rejects_when_write_quorum_is_not_met() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    storage_a.handle.abort();

    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), "http://127.0.0.1:9".to_string()],
        2,
    )
    .expect("cluster config")
    .with_write_quorum(2)
    .expect("write quorum");

    let insert = spawn_server(build_insert_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "quorum" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-cluster-quorum-2",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /quorum",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should complete");
    assert_eq!(
        ingest_response.status(),
        ReqwestStatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn cluster_select_quarantines_failed_node_between_reads() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let storage_b = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), storage_b.base_url.clone()],
        2,
    )
    .expect("cluster config")
    .with_failure_backoff(Duration::from_secs(60));

    let failing_trace_id = (0..1_024)
        .map(|index| format!("trace-cluster-health-{index}"))
        .find(|trace_id| cluster.placements(trace_id)[0] == storage_a.base_url.as_str())
        .expect("trace id that places dead node first");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "health" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": failing_trace_id,
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /health",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    storage_a.handle.abort();

    for _ in 0..2 {
        let query_response = client
            .get(select.url(&format!("/api/v1/traces/{failing_trace_id}")))
            .send()
            .await
            .expect("cluster query should succeed");
        assert_eq!(query_response.status(), ReqwestStatusCode::OK);
    }

    let select_metrics = client
        .get(select.url("/metrics"))
        .send()
        .await
        .expect("select metrics request");
    assert_eq!(select_metrics.status(), ReqwestStatusCode::OK);
    let select_metrics_body = select_metrics.text().await.expect("select metrics body");
    assert!(select_metrics_body.contains("vt_remote_read_requests_total 3"));
    assert!(select_metrics_body.contains("vt_remote_read_errors_total 1"));
    assert!(select_metrics_body.contains("vt_remote_read_skips_total 1"));
    assert!(select_metrics_body.contains("vt_cluster_unhealthy_nodes 1"));
}

#[tokio::test]
async fn cluster_select_enforces_read_quorum_for_trace_reads() {
    let storage_a = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let storage_b = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let cluster = ClusterConfig::new(
        vec![storage_a.base_url.clone(), storage_b.base_url.clone()],
        2,
    )
    .expect("cluster config")
    .with_read_quorum(2)
    .expect("read quorum");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-cluster-read-quorum-1",
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    storage_b.handle.abort();

    let query_response = client
        .get(select.url("/api/v1/traces/trace-cluster-read-quorum-1"))
        .send()
        .await
        .expect("cluster query should complete");
    assert_eq!(query_response.status(), ReqwestStatusCode::BAD_GATEWAY);
}

#[tokio::test]
async fn cluster_select_repairs_missing_replica_after_successful_read() {
    let storage_a_addr = reserve_addr();
    let storage_b_addr = reserve_addr();
    let storage_a = spawn_server_at(
        storage_a_addr,
        build_storage_router(Arc::new(MemoryStorageEngine::new())),
    )
    .await;

    let cluster = ClusterConfig::new(
        vec![
            format!("http://{storage_a_addr}"),
            format!("http://{storage_b_addr}"),
        ],
        2,
    )
    .expect("cluster config")
    .with_write_quorum(1)
    .expect("write quorum")
    .with_failure_backoff(Duration::from_millis(10));

    let repair_trace_id = (0..1_024)
        .map(|index| format!("trace-cluster-repair-{index}"))
        .find(|trace_id| cluster.placements(trace_id)[0] == format!("http://{storage_b_addr}"))
        .expect("trace id with missing replica first");

    let insert = spawn_server(build_insert_router(cluster.clone())).await;
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "repair" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": repair_trace_id,
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /repair",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    let storage_b = spawn_server_at(
        storage_b_addr,
        build_storage_router(Arc::new(MemoryStorageEngine::new())),
    )
    .await;

    let query_response = client
        .get(select.url(&format!("/api/v1/traces/{repair_trace_id}")))
        .send()
        .await
        .expect("cluster query should succeed");
    assert_eq!(query_response.status(), ReqwestStatusCode::OK);

    let repaired_replica_response = client
        .get(storage_b.url(&format!("/internal/v1/traces/{repair_trace_id}")))
        .send()
        .await
        .expect("replica query");
    assert_eq!(repaired_replica_response.status(), ReqwestStatusCode::OK);
    let repaired_body: Value = repaired_replica_response
        .json()
        .await
        .expect("repaired replica json");
    assert_eq!(repaired_body["rows"].as_array().unwrap().len(), 1);

    let select_metrics = client
        .get(select.url("/metrics"))
        .send()
        .await
        .expect("select metrics request");
    let select_metrics_body = select_metrics.text().await.expect("select metrics body");
    assert!(select_metrics_body.contains("vt_read_repairs_total 1"));

    drop(storage_a);
    drop(storage_b);
}

#[tokio::test]
async fn cluster_rebalance_repairs_missing_desired_replica() {
    let storage_a_addr = reserve_addr();
    let storage_b_addr = reserve_addr();
    let storage_c_addr = reserve_addr();

    let _storage_a = spawn_server_at(
        storage_a_addr,
        build_storage_router(Arc::new(MemoryStorageEngine::new())),
    )
    .await;
    let _storage_b = spawn_server_at(
        storage_b_addr,
        build_storage_router(Arc::new(MemoryStorageEngine::new())),
    )
    .await;
    let storage_c = spawn_server_at(
        storage_c_addr,
        build_storage_router(Arc::new(MemoryStorageEngine::new())),
    )
    .await;

    let old_cluster = ClusterConfig::new(
        vec![
            format!("http://{storage_a_addr}"),
            format!("http://{storage_b_addr}"),
        ],
        1,
    )
    .expect("old cluster");
    let new_cluster = ClusterConfig::new(
        vec![
            format!("http://{storage_a_addr}"),
            format!("http://{storage_b_addr}"),
            format!("http://{storage_c_addr}"),
        ],
        2,
    )
    .expect("new cluster");

    let rebalance_trace_id = (0..4_096)
        .map(|index| format!("trace-cluster-rebalance-{index}"))
        .find(|trace_id| {
            let old_owner = old_cluster.placements(trace_id)[0];
            let new_placements = new_cluster.placements(trace_id);
            new_placements.contains(&old_owner)
                && new_placements.contains(&format!("http://{storage_c_addr}").as_str())
        })
        .expect("trace id with desired new replica");

    let insert = spawn_server(build_insert_router(old_cluster)).await;
    let select = spawn_server(build_select_router(new_cluster)).await;
    let client = Client::new();
    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "rebalance" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": rebalance_trace_id,
                                "span_id": "span-1",
                                "parent_span_id": null,
                                "name": "GET /rebalance",
                                "start_time_unix_nano": 100,
                                "end_time_unix_nano": 180,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = client
        .post(insert.url("/api/v1/traces/ingest"))
        .json(&ingest_payload)
        .send()
        .await
        .expect("cluster ingest should succeed");
    assert_eq!(ingest_response.status(), ReqwestStatusCode::OK);

    let rebalance_response = client
        .post(select.url("/admin/v1/cluster/rebalance"))
        .send()
        .await
        .expect("rebalance request");
    assert_eq!(rebalance_response.status(), ReqwestStatusCode::OK);
    let rebalance_body: Value = rebalance_response.json().await.expect("rebalance json");
    assert_eq!(rebalance_body["repaired_replicas"], 1);

    let repaired_replica_response = client
        .get(storage_c.url(&format!("/internal/v1/traces/{rebalance_trace_id}")))
        .send()
        .await
        .expect("replica query");
    assert_eq!(repaired_replica_response.status(), ReqwestStatusCode::OK);
    let repaired_body: Value = repaired_replica_response
        .json()
        .await
        .expect("repaired replica json");
    assert_eq!(repaired_body["rows"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn jaeger_endpoints_expose_services_operations_and_trace_payloads() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } },
                    { "key": "deployment.environment", "value": { "kind": "string", "value": "prod" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": "io.opentelemetry.auto",
                        "scope_version": "1.0.0",
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-jaeger-1",
                                "span_id": "span-jaeger-root",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100000,
                                "end_time_unix_nano": 180000,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "GET" } }
                                ],
                                "status": { "code": 0, "message": "OK" }
                            },
                            {
                                "trace_id": "trace-jaeger-1",
                                "span_id": "span-jaeger-child",
                                "parent_span_id": "span-jaeger-root",
                                "name": "SELECT cart_items",
                                "start_time_unix_nano": 120000,
                                "end_time_unix_nano": 160000,
                                "attributes": [
                                    { "key": "db.system", "value": { "kind": "string", "value": "postgresql" } }
                                ],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request should succeed");
    assert_eq!(ingest_response.status(), StatusCode::OK);

    let services_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/services")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger services request should succeed");
    assert_eq!(services_response.status(), StatusCode::OK);
    let services_body = axum::body::to_bytes(services_response.into_body(), usize::MAX)
        .await
        .expect("services body");
    let services: Value = serde_json::from_slice(&services_body).expect("services json");
    assert_eq!(
        services["data"].as_array().unwrap(),
        &vec![json!("checkout")]
    );
    assert_eq!(services["total"], 1);
    assert!(services["errors"].is_null());

    let operations_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/services/checkout/operations")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger operations request should succeed");
    assert_eq!(operations_response.status(), StatusCode::OK);
    let operations_body = axum::body::to_bytes(operations_response.into_body(), usize::MAX)
        .await
        .expect("operations body");
    let operations: Value = serde_json::from_slice(&operations_body).expect("operations json");
    assert_eq!(
        operations["data"].as_array().unwrap(),
        &vec![json!("GET /checkout"), json!("SELECT cart_items")]
    );
    assert_eq!(operations["total"], 2);

    let trace_response = app
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/traces/trace-jaeger-1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger trace request should succeed");
    assert_eq!(trace_response.status(), StatusCode::OK);
    let trace_body = axum::body::to_bytes(trace_response.into_body(), usize::MAX)
        .await
        .expect("trace body");
    let trace: Value = serde_json::from_slice(&trace_body).expect("trace json");
    assert_eq!(trace["total"], 1);
    assert!(trace["errors"].is_null());
    assert_eq!(trace["data"].as_array().unwrap().len(), 1);
    assert_eq!(trace["data"][0]["spans"].as_array().unwrap().len(), 2);
    assert_eq!(trace["data"][0]["spans"][0]["traceID"], "trace-jaeger-1");
    assert_eq!(
        trace["data"][0]["processes"]["p1"]["serviceName"],
        "checkout"
    );
}

#[tokio::test]
async fn jaeger_trace_search_filters_by_service_and_operation() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-jaeger-search-1",
                                "span_id": "span-jaeger-search-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100000,
                                "end_time_unix_nano": 180000,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            },
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "payments" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-jaeger-search-2",
                                "span_id": "span-jaeger-search-2",
                                "parent_span_id": null,
                                "name": "POST /pay",
                                "start_time_unix_nano": 100000,
                                "end_time_unix_nano": 170000,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request should succeed");
    assert_eq!(ingest_response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/traces?service=checkout&operation=GET%20%2Fcheckout&start=50&end=200&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger traces request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let decoded: Value = serde_json::from_slice(&body).expect("search json");
    assert_eq!(decoded["total"], 1);
    assert!(decoded["errors"].is_null());
    assert_eq!(decoded["data"].as_array().unwrap().len(), 1);
    assert_eq!(
        decoded["data"][0]["spans"][0]["traceID"],
        "trace-jaeger-search-1"
    );
    assert_eq!(
        decoded["data"][0]["spans"][0]["operationName"],
        "GET /checkout"
    );
}

#[tokio::test]
async fn jaeger_trace_lookup_returns_not_found_envelope_for_missing_trace() {
    let app = build_router(MemoryStorageEngine::new());

    let response = app
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/traces/missing-trace")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger trace request should succeed");
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    let decoded: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(decoded["data"].as_array().unwrap().len(), 0);
    assert_eq!(decoded["total"], 0);
    assert_eq!(decoded["errors"][0]["code"], 404);
}

#[tokio::test]
async fn official_victoria_traces_insert_path_accepts_standard_otlp_json() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        { "key": "service.name", "value": { "stringValue": "checkout" } },
                        { "key": "deployment.environment", "value": { "stringValue": "prod" } }
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {
                            "name": "wtrace-legacy-migration",
                            "version": "1.0.0"
                        },
                        "spans": [
                            {
                                "traceId": "1234567890abcdef1234567890abcdef",
                                "spanId": "0123456789abcdef",
                                "parentSpanId": "",
                                "name": "GET /checkout",
                                "kind": 2,
                                "startTimeUnixNano": "100000",
                                "endTimeUnixNano": "180000",
                                "attributes": [
                                    { "key": "http.method", "value": { "stringValue": "GET" } },
                                    { "key": "http.status_code", "value": { "intValue": "200" } },
                                    { "key": "cache.hit", "value": { "boolValue": true } }
                                ],
                                "events": [
                                    {
                                        "name": "annotation",
                                        "timeUnixNano": "140000",
                                        "attributes": [
                                            { "key": "annotation.present", "value": { "boolValue": true } }
                                        ]
                                    }
                                ],
                                "status": { "code": 1 }
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/insert/opentelemetry/v1/traces")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("official insert request should succeed");
    assert_eq!(ingest_response.status(), StatusCode::OK);

    let trace_response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/traces/1234567890abcdef1234567890abcdef")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("trace lookup should succeed");
    assert_eq!(trace_response.status(), StatusCode::OK);

    let trace_body = axum::body::to_bytes(trace_response.into_body(), usize::MAX)
        .await
        .expect("trace body");
    let trace: Value = serde_json::from_slice(&trace_body).expect("trace json");
    assert_eq!(
        trace["resource_spans"][0]["resource_attributes"][0]["key"],
        "deployment.environment"
    );
    assert_eq!(
        trace["resource_spans"][0]["scope_spans"][0]["spans"][0]["trace_id"],
        "1234567890abcdef1234567890abcdef"
    );
    assert_eq!(
        trace["resource_spans"][0]["scope_spans"][0]["spans"][0]["attributes"][0]["key"],
        "cache.hit"
    );
}

#[tokio::test]
async fn jaeger_trace_search_supports_tags_and_duration_filters() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-jaeger-filter-1",
                                "span_id": "span-jaeger-filter-1",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100000,
                                "end_time_unix_nano": 180000,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "GET" } }
                                ],
                                "status": null
                            },
                            {
                                "trace_id": "trace-jaeger-filter-2",
                                "span_id": "span-jaeger-filter-2",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100000,
                                "end_time_unix_nano": 260000,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "GET" } }
                                ],
                                "status": null
                            },
                            {
                                "trace_id": "trace-jaeger-filter-3",
                                "span_id": "span-jaeger-filter-3",
                                "parent_span_id": null,
                                "name": "GET /checkout",
                                "start_time_unix_nano": 100000,
                                "end_time_unix_nano": 180000,
                                "attributes": [
                                    { "key": "http.method", "value": { "kind": "string", "value": "POST" } }
                                ],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request should succeed");
    assert_eq!(ingest_response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/traces?service=checkout&tags=%7B%22http.method%22%3A%22GET%22%7D&minDuration=50us&maxDuration=90us&limit=10")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger traces request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let decoded: Value = serde_json::from_slice(&body).expect("search json");
    assert_eq!(decoded["total"], 1);
    assert_eq!(
        decoded["data"][0]["traceID"],
        "trace-jaeger-filter-1"
    );
}

#[tokio::test]
async fn jaeger_trace_search_overfetches_for_duration_filtered_matches() {
    let app = build_router(MemoryStorageEngine::new());

    let mut spans = Vec::new();
    for index in 0..10 {
        spans.push(json!({
            "trace_id": format!("trace-jaeger-duration-slow-{index}"),
            "span_id": format!("span-jaeger-duration-slow-{index}"),
            "parent_span_id": null,
            "name": "GET /checkout",
            "start_time_unix_nano": 1_000_000 + index * 10_000,
            "end_time_unix_nano": 1_250_000 + index * 10_000,
            "attributes": [],
            "status": null
        }));
    }
    for index in 0..5 {
        spans.push(json!({
            "trace_id": format!("trace-jaeger-duration-fast-{index}"),
            "span_id": format!("span-jaeger-duration-fast-{index}"),
            "parent_span_id": null,
            "name": "GET /checkout",
            "start_time_unix_nano": 100_000 + index * 10_000,
            "end_time_unix_nano": 180_000 + index * 10_000,
            "attributes": [],
            "status": null
        }));
    }

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": spans
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request should succeed");
    assert_eq!(ingest_response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/traces?service=checkout&minDuration=50us&maxDuration=100us&limit=5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger traces request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("search body");
    let decoded: Value = serde_json::from_slice(&body).expect("search json");
    assert_eq!(decoded["total"], 5);
    let returned_trace_ids: std::collections::BTreeSet<_> = decoded["data"]
        .as_array()
        .expect("jaeger data array")
        .iter()
        .map(|trace| {
            trace["traceID"]
                .as_str()
                .expect("traceID")
                .to_string()
        })
        .collect();
    let expected_trace_ids: std::collections::BTreeSet<_> = (0..5)
        .map(|index| format!("trace-jaeger-duration-fast-{index}"))
        .collect();
    assert_eq!(returned_trace_ids, expected_trace_ids);
}

#[tokio::test]
async fn jaeger_dependencies_endpoint_returns_service_edges() {
    let app = build_router(MemoryStorageEngine::new());

    let ingest_payload = json!({
        "resource_spans": [
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "frontend" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-jaeger-deps-1",
                                "span_id": "span-root",
                                "parent_span_id": null,
                                "name": "GET /",
                                "start_time_unix_nano": 1_000_000,
                                "end_time_unix_nano": 2_000_000,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            },
            {
                "resource_attributes": [
                    { "key": "service.name", "value": { "kind": "string", "value": "checkout" } }
                ],
                "scope_spans": [
                    {
                        "scope_name": null,
                        "scope_version": null,
                        "scope_attributes": [],
                        "spans": [
                            {
                                "trace_id": "trace-jaeger-deps-1",
                                "span_id": "span-child",
                                "parent_span_id": "span-root",
                                "name": "POST /checkout",
                                "start_time_unix_nano": 1_200_000,
                                "end_time_unix_nano": 1_800_000,
                                "attributes": [],
                                "status": null
                            }
                        ]
                    }
                ]
            }
        ]
    });

    let ingest_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/api/v1/traces/ingest")
                .header("content-type", "application/json")
                .body(Body::from(ingest_payload.to_string()))
                .unwrap(),
        )
        .await
        .expect("ingest request should succeed");
    assert_eq!(ingest_response.status(), StatusCode::OK);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/select/jaeger/api/dependencies?endTs=2000&lookback=10000")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .expect("jaeger dependencies request should succeed");
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("dependencies body");
    let decoded: Value = serde_json::from_slice(&body).expect("dependencies json");
    assert_eq!(decoded.as_array().unwrap().len(), 1);
    assert_eq!(decoded[0]["parent"], "frontend");
    assert_eq!(decoded[0]["child"], "checkout");
    assert_eq!(decoded[0]["callCount"], 1);
}

#[tokio::test]
async fn tls_server_accepts_https_requests() {
    let materials = generate_tls_materials();
    let tls_config = ServerTlsConfig::from_pem(
        materials.server_cert_pem.as_bytes().to_vec(),
        materials.server_key_pem.as_bytes().to_vec(),
    );
    let server = spawn_tls_server(build_router(MemoryStorageEngine::new()), tls_config).await;
    let client = Client::builder()
        .add_root_certificate(
            reqwest::Certificate::from_pem(materials.ca_cert_pem.as_bytes())
                .expect("load CA certificate"),
        )
        .build()
        .expect("build HTTPS client");

    let response = client
        .get(server.url("/healthz"))
        .send()
        .await
        .expect("HTTPS request should succeed");
    assert_eq!(response.status(), ReqwestStatusCode::OK);
}

#[tokio::test]
async fn tls_server_reloads_rotated_certificate_files() {
    let materials_a = generate_tls_materials();
    let materials_b = generate_tls_materials();
    let tls_dir = temp_test_dir("tls-reload");
    let cert_path = tls_dir.join("server.crt");
    let key_path = tls_dir.join("server.key");
    fs::write(&cert_path, materials_a.server_cert_pem.as_bytes()).expect("write initial cert");
    fs::write(&key_path, materials_a.server_key_pem.as_bytes()).expect("write initial key");

    let tls_config = ServerTlsConfig::from_pem_files(&cert_path, &key_path)
        .with_reload_interval(Duration::from_millis(50));
    let server = spawn_tls_server(build_router(MemoryStorageEngine::new()), tls_config).await;

    let initial_client = Client::builder()
        .add_root_certificate(
            reqwest::Certificate::from_pem(materials_a.ca_cert_pem.as_bytes())
                .expect("load initial CA certificate"),
        )
        .build()
        .expect("build initial HTTPS client");
    let initial_response = initial_client
        .get(server.url("/healthz"))
        .send()
        .await
        .expect("initial HTTPS request should succeed");
    assert_eq!(initial_response.status(), ReqwestStatusCode::OK);

    fs::write(&cert_path, materials_b.server_cert_pem.as_bytes()).expect("write rotated cert");
    fs::write(&key_path, materials_b.server_key_pem.as_bytes()).expect("write rotated key");

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut reload_observed = false;
    while Instant::now() < deadline {
        let new_client = Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(materials_b.ca_cert_pem.as_bytes())
                    .expect("load rotated CA certificate"),
            )
            .build()
            .expect("build rotated HTTPS client");
        let old_client = Client::builder()
            .add_root_certificate(
                reqwest::Certificate::from_pem(materials_a.ca_cert_pem.as_bytes())
                    .expect("load original CA certificate"),
            )
            .build()
            .expect("build original HTTPS client");

        let new_client_ok = new_client
            .get(server.url("/healthz"))
            .send()
            .await
            .map(|response| response.status() == ReqwestStatusCode::OK)
            .unwrap_or(false);
        let old_client_failed = old_client.get(server.url("/healthz")).send().await.is_err();

        if new_client_ok && old_client_failed {
            reload_observed = true;
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        reload_observed,
        "rotated certificate was not observed in time"
    );
}

#[tokio::test]
async fn mtls_server_requires_client_identity() {
    let materials = generate_tls_materials();
    let tls_config = ServerTlsConfig::from_pem(
        materials.server_cert_pem.as_bytes().to_vec(),
        materials.server_key_pem.as_bytes().to_vec(),
    )
    .with_client_ca_pem(materials.ca_cert_pem.as_bytes().to_vec());
    let server = spawn_tls_server(build_router(MemoryStorageEngine::new()), tls_config).await;
    let ca_certificate = reqwest::Certificate::from_pem(materials.ca_cert_pem.as_bytes())
        .expect("load CA certificate");

    let unauthenticated_client = Client::builder()
        .add_root_certificate(ca_certificate.clone())
        .build()
        .expect("build unauthenticated client");
    let unauthenticated_result = unauthenticated_client
        .get(server.url("/healthz"))
        .send()
        .await;
    assert!(unauthenticated_result.is_err());

    let mut identity_pem = materials.client_cert_pem.as_bytes().to_vec();
    identity_pem.extend(materials.client_key_pem.as_bytes());
    let authenticated_client = Client::builder()
        .add_root_certificate(ca_certificate)
        .identity(reqwest::Identity::from_pem(&identity_pem).expect("build client identity"))
        .build()
        .expect("build authenticated client");

    let response = authenticated_client
        .get(server.url("/healthz"))
        .send()
        .await
        .expect("mTLS request should succeed");
    assert_eq!(response.status(), ReqwestStatusCode::OK);
}

#[tokio::test]
async fn cluster_client_reloads_rotated_mtls_identity_without_restart() {
    let materials = generate_tls_materials();
    let wrong_identity = generate_tls_materials();
    let storage: Arc<dyn StorageEngine> = Arc::new(MemoryStorageEngine::new());
    storage
        .append_rows(vec![TraceSpanRow::new(
            "trace-cluster-mtls-reload",
            "span-cluster-mtls-reload",
            None,
            "GET /cluster-client-reload",
            10,
            20,
            vec![vtcore::Field::new(
                "resource_attr:service.name",
                "cluster-client-reload",
            )],
        )
        .expect("build trace row")])
        .expect("seed storage");
    let storage_tls = ServerTlsConfig::from_pem(
        materials.server_cert_pem.as_bytes().to_vec(),
        materials.server_key_pem.as_bytes().to_vec(),
    )
    .with_client_ca_pem(materials.ca_cert_pem.as_bytes().to_vec());
    let storage_server = spawn_tls_server(build_storage_router(storage), storage_tls).await;

    let tls_dir = temp_test_dir("cluster-client-reload");
    let ca_cert_path = tls_dir.join("cluster-ca.pem");
    let client_cert_path = tls_dir.join("cluster-client.crt");
    let client_key_path = tls_dir.join("cluster-client.key");
    fs::write(&ca_cert_path, materials.ca_cert_pem.as_bytes()).expect("write cluster CA");
    fs::write(&client_cert_path, wrong_identity.client_cert_pem.as_bytes())
        .expect("write wrong client cert");
    fs::write(&client_key_path, wrong_identity.client_key_pem.as_bytes())
        .expect("write wrong client key");

    let cluster_client = ClusterHttpClient::from_reloading_config(
        ClusterHttpClientConfig::new(Duration::from_millis(50))
            .with_ca_cert_path(&ca_cert_path)
            .with_client_identity_paths(&client_cert_path, &client_key_path),
    )
    .expect("build reloading cluster client");
    let cluster = ClusterConfig::new(vec![storage_server.base_url.clone()], 1)
        .expect("cluster config")
        .with_failure_backoff(Duration::from_millis(50));
    let select = spawn_server(build_select_router_with_client_auth_and_limits(
        cluster,
        cluster_client,
        AuthConfig::default(),
        ApiLimitsConfig::default(),
    ))
    .await;

    let control_client = Client::new();
    let initial_response = control_client
        .get(select.url("/api/v1/services"))
        .send()
        .await
        .expect("initial select request should complete");
    assert_eq!(initial_response.status(), ReqwestStatusCode::BAD_GATEWAY);

    fs::write(&client_cert_path, materials.client_cert_pem.as_bytes())
        .expect("write rotated client cert");
    fs::write(&client_key_path, materials.client_key_pem.as_bytes())
        .expect("write rotated client key");

    let deadline = Instant::now() + Duration::from_secs(3);
    let mut reload_observed = false;
    while Instant::now() < deadline {
        let response = control_client
            .get(select.url("/api/v1/services"))
            .send()
            .await
            .expect("select request should complete");
        if response.status() == ReqwestStatusCode::OK {
            let body: Value = response.json().await.expect("services json");
            if body["services"]
                .as_array()
                .into_iter()
                .flatten()
                .any(|service| service == "cluster-client-reload")
            {
                reload_observed = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    assert!(
        reload_observed,
        "reloaded cluster client identity was not observed in time"
    );
}

#[tokio::test]
async fn cluster_members_endpoint_reports_node_health_and_topology() {
    let storage = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let dead_node = format!("http://{}", reserve_addr());
    let mut topology_groups = std::collections::HashMap::new();
    topology_groups.insert(storage.base_url.clone(), "az-a".to_string());
    topology_groups.insert(dead_node.clone(), "az-b".to_string());
    let mut node_weights = std::collections::HashMap::new();
    node_weights.insert(storage.base_url.clone(), 3);
    node_weights.insert(dead_node.clone(), 1);

    let cluster = ClusterConfig::new(vec![storage.base_url.clone(), dead_node.clone()], 2)
        .expect("cluster config")
        .with_topology_groups(topology_groups)
        .with_node_weights(node_weights);
    let select = spawn_server(build_select_router(cluster)).await;
    let client = Client::new();

    let response = client
        .get(select.url("/admin/v1/cluster/members"))
        .send()
        .await
        .expect("cluster members request should succeed");
    assert_eq!(response.status(), ReqwestStatusCode::OK);
    let body: Value = response.json().await.expect("cluster members json");
    let members = body["members"].as_array().expect("members array");
    assert_eq!(members.len(), 2);

    let healthy_member = members
        .iter()
        .find(|member| member["node"] == storage.base_url)
        .expect("healthy member");
    assert_eq!(healthy_member["observed"], true);
    assert_eq!(healthy_member["healthy"], true);
    assert_eq!(healthy_member["quarantined"], false);
    assert_eq!(healthy_member["topology_group"], "az-a");
    assert_eq!(healthy_member["weight"], 3);

    let unhealthy_member = members
        .iter()
        .find(|member| member["node"] == dead_node)
        .expect("unhealthy member");
    assert_eq!(unhealthy_member["observed"], true);
    assert_eq!(unhealthy_member["healthy"], false);
    assert_eq!(unhealthy_member["quarantined"], true);
    assert_eq!(unhealthy_member["topology_group"], "az-b");
    assert_eq!(unhealthy_member["weight"], 1);
    assert!(unhealthy_member["last_error"].as_str().is_some());

    let metrics_body = client
        .get(select.url("/metrics"))
        .send()
        .await
        .expect("metrics request should succeed")
        .text()
        .await
        .expect("metrics body");
    assert!(metrics_body.contains("vt_cluster_membership_probes_total 2"));
    assert!(metrics_body.contains("vt_cluster_membership_probe_errors_total 1"));
    assert!(metrics_body.contains("vt_cluster_healthy_nodes 1"));
}

#[tokio::test]
async fn cluster_leader_endpoint_elects_local_healthy_control_node() {
    let storage = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let local_addr = reserve_addr();
    let local_base_url = format!("http://{local_addr}");
    let dead_control_node = format!("http://{}", reserve_addr());
    let cluster = ClusterConfig::new(vec![storage.base_url.clone()], 1)
        .expect("cluster config")
        .with_control_nodes(vec![local_base_url.clone(), dead_control_node])
        .with_local_control_node(local_base_url.clone());
    let select = spawn_server_at(local_addr, build_select_router(cluster)).await;
    let client = Client::new();

    let response = client
        .get(select.url("/admin/v1/cluster/leader"))
        .send()
        .await
        .expect("leader request should succeed");
    assert_eq!(response.status(), ReqwestStatusCode::OK);

    let body: Value = response.json().await.expect("leader json");
    assert_eq!(body["leader"], local_base_url);
    assert_eq!(body["local"], true);
}

#[tokio::test]
async fn cluster_control_state_absorbs_fresher_peer_snapshot() {
    let storage = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let local_addr = reserve_addr();
    let local_base_url = format!("http://{local_addr}");
    let remote_peer = spawn_server(
        Router::new()
            .route("/healthz", get(|| async { "ok" }))
            .route(
                "/admin/v1/cluster/state",
                get(|| async {
                    axum::Json(json!({
                        "node": "http://127.0.0.1:19999",
                        "leader": "http://127.0.0.1:19999",
                        "local": true,
                        "epoch": 42,
                        "members": [
                            {
                                "node": "http://storage-a",
                                "observed": true,
                                "healthy": true,
                                "quarantined": false,
                                "consecutive_failures": 0,
                                "last_checked_unix_millis": 1,
                                "last_error": serde_json::Value::Null,
                                "topology_group": serde_json::Value::Null,
                                "weight": 1
                            }
                        ],
                        "peers": [
                            {
                                "node": "http://127.0.0.1:19999",
                                "observed": true,
                                "healthy": true,
                                "quarantined": false,
                                "consecutive_failures": 0,
                                "last_checked_unix_millis": 1,
                                "last_error": serde_json::Value::Null
                            }
                        ]
                    }))
                }),
            ),
    )
    .await;
    let cluster = ClusterConfig::new(vec![storage.base_url.clone()], 1)
        .expect("cluster config")
        .with_control_nodes(vec![local_base_url.clone(), remote_peer.base_url.clone()])
        .with_local_control_node(local_base_url);
    let select = spawn_server_at(local_addr, build_select_router(cluster)).await;
    let client = Client::new();

    let leader_response = client
        .get(select.url("/admin/v1/cluster/leader"))
        .send()
        .await
        .expect("leader request should succeed");
    assert_eq!(leader_response.status(), ReqwestStatusCode::OK);
    let leader_body: Value = leader_response.json().await.expect("leader json");
    assert_eq!(leader_body["leader"], "http://127.0.0.1:19999");
    assert_eq!(leader_body["local"], false);
    assert!(leader_body["epoch"].as_u64().unwrap_or_default() >= 42);

    let state_response = client
        .get(select.url("/admin/v1/cluster/state"))
        .send()
        .await
        .expect("state request should succeed");
    assert_eq!(state_response.status(), ReqwestStatusCode::OK);
    let state_body: Value = state_response.json().await.expect("state json");
    assert_eq!(state_body["leader"], "http://127.0.0.1:19999");
    assert!(state_body["epoch"].as_u64().unwrap_or_default() >= 42);
    let peer_count = state_body["peers"]
        .as_array()
        .map(|peers| peers.len())
        .unwrap_or_default();
    assert!(peer_count >= 2);
}

#[tokio::test]
async fn cluster_leader_replication_exposes_control_journal() {
    let storage = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let first_addr = reserve_addr();
    let second_addr = reserve_addr();
    let (local_addr, remote_addr) = if first_addr.port() < second_addr.port() {
        (first_addr, second_addr)
    } else {
        (second_addr, first_addr)
    };
    let local_base_url = format!("http://{local_addr}");
    let appended_entries = Arc::new(Mutex::new(Vec::<Value>::new()));
    let remote_peer = {
        let appended_entries = appended_entries.clone();
        spawn_server_at(
            remote_addr,
            Router::new()
                .route("/healthz", get(|| async { "ok" }))
                .route(
                    "/admin/v1/cluster/state",
                    get(|| async {
                        axum::Json(json!({
                            "node": "http://127.0.0.1:29999",
                            "leader": serde_json::Value::Null,
                            "local": false,
                            "epoch": 0,
                            "members": [],
                            "peers": [],
                            "journal_head_index": 0
                        }))
                    }),
                )
                .route(
                    "/admin/v1/cluster/journal/append",
                    post(move |axum::Json(body): axum::Json<Value>| {
                        let appended_entries = appended_entries.clone();
                        async move {
                            appended_entries
                                .lock()
                                .expect("lock appended entries")
                                .push(body);
                            StatusCode::OK
                        }
                    }),
                ),
        )
        .await
    };
    let cluster = ClusterConfig::new(vec![storage.base_url.clone()], 1)
        .expect("cluster config")
        .with_control_nodes(vec![local_base_url.clone(), remote_peer.base_url.clone()])
        .with_local_control_node(local_base_url.clone());
    let select = spawn_server_at(local_addr, build_select_router(cluster)).await;
    let client = Client::new();

    let leader_response = client
        .get(select.url("/admin/v1/cluster/leader"))
        .send()
        .await
        .expect("leader request should succeed");
    assert_eq!(leader_response.status(), ReqwestStatusCode::OK);
    let leader_body: Value = leader_response.json().await.expect("leader json");
    assert_eq!(leader_body["leader"], local_base_url);
    assert_eq!(leader_body["local"], true);

    let journal_response = client
        .get(select.url("/admin/v1/cluster/journal"))
        .send()
        .await
        .expect("journal request should succeed");
    assert_eq!(journal_response.status(), ReqwestStatusCode::OK);

    let body: Value = journal_response.json().await.expect("journal json");
    let entries = body["entries"].as_array().cloned().unwrap_or_default();
    assert!(!entries.is_empty());
    assert_eq!(entries[0]["kind"], "leader_elected");

    let replicated = appended_entries
        .lock()
        .expect("lock replicated entries")
        .clone();
    assert!(!replicated.is_empty());
}

#[tokio::test]
async fn cluster_rebalance_requires_local_control_leadership() {
    let storage = spawn_server(build_storage_router(Arc::new(MemoryStorageEngine::new()))).await;
    let first_addr = reserve_addr();
    let second_addr = reserve_addr();
    let (leader_addr, local_addr) = if first_addr.port() < second_addr.port() {
        (first_addr, second_addr)
    } else {
        (second_addr, first_addr)
    };
    let leader = spawn_server_at(
        leader_addr,
        Router::new().route("/healthz", get(|| async { "ok" })),
    )
    .await;
    let local_base_url = format!("http://{local_addr}");
    let cluster = ClusterConfig::new(vec![storage.base_url.clone()], 1)
        .expect("cluster config")
        .with_control_nodes(vec![leader.base_url.clone(), local_base_url.clone()])
        .with_local_control_node(local_base_url);
    let select = spawn_server_at(local_addr, build_select_router(cluster)).await;
    let client = Client::new();

    let leader_response = client
        .get(select.url("/admin/v1/cluster/leader"))
        .send()
        .await
        .expect("leader request should succeed");
    assert_eq!(leader_response.status(), ReqwestStatusCode::OK);
    let leader_body: Value = leader_response.json().await.expect("leader json");
    assert_eq!(leader_body["leader"], leader.base_url);
    assert_eq!(leader_body["local"], false);

    let response = client
        .post(select.url("/admin/v1/cluster/rebalance"))
        .send()
        .await
        .expect("rebalance request should succeed");
    assert_eq!(response.status(), ReqwestStatusCode::CONFLICT);
    let body: Value = response.json().await.expect("rebalance json");
    assert!(body["error"]
        .as_str()
        .unwrap_or_default()
        .contains("leader-gated"));
}
