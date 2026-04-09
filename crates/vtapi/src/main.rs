use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    fs, io,
    net::SocketAddr,
    sync::{Arc, RwLock},
    thread,
    time::Duration,
};

#[cfg(feature = "mimalloc_allocator")]
use mimalloc::MiMalloc;
use reqwest::{Certificate, Client, Identity};
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
use tracing::{info, warn};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use vtapi::{
    build_insert_router_with_client_auth_and_limits,
    build_router_with_storage_startup_auth_limits_and_trace_ingest_profile,
    build_select_router_with_client_auth_and_limits,
    build_storage_router_with_startup_auth_and_limits, serve_app,
    spawn_background_control_refresh_task_with_client_and_auth,
    spawn_background_membership_refresh_task_with_client_and_auth,
    spawn_background_rebalance_task_with_client_and_auth, ApiLimitsConfig, AuthConfig,
    ClusterConfig, ClusterHttpClient, ClusterHttpClientConfig, ServerTlsConfig,
    StorageStartupState, TraceIngestProfile,
};
use vtstorage::{
    BatchingStorageConfig, BatchingStorageEngine, DiskStorageConfig, DiskStorageEngine,
    DiskSyncPolicy, MemoryStorageEngine, StorageEngine,
};

#[cfg(feature = "mimalloc_allocator")]
#[global_allocator]
static GLOBAL_ALLOCATOR: MiMalloc = MiMalloc;

const THROUGHPUT_PROFILE_TARGET_SEGMENT_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const THROUGHPUT_PROFILE_TRACE_SEAL_WORKER_COUNT: usize = 4;
const SYSTEMD_STARTUP_EXTEND_INTERVAL: Duration = Duration::from_secs(10);
const SYSTEMD_STARTUP_EXTEND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TraceIngestDiskOverrides {
    deferred_wal_writes: bool,
    target_segment_size_bytes: Option<u64>,
    trace_seal_worker_count: Option<usize>,
}

struct DeferredStartupApp {
    app: axum::Router,
    runtime: StartupRuntime,
}

struct StartupRuntime {
    startup: StorageStartupState,
    deferred: Arc<DeferredStorageEngine>,
    trace_ingest_profile: TraceIngestProfile,
}

#[derive(Clone, Default)]
struct SystemdNotifier {
    notify_socket: Option<Arc<std::ffi::OsString>>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let role = env::var("VT_ROLE").unwrap_or_else(|_| "single".to_string());
    let limits = load_api_limits();
    let auth = load_auth_config();
    let trace_ingest_profile = load_trace_ingest_profile();
    let server_tls = load_server_tls_config()?;
    let cluster_client = load_cluster_http_client()?;
    let addr = load_bind_addr(&role)?;
    let notifier = SystemdNotifier::from_env();
    let base_url = format!(
        "{}://{addr}",
        if server_tls.is_some() {
            "https"
        } else {
            "http"
        }
    );

    let (app, startup_runtime) = match role.as_str() {
        "insert" => (
            build_insert_router_with_client_auth_and_limits(
                load_cluster_config(None)?,
                cluster_client.clone(),
                auth.clone(),
                limits.clone(),
            ),
            None,
        ),
        "select" => (
            build_select_router_with_client_auth_and_limits(
                load_cluster_config(Some(base_url.as_str()))?,
                cluster_client.clone(),
                auth.clone(),
                limits.clone(),
            ),
            None,
        ),
        "storage" => {
            let startup_app =
                build_storage_startup_app(trace_ingest_profile, auth.clone(), limits.clone());
            (startup_app.app, Some(startup_app.runtime))
        }
        _ => {
            let startup_app =
                build_single_startup_app(trace_ingest_profile, auth.clone(), limits.clone());
            (startup_app.app, Some(startup_app.runtime))
        }
    };

    if role == "select" {
        let admin_bearer_token = auth.admin_or_internal_bearer_token();

        if let Some(interval) = load_optional_interval(
            "VT_CLUSTER_CONTROL_REFRESH_INTERVAL_SECS",
            Some(Duration::from_secs(5)),
        ) {
            let _ = spawn_background_control_refresh_task_with_client_and_auth(
                base_url.clone(),
                interval,
                cluster_client.clone(),
                admin_bearer_token.clone(),
            );
        }

        if let Some(interval) = load_optional_interval(
            "VT_CLUSTER_MEMBERSHIP_REFRESH_INTERVAL_SECS",
            Some(Duration::from_secs(15)),
        ) {
            let _ = spawn_background_membership_refresh_task_with_client_and_auth(
                base_url.clone(),
                interval,
                cluster_client.clone(),
                admin_bearer_token.clone(),
            );
        }

        if let Some(interval) = load_optional_interval("VT_CLUSTER_REBALANCE_INTERVAL_SECS", None) {
            let _ = spawn_background_rebalance_task_with_client_and_auth(
                base_url.clone(),
                interval,
                cluster_client.clone(),
                admin_bearer_token,
            );
        }
    }

    let listener = tokio::net::TcpListener::bind(addr).await?;
    if let Some(runtime) = startup_runtime {
        let startup = runtime.startup.clone();
        notifier.startup_progress(
            format!("{role} listening on {addr}; storage recovery in progress"),
            SYSTEMD_STARTUP_EXTEND_TIMEOUT,
        );
        spawn_startup_timeout_extender(startup.clone(), notifier.clone(), role.clone(), addr);
        spawn_storage_loader(runtime, notifier.clone(), role.clone(), addr);
    } else {
        notifier.ready(format!("{role} ready on {addr}"));
    }
    serve_app(listener, app, server_tls).await?;
    Ok(())
}

fn load_api_limits() -> ApiLimitsConfig {
    let mut limits = ApiLimitsConfig::default();
    if let Some(max_body_bytes) = env::var("VT_MAX_REQUEST_BODY_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        limits = limits.with_max_request_body_bytes(max_body_bytes);
    }
    if let Some(concurrency_limit) = env::var("VT_API_CONCURRENCY_LIMIT")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        limits = limits.with_concurrency_limit(concurrency_limit);
    }
    limits
}

fn load_auth_config() -> AuthConfig {
    let mut auth = AuthConfig::default();
    if let Some(token) = env::var("VT_API_BEARER_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        auth = auth.with_public_bearer_token(token);
    }
    if let Some(token) = env::var("VT_INTERNAL_BEARER_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        auth = auth.with_internal_bearer_token(token);
    }
    if let Some(token) = env::var("VT_ADMIN_BEARER_TOKEN")
        .ok()
        .filter(|value| !value.trim().is_empty())
    {
        auth = auth.with_admin_bearer_token(token);
    }
    auth
}

fn load_trace_ingest_profile() -> TraceIngestProfile {
    parse_trace_ingest_profile(env::var("VT_TRACE_INGEST_PROFILE").ok().as_deref())
}

fn parse_trace_ingest_profile(value: Option<&str>) -> TraceIngestProfile {
    match value
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "throughput" | "direct" | "benchmark" => TraceIngestProfile::Throughput,
        _ => TraceIngestProfile::Default,
    }
}

fn parse_trace_retention(hours: Option<&str>, days: Option<&str>) -> Option<Duration> {
    if let Some(hours) = hours {
        if let Ok(hours) = hours.trim().parse::<u64>() {
            return (hours > 0).then_some(Duration::from_secs(hours.saturating_mul(60 * 60)));
        }
    }
    if let Some(days) = days {
        if let Ok(days) = days.trim().parse::<u64>() {
            return (days > 0).then_some(Duration::from_secs(days.saturating_mul(24 * 60 * 60)));
        }
    }
    None
}

fn load_trace_retention() -> Option<Duration> {
    parse_trace_retention(
        env::var("VT_STORAGE_RETENTION_HOURS").ok().as_deref(),
        env::var("VT_STORAGE_RETENTION_DAYS").ok().as_deref(),
    )
}

fn disk_overrides_for_trace_ingest_profile(
    trace_ingest_profile: TraceIngestProfile,
    sync_policy: DiskSyncPolicy,
    target_segment_size_explicit: bool,
    trace_seal_worker_count_explicit: bool,
) -> TraceIngestDiskOverrides {
    if matches!(trace_ingest_profile, TraceIngestProfile::Throughput)
        && matches!(sync_policy, DiskSyncPolicy::None)
    {
        return TraceIngestDiskOverrides {
            deferred_wal_writes: true,
            target_segment_size_bytes: (!target_segment_size_explicit)
                .then_some(THROUGHPUT_PROFILE_TARGET_SEGMENT_SIZE_BYTES),
            trace_seal_worker_count: (!trace_seal_worker_count_explicit)
                .then_some(THROUGHPUT_PROFILE_TRACE_SEAL_WORKER_COUNT),
        };
    }

    TraceIngestDiskOverrides {
        deferred_wal_writes: false,
        target_segment_size_bytes: None,
        trace_seal_worker_count: None,
    }
}

fn load_storage_sync(
    trace_ingest_profile: TraceIngestProfile,
) -> anyhow::Result<Arc<dyn StorageEngine>> {
    let storage_mode = env::var("VT_STORAGE_MODE").unwrap_or_else(|_| "memory".to_string());
    let configured_trace_shards = env::var("VT_STORAGE_TRACE_SHARDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let storage: Arc<dyn StorageEngine> = match storage_mode.as_str() {
        "disk" => {
            let path =
                env::var("VT_STORAGE_PATH").unwrap_or_else(|_| "./var/victoria-traces".to_string());
            let mut config = DiskStorageConfig::default();
            if let Some(trace_shards) = configured_trace_shards {
                config = config.with_trace_shards(trace_shards);
            }
            if let Some(trace_retention) = load_trace_retention() {
                config = config.with_trace_retention(trace_retention);
            }
            let configured_target_segment_size_bytes =
                env::var("VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok());
            if let Some(target_segment_size_bytes) = configured_target_segment_size_bytes {
                config = config.with_target_segment_size_bytes(target_segment_size_bytes);
            }
            if let Some(trace_group_commit_wait_micros) =
                env::var("VT_STORAGE_TRACE_GROUP_COMMIT_WAIT_MICROS")
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok())
            {
                config = config.with_trace_group_commit_wait(Duration::from_micros(
                    trace_group_commit_wait_micros,
                ));
            }
            if let Some(trace_wal_writer_capacity_bytes) =
                env::var("VT_STORAGE_TRACE_WAL_WRITER_CAPACITY_BYTES")
                    .ok()
                    .and_then(|value| value.parse::<usize>().ok())
            {
                config =
                    config.with_trace_wal_writer_capacity_bytes(trace_wal_writer_capacity_bytes);
            }
            if let Some(trace_wal_enabled) = env_bool("VT_STORAGE_TRACE_WAL_ENABLED") {
                config = config.with_trace_wal_enabled(trace_wal_enabled);
            }
            let configured_trace_seal_worker_count = env::var("VT_STORAGE_TRACE_SEAL_WORKER_COUNT")
                .ok()
                .and_then(|value| value.parse::<usize>().ok());
            if let Some(trace_seal_worker_count) = configured_trace_seal_worker_count {
                config = config.with_trace_seal_worker_count(trace_seal_worker_count);
            }
            let sync_policy = if matches!(
                env::var("VT_STORAGE_SYNC_POLICY")
                    .unwrap_or_else(|_| "none".to_string())
                    .to_ascii_lowercase()
                    .as_str(),
                "data" | "sync-data" | "sync_data"
            ) {
                DiskSyncPolicy::Data
            } else {
                DiskSyncPolicy::None
            };
            config = config.with_sync_policy(sync_policy);
            let disk_overrides = disk_overrides_for_trace_ingest_profile(
                trace_ingest_profile,
                sync_policy,
                configured_target_segment_size_bytes.is_some(),
                configured_trace_seal_worker_count.is_some(),
            );
            if disk_overrides.deferred_wal_writes {
                config = config.with_trace_deferred_wal_writes(true);
            }
            if let Some(target_segment_size_bytes) = disk_overrides.target_segment_size_bytes {
                config = config.with_target_segment_size_bytes(target_segment_size_bytes);
            }
            if let Some(trace_seal_worker_count) = disk_overrides.trace_seal_worker_count {
                config = config.with_trace_seal_worker_count(trace_seal_worker_count);
            }
            Arc::new(DiskStorageEngine::open_with_config(path, config)?)
        }
        _ => configured_trace_shards
            .map(MemoryStorageEngine::with_trace_shards)
            .map(Arc::new)
            .unwrap_or_else(|| Arc::new(MemoryStorageEngine::new())),
    };
    if env_truthy("VT_STORAGE_BATCH_DISABLED") {
        return Ok(storage);
    }
    let mut batching = BatchingStorageConfig::default();
    if let Some(max_batch_rows) = env::var("VT_STORAGE_BATCH_MAX_ROWS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        batching = batching.with_max_batch_rows(max_batch_rows);
    }
    if let Some(trace_shards) = env::var("VT_STORAGE_BATCH_TRACE_SHARDS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        batching = batching.with_trace_shards(trace_shards);
    }
    if let Some(max_trace_batch_blocks) = env::var("VT_STORAGE_TRACE_BATCH_MAX_BLOCKS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
    {
        batching = batching.with_max_trace_batch_blocks(max_trace_batch_blocks);
    }
    if let Some(max_batch_wait_micros) = env::var("VT_STORAGE_BATCH_MAX_WAIT_MICROS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
    {
        batching = batching.with_max_batch_wait(Duration::from_micros(max_batch_wait_micros));
    }

    Ok(Arc::new(BatchingStorageEngine::with_config(
        storage, batching,
    )))
}

fn build_storage_startup_app(
    trace_ingest_profile: TraceIngestProfile,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> DeferredStartupApp {
    let startup = StorageStartupState::starting();
    let deferred = Arc::new(DeferredStorageEngine::default());
    let deferred_storage: Arc<dyn StorageEngine> = deferred.clone();
    DeferredStartupApp {
        app: build_storage_router_with_startup_auth_and_limits(
            deferred_storage,
            startup.clone(),
            auth,
            limits,
        ),
        runtime: StartupRuntime {
            startup,
            deferred,
            trace_ingest_profile,
        },
    }
}

fn build_single_startup_app(
    trace_ingest_profile: TraceIngestProfile,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> DeferredStartupApp {
    let startup = StorageStartupState::starting();
    let deferred = Arc::new(DeferredStorageEngine::default());
    let deferred_storage: Arc<dyn StorageEngine> = deferred.clone();
    DeferredStartupApp {
        app: build_router_with_storage_startup_auth_limits_and_trace_ingest_profile(
            deferred_storage,
            startup.clone(),
            auth,
            limits,
            trace_ingest_profile,
        ),
        runtime: StartupRuntime {
            startup,
            deferred,
            trace_ingest_profile,
        },
    }
}

fn spawn_storage_loader(
    runtime: StartupRuntime,
    notifier: SystemdNotifier,
    role: String,
    addr: SocketAddr,
) {
    thread::spawn(
        move || match load_storage_sync(runtime.trace_ingest_profile) {
            Ok(storage) => {
                runtime.deferred.install(storage);
                runtime.startup.mark_ready();
                info!(role = %role, addr = %addr, "storage recovery completed");
                notifier.ready(format!("{role} ready on {addr}"));
            }
            Err(error) => {
                let error_message = error.to_string();
                runtime.startup.mark_failed(error_message.clone());
                warn!(
                    role = %role,
                    addr = %addr,
                    error = %error_message,
                    "storage startup failed"
                );
                notifier.status(format!("{role} startup failed on {addr}: {error_message}"));
                thread::sleep(Duration::from_millis(200));
                std::process::exit(1);
            }
        },
    );
}

fn spawn_startup_timeout_extender(
    startup: StorageStartupState,
    notifier: SystemdNotifier,
    role: String,
    addr: SocketAddr,
) {
    if !notifier.is_enabled() {
        return;
    }

    thread::spawn(move || {
        while !startup.is_ready() && startup.failure().is_none() {
            thread::sleep(SYSTEMD_STARTUP_EXTEND_INTERVAL);
            if startup.is_ready() || startup.failure().is_some() {
                break;
            }
            notifier.startup_progress(
                format!("{role} listening on {addr}; storage recovery in progress"),
                SYSTEMD_STARTUP_EXTEND_TIMEOUT,
            );
        }
    });
}

impl SystemdNotifier {
    fn from_env() -> Self {
        Self {
            notify_socket: env::var_os("NOTIFY_SOCKET").map(Arc::new),
        }
    }

    fn is_enabled(&self) -> bool {
        self.notify_socket.is_some()
    }

    fn status(&self, status: impl AsRef<str>) {
        self.notify(&format!(
            "STATUS={}",
            sanitize_systemd_value(status.as_ref())
        ));
    }

    fn startup_progress(&self, status: impl AsRef<str>, timeout: Duration) {
        self.notify(&format!(
            "STATUS={}\nEXTEND_TIMEOUT_USEC={}",
            sanitize_systemd_value(status.as_ref()),
            timeout.as_micros()
        ));
    }

    fn ready(&self, status: impl AsRef<str>) {
        self.notify(&format!(
            "READY=1\nSTATUS={}",
            sanitize_systemd_value(status.as_ref())
        ));
    }

    fn notify(&self, state: &str) {
        let Some(socket) = self.notify_socket.as_deref() else {
            return;
        };

        if let Err(error) = send_systemd_notification(socket.as_ref(), state) {
            warn!(error = %error, state = %state, "systemd notify failed");
        }
    }
}

fn sanitize_systemd_value(value: &str) -> String {
    value.replace('\n', " ")
}

#[cfg(unix)]
fn send_systemd_notification(socket: &OsStr, state: &str) -> io::Result<()> {
    let socket_bytes = socket.as_bytes();
    if socket_bytes.is_empty() {
        return Ok(());
    }

    unsafe {
        let fd = libc::socket(libc::AF_UNIX, libc::SOCK_DGRAM, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }

        let mut addr: libc::sockaddr_un = std::mem::zeroed();
        addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
        let base_len = (&addr.sun_path as *const _ as usize) - (&addr as *const _ as usize);
        let addr_len = if socket_bytes[0] == b'@' {
            #[cfg(target_os = "linux")]
            {
                let name = &socket_bytes[1..];
                if name.len() + 1 > addr.sun_path.len() {
                    libc::close(fd);
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "NOTIFY_SOCKET abstract path too long",
                    ));
                }
                addr.sun_path[0] = 0;
                for (index, byte) in name.iter().enumerate() {
                    addr.sun_path[index + 1] = *byte as libc::c_char;
                }
                base_len + 1 + name.len()
            }
            #[cfg(not(target_os = "linux"))]
            {
                libc::close(fd);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "abstract NOTIFY_SOCKET is only supported on Linux",
                ));
            }
        } else {
            if socket_bytes.len() >= addr.sun_path.len() {
                libc::close(fd);
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "NOTIFY_SOCKET path too long",
                ));
            }
            for (index, byte) in socket_bytes.iter().enumerate() {
                addr.sun_path[index] = *byte as libc::c_char;
            }
            addr.sun_path[socket_bytes.len()] = 0;
            base_len + socket_bytes.len() + 1
        };

        let result = libc::sendto(
            fd,
            state.as_ptr().cast(),
            state.len(),
            0,
            (&addr as *const libc::sockaddr_un).cast(),
            addr_len as libc::socklen_t,
        );
        let send_result = if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        };
        libc::close(fd);
        send_result
    }
}

#[cfg(not(unix))]
fn send_systemd_notification(_socket: &OsStr, _state: &str) -> io::Result<()> {
    Ok(())
}

#[derive(Default)]
struct DeferredStorageEngine {
    inner: RwLock<Option<Arc<dyn StorageEngine>>>,
}

impl DeferredStorageEngine {
    fn install(&self, storage: Arc<dyn StorageEngine>) {
        *self
            .inner
            .write()
            .expect("deferred storage rwlock poisoned") = Some(storage);
    }

    fn ready_storage(&self) -> Result<Arc<dyn StorageEngine>, vtstorage::StorageError> {
        self.inner
            .read()
            .expect("deferred storage rwlock poisoned")
            .clone()
            .ok_or_else(|| {
                vtstorage::StorageError::Message("storage startup in progress".to_string())
            })
    }
}

impl StorageEngine for DeferredStorageEngine {
    fn append_rows(&self, rows: Vec<vtcore::TraceSpanRow>) -> Result<(), vtstorage::StorageError> {
        self.ready_storage()?.append_rows(rows)
    }

    fn append_logs(&self, rows: Vec<vtcore::LogRow>) -> Result<(), vtstorage::StorageError> {
        self.ready_storage()?.append_logs(rows)
    }

    fn trace_window(&self, trace_id: &str) -> Option<vtcore::TraceWindow> {
        self.ready_storage()
            .ok()
            .and_then(|storage| storage.trace_window(trace_id))
    }

    fn list_trace_ids(&self) -> Vec<String> {
        self.ready_storage()
            .map(|storage| storage.list_trace_ids())
            .unwrap_or_default()
    }

    fn list_services(&self) -> Vec<String> {
        self.ready_storage()
            .map(|storage| storage.list_services())
            .unwrap_or_default()
    }

    fn list_field_names(&self) -> Vec<String> {
        self.ready_storage()
            .map(|storage| storage.list_field_names())
            .unwrap_or_default()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.ready_storage()
            .map(|storage| storage.list_field_values(field_name))
            .unwrap_or_default()
    }

    fn search_traces(&self, request: &vtcore::TraceSearchRequest) -> Vec<vtcore::TraceSearchHit> {
        self.ready_storage()
            .map(|storage| storage.search_traces(request))
            .unwrap_or_default()
    }

    fn search_logs(&self, request: &vtcore::LogSearchRequest) -> Vec<vtcore::LogRow> {
        self.ready_storage()
            .map(|storage| storage.search_logs(request))
            .unwrap_or_default()
    }

    fn rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<vtcore::TraceSpanRow> {
        self.ready_storage()
            .map(|storage| storage.rows_for_trace(trace_id, start_unix_nano, end_unix_nano))
            .unwrap_or_default()
    }

    fn stats(&self) -> vtstorage::StorageStatsSnapshot {
        self.ready_storage()
            .map(|storage| storage.stats())
            .unwrap_or_default()
    }

    fn preferred_trace_ingest_shards(&self) -> usize {
        self.ready_storage()
            .map(|storage| storage.preferred_trace_ingest_shards())
            .unwrap_or(1)
    }

    fn trace_batch_payload_mode(&self) -> vtstorage::TraceBatchPayloadMode {
        self.ready_storage()
            .map(|storage| storage.trace_batch_payload_mode())
            .unwrap_or_default()
    }
}

fn parse_env_bool_value(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "on" => Some(true),
        "0" | "false" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn env_bool(name: &str) -> Option<bool> {
    env::var(name)
        .ok()
        .and_then(|value| parse_env_bool_value(&value))
}

fn env_truthy(name: &str) -> bool {
    env_bool(name).unwrap_or(false)
}

fn load_cluster_config(local_control_node: Option<&str>) -> anyhow::Result<ClusterConfig> {
    let raw_nodes = env::var("VT_CLUSTER_STORAGE_NODES")
        .map_err(|_| anyhow::anyhow!("VT_CLUSTER_STORAGE_NODES must be set for cluster roles"))?;
    let nodes = raw_nodes
        .split(',')
        .map(|value| value.trim().to_string())
        .collect();
    let control_nodes: Vec<String> = env::var("VT_CLUSTER_CONTROL_NODES")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(|node| node.trim().to_string())
                .filter(|node| !node.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let replication_factor = env::var("VT_CLUSTER_REPLICATION_FACTOR")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(1);
    let write_quorum = env::var("VT_CLUSTER_WRITE_QUORUM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let read_quorum = env::var("VT_CLUSTER_READ_QUORUM")
        .ok()
        .and_then(|value| value.parse::<usize>().ok());
    let failure_backoff_ms = env::var("VT_CLUSTER_FAILURE_BACKOFF_MS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok());
    let topology_groups = parse_topology_groups(
        env::var("VT_CLUSTER_STORAGE_TOPOLOGY")
            .ok()
            .as_deref()
            .unwrap_or(""),
    );
    let node_weights = parse_node_weights(
        env::var("VT_CLUSTER_STORAGE_WEIGHTS")
            .ok()
            .as_deref()
            .unwrap_or(""),
    );

    let mut config = ClusterConfig::new(nodes, replication_factor)?;
    if let Some(write_quorum) = write_quorum {
        config = config.with_write_quorum(write_quorum)?;
    }
    if let Some(read_quorum) = read_quorum {
        config = config.with_read_quorum(read_quorum)?;
    }
    if let Some(failure_backoff_ms) = failure_backoff_ms {
        config = config.with_failure_backoff(Duration::from_millis(failure_backoff_ms));
    }
    if !topology_groups.is_empty() {
        config = config.with_topology_groups(topology_groups);
    }
    if !node_weights.is_empty() {
        config = config.with_node_weights(node_weights);
    }
    if !control_nodes.is_empty() {
        config = config.with_control_nodes(control_nodes);
    }
    if let Some(local_control_node) = local_control_node {
        config = config.with_local_control_node(local_control_node.to_string());
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::{
        disk_overrides_for_trace_ingest_profile, parse_env_bool_value, parse_trace_ingest_profile,
        parse_trace_retention, TraceIngestDiskOverrides, TraceIngestProfile,
        THROUGHPUT_PROFILE_TARGET_SEGMENT_SIZE_BYTES, THROUGHPUT_PROFILE_TRACE_SEAL_WORKER_COUNT,
    };
    use std::time::Duration;
    use vtstorage::DiskSyncPolicy;

    #[test]
    fn parse_trace_ingest_profile_defaults_to_default() {
        assert_eq!(
            parse_trace_ingest_profile(None),
            TraceIngestProfile::Default
        );
        assert_eq!(
            parse_trace_ingest_profile(Some("")),
            TraceIngestProfile::Default
        );
        assert_eq!(
            parse_trace_ingest_profile(Some("unknown")),
            TraceIngestProfile::Default
        );
    }

    #[test]
    fn parse_trace_ingest_profile_accepts_throughput_aliases() {
        assert_eq!(
            parse_trace_ingest_profile(Some("throughput")),
            TraceIngestProfile::Throughput
        );
        assert_eq!(
            parse_trace_ingest_profile(Some("direct")),
            TraceIngestProfile::Throughput
        );
        assert_eq!(
            parse_trace_ingest_profile(Some("benchmark")),
            TraceIngestProfile::Throughput
        );
    }

    #[test]
    fn throughput_profile_enables_deferred_wal_and_bounded_default_segment_size() {
        assert_eq!(
            disk_overrides_for_trace_ingest_profile(
                TraceIngestProfile::Throughput,
                DiskSyncPolicy::None,
                false,
                false,
            ),
            TraceIngestDiskOverrides {
                deferred_wal_writes: true,
                target_segment_size_bytes: Some(THROUGHPUT_PROFILE_TARGET_SEGMENT_SIZE_BYTES),
                trace_seal_worker_count: Some(THROUGHPUT_PROFILE_TRACE_SEAL_WORKER_COUNT),
            }
        );
    }

    #[test]
    fn throughput_profile_respects_explicit_segment_size_and_sync_policy() {
        assert_eq!(
            disk_overrides_for_trace_ingest_profile(
                TraceIngestProfile::Throughput,
                DiskSyncPolicy::None,
                true,
                false,
            ),
            TraceIngestDiskOverrides {
                deferred_wal_writes: true,
                target_segment_size_bytes: None,
                trace_seal_worker_count: Some(THROUGHPUT_PROFILE_TRACE_SEAL_WORKER_COUNT),
            }
        );
        assert_eq!(
            disk_overrides_for_trace_ingest_profile(
                TraceIngestProfile::Throughput,
                DiskSyncPolicy::Data,
                false,
                false,
            ),
            TraceIngestDiskOverrides {
                deferred_wal_writes: false,
                target_segment_size_bytes: None,
                trace_seal_worker_count: None,
            }
        );
    }

    #[test]
    fn parse_trace_retention_prefers_hours_over_days() {
        assert_eq!(
            parse_trace_retention(Some("24"), Some("7"),),
            Some(Duration::from_secs(24 * 60 * 60)),
        );
        assert_eq!(
            parse_trace_retention(None, Some("1"),),
            Some(Duration::from_secs(24 * 60 * 60)),
        );
        assert_eq!(parse_trace_retention(Some("0"), Some("1")), None);
        assert_eq!(
            parse_trace_retention(Some("invalid"), Some("1")),
            Some(Duration::from_secs(24 * 60 * 60))
        );
    }

    #[test]
    fn parse_env_bool_value_supports_truthy_and_falsey_values() {
        for value in ["1", "true", "TRUE", " yes ", "On"] {
            assert_eq!(parse_env_bool_value(value), Some(true), "{value}");
        }
        for value in ["0", "false", "FALSE", " no ", "Off"] {
            assert_eq!(parse_env_bool_value(value), Some(false), "{value}");
        }
        for value in ["", "maybe", "2"] {
            assert_eq!(parse_env_bool_value(value), None, "{value}");
        }
    }
}

fn load_bind_addr(role: &str) -> anyhow::Result<SocketAddr> {
    let default_addr = match role {
        "insert" => "127.0.0.1:13001",
        "select" => "127.0.0.1:13002",
        "storage" => "127.0.0.1:13003",
        _ => "127.0.0.1:13000",
    };
    let raw_addr = env::var("VT_BIND_ADDR").unwrap_or_else(|_| default_addr.to_string());
    Ok(raw_addr.parse()?)
}

fn load_server_tls_config() -> anyhow::Result<Option<ServerTlsConfig>> {
    let cert_path = env::var("VT_TLS_CERT_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let key_path = env::var("VT_TLS_KEY_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let client_ca_path = env::var("VT_TLS_CLIENT_CA_CERT_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let reload_interval = load_optional_interval("VT_TLS_RELOAD_INTERVAL_SECS", None);

    match (cert_path, key_path) {
        (None, None) => Ok(None),
        (Some(cert_path), Some(key_path)) => {
            let mut tls_config = ServerTlsConfig::from_pem_files(cert_path, key_path);
            if let Some(client_ca_path) = client_ca_path {
                tls_config = tls_config.with_client_ca_path(client_ca_path);
            }
            if let Some(reload_interval) = reload_interval {
                tls_config = tls_config.with_reload_interval(reload_interval);
            }
            Ok(Some(tls_config))
        }
        _ => Err(anyhow::anyhow!(
            "VT_TLS_CERT_PATH and VT_TLS_KEY_PATH must be configured together"
        )),
    }
}

fn load_cluster_http_client() -> anyhow::Result<ClusterHttpClient> {
    let ca_cert_path = env::var("VT_CLUSTER_TLS_CA_CERT_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let client_cert_path = env::var("VT_CLUSTER_TLS_CLIENT_CERT_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let client_key_path = env::var("VT_CLUSTER_TLS_CLIENT_KEY_PATH")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let insecure_skip_verify = matches!(
        env::var("VT_CLUSTER_TLS_INSECURE_SKIP_VERIFY")
            .ok()
            .as_deref(),
        Some("1" | "true" | "TRUE" | "True" | "yes" | "YES" | "Yes")
    );
    let reload_interval = load_optional_interval("VT_CLUSTER_TLS_RELOAD_INTERVAL_SECS", None);

    if let Some(reload_interval) = reload_interval {
        let mut config = ClusterHttpClientConfig::new(reload_interval)
            .with_insecure_skip_verify(insecure_skip_verify);
        if let Some(ca_cert_path) = ca_cert_path.as_deref() {
            config = config.with_ca_cert_path(ca_cert_path);
        }
        match (client_cert_path.as_deref(), client_key_path.as_deref()) {
            (Some(client_cert_path), Some(client_key_path)) => {
                config = config.with_client_identity_paths(client_cert_path, client_key_path);
            }
            (None, None) => {}
            _ => {
                return Err(anyhow::anyhow!(
                    "VT_CLUSTER_TLS_CLIENT_CERT_PATH and VT_CLUSTER_TLS_CLIENT_KEY_PATH must be configured together"
                ));
            }
        }
        return ClusterHttpClient::from_reloading_config(config);
    }

    let mut builder = Client::builder().use_rustls_tls();
    if let Some(ca_cert_path) = ca_cert_path {
        let certificate = Certificate::from_pem(&fs::read(ca_cert_path)?)?;
        builder = builder.add_root_certificate(certificate);
    }
    match (client_cert_path, client_key_path) {
        (Some(client_cert_path), Some(client_key_path)) => {
            let mut identity_pem = fs::read(client_cert_path)?;
            identity_pem.extend(fs::read(client_key_path)?);
            builder = builder.identity(Identity::from_pem(&identity_pem)?);
        }
        (None, None) => {}
        _ => {
            return Err(anyhow::anyhow!(
                "VT_CLUSTER_TLS_CLIENT_CERT_PATH and VT_CLUSTER_TLS_CLIENT_KEY_PATH must be configured together"
            ));
        }
    }

    if insecure_skip_verify {
        builder = builder.danger_accept_invalid_certs(true);
    }

    Ok(ClusterHttpClient::from_client(builder.build()?))
}

fn load_optional_interval(env_key: &str, default: Option<Duration>) -> Option<Duration> {
    match env::var(env_key) {
        Ok(value) => value
            .parse::<u64>()
            .ok()
            .and_then(|seconds| (seconds > 0).then(|| Duration::from_secs(seconds))),
        Err(_) => default,
    }
}

fn parse_topology_groups(raw_value: &str) -> HashMap<String, String> {
    raw_value
        .split(',')
        .filter_map(|entry| {
            let (node, group) = entry.split_once('=')?;
            let node = node.trim().trim_end_matches('/').to_string();
            let group = group.trim().to_string();
            if node.is_empty() || group.is_empty() {
                None
            } else {
                Some((node, group))
            }
        })
        .collect()
}

fn parse_node_weights(raw_value: &str) -> HashMap<String, u32> {
    raw_value
        .split(',')
        .filter_map(|entry| {
            let (node, weight) = entry.split_once('=')?;
            let node = node.trim().trim_end_matches('/').to_string();
            let weight = weight.trim().parse::<u32>().ok()?;
            if node.is_empty() || weight == 0 {
                None
            } else {
                Some((node, weight))
            }
        })
        .collect()
}
