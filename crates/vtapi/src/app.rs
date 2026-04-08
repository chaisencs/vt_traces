use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, Query, Request, State},
    http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{
    sync::Semaphore,
    task::{self, JoinSet},
};
use tracing::{debug, info, warn};
use vtcore::{
    decode_log_rows, decode_trace_rows, encode_log_row, encode_log_rows_from_encoded_rows,
    encode_trace_row, encode_trace_rows, encode_trace_rows_from_encoded_rows, FieldFilter, LogRow,
    LogSearchRequest, TraceBlock, TraceSearchHit, TraceSearchRequest, TraceSpanRow,
};
use vtingest::{
    decode_export_logs_service_request_protobuf, decode_trace_block_protobuf,
    decode_trace_blocks_protobuf_sharded, decode_trace_rows_protobuf, AttributeValue, KeyValue,
    ResourceSpans, ScopeSpans, SpanRecord,
    Status as OtlpStatus, encode_export_trace_service_request_protobuf, export_request_from_rows,
    flatten_export_logs_request, flatten_export_request, ExportLogsServiceRequest,
    ExportTraceServiceRequest,
};
use vtquery::QueryService;
use vtstorage::{MemoryStorageEngine, StorageEngine, StorageError, StorageStatsSnapshot};

use crate::{cluster::ClusterConfig, http_client::ClusterHttpClient};

const MAX_JAEGER_LIMIT: usize = 1_000;
const JAEGER_POST_FILTER_FETCH_MULTIPLIER: usize = 8;
const MAX_JAEGER_TRACE_FETCH_LIMIT: usize = MAX_JAEGER_LIMIT * 4;
const MAX_JAEGER_DEPENDENCIES_TRACE_SCAN_LIMIT: usize = 5_000;
const DEFAULT_JAEGER_DEPENDENCIES_LOOKBACK_MILLIS: i64 = 60 * 60 * 1000;
const DEFAULT_MAX_REQUEST_BODY_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_CONCURRENCY_LIMIT: usize = 1_024;
const INTERNAL_TRACE_ROWS_CONTENT_TYPE: &str = "application/x-vt-row-batch";
const INTERNAL_LOG_ROWS_CONTENT_TYPE: &str = "application/x-vt-log-batch";

#[derive(Debug, Clone)]
pub struct ApiLimitsConfig {
    max_request_body_bytes: usize,
    concurrency_limit: usize,
}

#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    public_bearer_token: Option<String>,
    internal_bearer_token: Option<String>,
    admin_bearer_token: Option<String>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TraceIngestProfile {
    #[default]
    Default,
    Throughput,
}

impl Default for ApiLimitsConfig {
    fn default() -> Self {
        Self {
            max_request_body_bytes: DEFAULT_MAX_REQUEST_BODY_BYTES,
            concurrency_limit: DEFAULT_CONCURRENCY_LIMIT,
        }
    }
}

impl ApiLimitsConfig {
    pub fn with_max_request_body_bytes(mut self, max_request_body_bytes: usize) -> Self {
        self.max_request_body_bytes = max_request_body_bytes.max(1);
        self
    }

    pub fn with_concurrency_limit(mut self, concurrency_limit: usize) -> Self {
        self.concurrency_limit = concurrency_limit.max(1);
        self
    }
}

impl AuthConfig {
    pub fn with_public_bearer_token(mut self, public_bearer_token: impl Into<String>) -> Self {
        self.public_bearer_token = sanitize_bearer_token(public_bearer_token);
        self
    }

    pub fn with_internal_bearer_token(mut self, internal_bearer_token: impl Into<String>) -> Self {
        self.internal_bearer_token = sanitize_bearer_token(internal_bearer_token);
        self
    }

    pub fn with_admin_bearer_token(mut self, admin_bearer_token: impl Into<String>) -> Self {
        self.admin_bearer_token = sanitize_bearer_token(admin_bearer_token);
        self
    }

    pub fn admin_or_internal_bearer_token(&self) -> Option<String> {
        self.admin_bearer_token()
            .map(ToString::to_string)
            .or_else(|| self.internal_bearer_token().map(ToString::to_string))
    }

    fn public_bearer_token(&self) -> Option<&str> {
        self.public_bearer_token.as_deref()
    }

    fn internal_bearer_token(&self) -> Option<&str> {
        self.internal_bearer_token.as_deref()
    }

    fn admin_bearer_token(&self) -> Option<&str> {
        self.admin_bearer_token
            .as_deref()
            .or(self.internal_bearer_token())
    }
}

struct StorageState {
    storage: Arc<dyn StorageEngine>,
    query: QueryService,
    trace_ingest_profile: TraceIngestProfile,
}

struct InsertState {
    cluster: ClusterConfig,
    http_client: ClusterHttpClient,
    metrics: ClusterMetrics,
    health: NodeHealthTracker,
    auth: AuthConfig,
}

struct SelectState {
    cluster: ClusterConfig,
    http_client: ClusterHttpClient,
    metrics: ClusterMetrics,
    health: NodeHealthTracker,
    control: ControlPlaneState,
    auth: AuthConfig,
}

#[derive(Debug)]
struct ControlPlaneState {
    health: NodeHealthTracker,
    view: Mutex<ControlPlaneView>,
    journal: Mutex<ControlJournalState>,
}

#[derive(Debug, Clone, Default)]
struct ControlPlaneView {
    leader: Option<String>,
    epoch: u64,
}

#[derive(Debug, Default)]
struct ControlJournalState {
    next_index: u64,
    entries: Vec<ControlJournalEntry>,
}

#[derive(Debug, Default)]
struct ClusterMetrics {
    remote_write_batches: AtomicU64,
    remote_write_rows: AtomicU64,
    remote_write_errors: AtomicU64,
    remote_write_skips: AtomicU64,
    remote_read_requests: AtomicU64,
    remote_read_errors: AtomicU64,
    remote_read_skips: AtomicU64,
    node_quarantines: AtomicU64,
    read_repairs: AtomicU64,
    read_repair_errors: AtomicU64,
    membership_probe_requests: AtomicU64,
    membership_probe_errors: AtomicU64,
    control_probe_requests: AtomicU64,
    control_probe_errors: AtomicU64,
    control_state_sync_requests: AtomicU64,
    control_state_sync_errors: AtomicU64,
    control_leader_changes: AtomicU64,
}

#[derive(Debug, Clone, Default)]
struct ClusterMetricsSnapshot {
    remote_write_batches: u64,
    remote_write_rows: u64,
    remote_write_errors: u64,
    remote_write_skips: u64,
    remote_read_requests: u64,
    remote_read_errors: u64,
    remote_read_skips: u64,
    node_quarantines: u64,
    read_repairs: u64,
    read_repair_errors: u64,
    membership_probe_requests: u64,
    membership_probe_errors: u64,
    control_probe_requests: u64,
    control_probe_errors: u64,
    control_state_sync_requests: u64,
    control_state_sync_errors: u64,
    control_leader_changes: u64,
    control_epoch: u64,
}

#[derive(Debug)]
struct NodeHealthTracker {
    failure_backoff: Duration,
    records: Mutex<HashMap<String, NodeHealthRecord>>,
}

#[derive(Debug)]
struct NodeSelection {
    nodes: Vec<String>,
    skipped: usize,
}

#[derive(Debug, Clone)]
struct NodeHealthRecord {
    consecutive_failures: u64,
    last_checked_at: Option<SystemTime>,
    last_error: Option<String>,
    quarantined_until: Option<Instant>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterMembersResponse {
    members: Vec<ClusterMemberResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterMemberResponse {
    node: String,
    observed: bool,
    healthy: bool,
    quarantined: bool,
    consecutive_failures: u64,
    last_checked_unix_millis: Option<u64>,
    last_error: Option<String>,
    topology_group: Option<String>,
    weight: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterLeaderResponse {
    leader: Option<String>,
    local: bool,
    epoch: u64,
    peers: Vec<ControlPeerResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ClusterControlStateResponse {
    node: Option<String>,
    leader: Option<String>,
    local: bool,
    epoch: u64,
    #[serde(default)]
    journal_head_index: u64,
    members: Vec<ClusterMemberResponse>,
    peers: Vec<ControlPeerResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ControlJournalEntry {
    index: u64,
    epoch: u64,
    kind: String,
    leader: Option<String>,
    node: Option<String>,
    created_unix_millis: u64,
    #[serde(default)]
    details: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlJournalResponse {
    head_index: u64,
    entries: Vec<ControlJournalEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlJournalAppendRequest {
    entries: Vec<ControlJournalEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ControlPeerResponse {
    node: String,
    observed: bool,
    healthy: bool,
    quarantined: bool,
    consecutive_failures: u64,
    last_checked_unix_millis: Option<u64>,
    last_error: Option<String>,
}

impl ClusterMetrics {
    fn record_write_batch(&self, rows: usize) {
        self.remote_write_batches.fetch_add(1, Ordering::Relaxed);
        self.remote_write_rows
            .fetch_add(rows as u64, Ordering::Relaxed);
    }

    fn record_write_error(&self) {
        self.remote_write_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_write_skips(&self, skipped: usize) {
        self.remote_write_skips
            .fetch_add(skipped as u64, Ordering::Relaxed);
    }

    fn record_read_request(&self) {
        self.remote_read_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_error(&self) {
        self.remote_read_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_skips(&self, skipped: usize) {
        self.remote_read_skips
            .fetch_add(skipped as u64, Ordering::Relaxed);
    }

    fn record_node_quarantine(&self) {
        self.node_quarantines.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> ClusterMetricsSnapshot {
        ClusterMetricsSnapshot {
            remote_write_batches: self.remote_write_batches.load(Ordering::Relaxed),
            remote_write_rows: self.remote_write_rows.load(Ordering::Relaxed),
            remote_write_errors: self.remote_write_errors.load(Ordering::Relaxed),
            remote_write_skips: self.remote_write_skips.load(Ordering::Relaxed),
            remote_read_requests: self.remote_read_requests.load(Ordering::Relaxed),
            remote_read_errors: self.remote_read_errors.load(Ordering::Relaxed),
            remote_read_skips: self.remote_read_skips.load(Ordering::Relaxed),
            node_quarantines: self.node_quarantines.load(Ordering::Relaxed),
            read_repairs: self.read_repairs.load(Ordering::Relaxed),
            read_repair_errors: self.read_repair_errors.load(Ordering::Relaxed),
            membership_probe_requests: self.membership_probe_requests.load(Ordering::Relaxed),
            membership_probe_errors: self.membership_probe_errors.load(Ordering::Relaxed),
            control_probe_requests: self.control_probe_requests.load(Ordering::Relaxed),
            control_probe_errors: self.control_probe_errors.load(Ordering::Relaxed),
            control_state_sync_requests: self.control_state_sync_requests.load(Ordering::Relaxed),
            control_state_sync_errors: self.control_state_sync_errors.load(Ordering::Relaxed),
            control_leader_changes: self.control_leader_changes.load(Ordering::Relaxed),
            control_epoch: 0,
        }
    }

    fn record_read_repair(&self) {
        self.read_repairs.fetch_add(1, Ordering::Relaxed);
    }

    fn record_read_repair_error(&self) {
        self.read_repair_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_membership_probe_request(&self) {
        self.membership_probe_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_membership_probe_error(&self) {
        self.membership_probe_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_control_probe_request(&self) {
        self.control_probe_requests.fetch_add(1, Ordering::Relaxed);
    }

    fn record_control_probe_error(&self) {
        self.control_probe_errors.fetch_add(1, Ordering::Relaxed);
    }

    fn record_control_state_sync_request(&self) {
        self.control_state_sync_requests
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_control_state_sync_error(&self) {
        self.control_state_sync_errors
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_control_leader_change(&self) {
        self.control_leader_changes.fetch_add(1, Ordering::Relaxed);
    }
}

impl NodeHealthTracker {
    fn new(failure_backoff: Duration) -> Self {
        Self {
            failure_backoff,
            records: Mutex::new(HashMap::new()),
        }
    }

    fn select_nodes<'a, I>(&self, nodes: I, min_required: usize) -> NodeSelection
    where
        I: IntoIterator<Item = &'a str>,
    {
        let now = Instant::now();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_records(&mut records, now);

        let mut healthy = Vec::new();
        let mut quarantined = Vec::new();
        for node in nodes {
            if records
                .get(node)
                .and_then(|record| record.quarantined_until)
                .map(|until| until > now)
                .unwrap_or(false)
            {
                quarantined.push(node.to_string());
            } else {
                healthy.push(node.to_string());
            }
        }

        if healthy.len() >= min_required {
            NodeSelection {
                nodes: healthy,
                skipped: quarantined.len(),
            }
        } else {
            healthy.extend(quarantined);
            NodeSelection {
                nodes: healthy,
                skipped: 0,
            }
        }
    }

    fn mark_failure(&self, node: &str, error: impl Into<String>) -> bool {
        let now = Instant::now();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_records(&mut records, now);

        let record = records.entry(node.to_string()).or_default();
        let was_healthy = record
            .quarantined_until
            .map(|until| until <= now)
            .unwrap_or(true);
        record.consecutive_failures = record.consecutive_failures.saturating_add(1);
        record.last_checked_at = Some(SystemTime::now());
        record.last_error = Some(error.into());
        record.quarantined_until = Some(now + self.failure_backoff);
        was_healthy
    }

    fn mark_success(&self, node: &str) {
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let record = records.entry(node.to_string()).or_default();
        record.consecutive_failures = 0;
        record.last_checked_at = Some(SystemTime::now());
        record.last_error = None;
        record.quarantined_until = None;
    }

    fn unhealthy_nodes(&self) -> usize {
        let now = Instant::now();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_records(&mut records, now);
        records
            .values()
            .filter(|record| {
                record
                    .quarantined_until
                    .map(|until| until > now)
                    .unwrap_or(false)
            })
            .count()
    }

    fn snapshot(&self, cluster: &ClusterConfig) -> Vec<ClusterMemberResponse> {
        let now = Instant::now();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_records(&mut records, now);

        cluster
            .storage_nodes()
            .iter()
            .map(|node| {
                let record = records.get(node);
                let quarantined = record
                    .and_then(|record| record.quarantined_until)
                    .map(|until| until > now)
                    .unwrap_or(false);
                ClusterMemberResponse {
                    node: node.clone(),
                    observed: record.is_some(),
                    healthy: !quarantined
                        && record
                            .and_then(|record| record.last_error.as_ref())
                            .is_none(),
                    quarantined,
                    consecutive_failures: record
                        .map(|record| record.consecutive_failures)
                        .unwrap_or(0),
                    last_checked_unix_millis: record
                        .and_then(|record| record.last_checked_at)
                        .and_then(system_time_to_unix_millis),
                    last_error: record.and_then(|record| record.last_error.clone()),
                    topology_group: cluster.topology_group(node).map(ToString::to_string),
                    weight: cluster.node_weight(node),
                }
            })
            .collect()
    }

    fn healthy_nodes<'a, I>(&self, nodes: I) -> Vec<String>
    where
        I: IntoIterator<Item = &'a str>,
    {
        let now = Instant::now();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_records(&mut records, now);

        nodes
            .into_iter()
            .filter(|node| {
                !records
                    .get(*node)
                    .and_then(|record| record.quarantined_until)
                    .map(|until| until > now)
                    .unwrap_or(false)
            })
            .map(ToString::to_string)
            .collect()
    }

    fn snapshot_nodes(&self, nodes: &[String]) -> Vec<ControlPeerResponse> {
        let now = Instant::now();
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        prune_expired_records(&mut records, now);

        nodes
            .iter()
            .map(|node| {
                let record = records.get(node);
                let quarantined = record
                    .and_then(|record| record.quarantined_until)
                    .map(|until| until > now)
                    .unwrap_or(false);
                ControlPeerResponse {
                    node: node.clone(),
                    observed: record.is_some(),
                    healthy: !quarantined
                        && record
                            .and_then(|record| record.last_error.as_ref())
                            .is_none(),
                    quarantined,
                    consecutive_failures: record
                        .map(|record| record.consecutive_failures)
                        .unwrap_or(0),
                    last_checked_unix_millis: record
                        .and_then(|record| record.last_checked_at)
                        .and_then(system_time_to_unix_millis),
                    last_error: record.and_then(|record| record.last_error.clone()),
                }
            })
            .collect()
    }

    fn merge_observation(
        &self,
        node: &str,
        observed: bool,
        healthy: bool,
        quarantined: bool,
        consecutive_failures: u64,
        last_checked_unix_millis: Option<u64>,
        last_error: Option<String>,
    ) {
        if !observed && last_checked_unix_millis.is_none() {
            return;
        }

        let remote_checked_at = last_checked_unix_millis.and_then(unix_millis_to_system_time);
        let mut records = self
            .records
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let record = records.entry(node.to_string()).or_default();
        let should_replace = match (record.last_checked_at, remote_checked_at) {
            (None, Some(_)) => true,
            (Some(local), Some(remote)) => remote >= local,
            (None, None) => observed,
            (Some(_), None) => false,
        };
        if !should_replace {
            return;
        }

        record.consecutive_failures = consecutive_failures;
        record.last_checked_at = remote_checked_at;
        record.last_error = if healthy && !quarantined {
            None
        } else {
            last_error
        };
        record.quarantined_until = if quarantined || !healthy {
            Some(Instant::now() + self.failure_backoff)
        } else {
            None
        };
    }
}

impl ControlPlaneState {
    fn new(failure_backoff: Duration) -> Self {
        Self {
            health: NodeHealthTracker::new(failure_backoff),
            view: Mutex::new(ControlPlaneView::default()),
            journal: Mutex::new(ControlJournalState::default()),
        }
    }

    fn snapshot(&self) -> ControlPlaneView {
        self.view
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn set_leader(&self, leader: Option<String>) -> bool {
        let mut current = self
            .view
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if current.leader == leader {
            false
        } else {
            current.leader = leader;
            current.epoch = current.epoch.saturating_add(1);
            true
        }
    }

    fn absorb_remote_state(&self, epoch: u64, leader: Option<String>) -> bool {
        let mut current = self
            .view
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if epoch > current.epoch {
            current.epoch = epoch;
            current.leader = leader;
            true
        } else {
            false
        }
    }

    fn journal_head_index(&self) -> u64 {
        self.journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .entries
            .last()
            .map(|entry| entry.index)
            .unwrap_or(0)
    }

    fn journal_after(&self, after: u64) -> ControlJournalResponse {
        let journal = self
            .journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        ControlJournalResponse {
            head_index: journal.entries.last().map(|entry| entry.index).unwrap_or(0),
            entries: journal
                .entries
                .iter()
                .filter(|entry| entry.index > after)
                .cloned()
                .collect(),
        }
    }

    fn append_local_journal_entry(
        &self,
        kind: impl Into<String>,
        node: Option<String>,
        details: BTreeMap<String, String>,
    ) -> ControlJournalEntry {
        let view = self.snapshot();
        let mut journal = self
            .journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        journal.next_index = journal.next_index.saturating_add(1);
        let entry = ControlJournalEntry {
            index: journal.next_index,
            epoch: view.epoch,
            kind: kind.into(),
            leader: view.leader,
            node,
            created_unix_millis: system_time_to_unix_millis(SystemTime::now()).unwrap_or_default(),
            details,
        };
        journal.entries.push(entry.clone());
        entry
    }

    fn merge_remote_journal_entries(&self, mut entries: Vec<ControlJournalEntry>) -> usize {
        entries.sort_by_key(|entry| entry.index);
        let mut journal = self
            .journal
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let mut merged = 0usize;
        let mut head_index = journal.entries.last().map(|entry| entry.index).unwrap_or(0);
        for entry in entries {
            if entry.index <= head_index {
                continue;
            }
            head_index = entry.index;
            journal.next_index = journal.next_index.max(entry.index);
            journal.entries.push(entry);
            merged += 1;
        }
        merged
    }
}

impl Default for NodeHealthRecord {
    fn default() -> Self {
        Self {
            consecutive_failures: 0,
            last_checked_at: None,
            last_error: None,
            quarantined_until: None,
        }
    }
}

fn prune_expired_records(records: &mut HashMap<String, NodeHealthRecord>, now: Instant) {
    for record in records.values_mut() {
        if record
            .quarantined_until
            .map(|until| until <= now)
            .unwrap_or(false)
        {
            record.quarantined_until = None;
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Debug, Serialize, Deserialize)]
struct IngestResponse {
    ingested_rows: usize,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct RowsIngestRequest {
    rows: Vec<TraceSpanRow>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct LogsIngestRequest {
    rows: Vec<LogRow>,
}

#[derive(Debug, Clone)]
struct SharedEncodedTraceBatch {
    row_count: usize,
    encoded_rows: Arc<Vec<Vec<u8>>>,
}

#[derive(Debug)]
struct NodeWritePlan {
    node: String,
    payload: Bytes,
    row_count: usize,
}

#[derive(Debug, Clone)]
struct SharedEncodedLogBatch {
    row_count: usize,
    encoded_rows: Arc<Vec<Vec<u8>>>,
}

#[derive(Debug)]
struct NodeLogWritePlan {
    node: String,
    payload: Bytes,
    row_count: usize,
}

#[derive(Debug)]
enum RemoteWriteOutcome {
    Success { node: String },
    Failure { node: String, error: String },
}

#[derive(Debug)]
enum RemoteTraceReadOutcome {
    Rows {
        node: String,
        rows: Vec<TraceSpanRow>,
    },
    Empty {
        node: String,
    },
    Failure {
        node: String,
        error: String,
    },
}

#[derive(Debug, Serialize, Deserialize)]
struct TraceResponse {
    trace_id: String,
    rows: Vec<TraceRowResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ServicesResponse {
    services: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TagNamesResponse {
    tags: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TagValuesResponse {
    values: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TraceIndexResponse {
    trace_ids: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct TraceSearchResponse {
    hits: Vec<TraceSearchHit>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogSearchResponse {
    rows: Vec<LogRowResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RebalanceResponse {
    scanned_traces: usize,
    repaired_replicas: usize,
}

#[derive(Debug, Serialize, Deserialize)]
struct TraceRowResponse {
    trace_id: String,
    span_id: String,
    parent_span_id: Option<String>,
    name: String,
    start_unix_nano: i64,
    end_unix_nano: i64,
    time_unix_nano: i64,
    fields: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct LogRowResponse {
    log_id: String,
    time_unix_nano: i64,
    observed_time_unix_nano: Option<i64>,
    severity_number: Option<i32>,
    severity_text: Option<String>,
    body: String,
    trace_id: Option<String>,
    span_id: Option<String>,
    fields: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Deserialize, Clone)]
struct SearchQuery {
    start_unix_nano: i64,
    end_unix_nano: i64,
    service_name: Option<String>,
    operation_name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    field_filter: Vec<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
struct LogSearchQuery {
    start_unix_nano: i64,
    end_unix_nano: i64,
    service_name: Option<String>,
    severity_text: Option<String>,
    #[serde(default, deserialize_with = "deserialize_string_vec")]
    field_filter: Vec<String>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
struct ControlJournalQuery {
    after: Option<u64>,
}

#[derive(Debug, Deserialize, Clone)]
struct JaegerTraceQuery {
    service: Option<String>,
    operation: Option<String>,
    tags: Option<String>,
    #[serde(rename = "minDuration")]
    min_duration: Option<String>,
    #[serde(rename = "maxDuration")]
    max_duration: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
struct JaegerDependenciesQuery {
    #[serde(rename = "endTs")]
    end_ts: Option<i64>,
    lookback: Option<i64>,
}

#[derive(Debug, Deserialize, Clone)]
struct TempoSearchQuery {
    tags: Option<String>,
    q: Option<String>,
    start: Option<i64>,
    end: Option<i64>,
    limit: Option<usize>,
}

#[derive(Debug, Deserialize, Clone)]
struct TempoTagQuery {
    scope: Option<String>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct JaegerResponse<T> {
    data: T,
    errors: Option<Vec<JaegerResponseError>>,
    limit: usize,
    offset: usize,
    total: usize,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
struct JaegerDependencyLink {
    parent: String,
    child: String,
    #[serde(rename = "callCount")]
    call_count: u64,
}

#[derive(Debug, Serialize)]
struct JaegerResponseError {
    code: u16,
    msg: String,
}

#[derive(Debug, Serialize)]
struct JaegerTrace {
    processes: BTreeMap<String, JaegerProcess>,
    spans: Vec<JaegerSpan>,
    #[serde(rename = "traceID")]
    trace_id: String,
    warnings: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Clone)]
struct JaegerProcess {
    #[serde(rename = "serviceName")]
    service_name: String,
    tags: Vec<JaegerTag>,
}

#[derive(Debug, Serialize)]
struct JaegerSpan {
    duration: i64,
    logs: Vec<JaegerLog>,
    #[serde(rename = "operationName")]
    operation_name: String,
    #[serde(rename = "processID")]
    process_id: String,
    references: Vec<JaegerReference>,
    #[serde(rename = "spanID")]
    span_id: String,
    #[serde(rename = "startTime")]
    start_time: i64,
    tags: Vec<JaegerTag>,
    #[serde(rename = "traceID")]
    trace_id: String,
    warnings: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
struct JaegerReference {
    #[serde(rename = "refType")]
    ref_type: String,
    #[serde(rename = "spanID")]
    span_id: String,
    #[serde(rename = "traceID")]
    trace_id: String,
}

#[derive(Debug, Serialize)]
struct JaegerLog {
    timestamp: i64,
    fields: Vec<JaegerTag>,
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct JaegerTag {
    key: String,
    #[serde(rename = "type")]
    value_type: &'static str,
    value: String,
}

#[derive(Debug, Serialize)]
struct TempoSearchResponse {
    traces: Vec<TempoSearchTrace>,
    metrics: TempoQueryMetrics,
}

#[derive(Debug, Serialize)]
struct TempoSearchTrace {
    #[serde(rename = "traceID")]
    trace_id: String,
    #[serde(rename = "rootServiceName")]
    root_service_name: String,
    #[serde(rename = "rootTraceName")]
    root_trace_name: String,
    #[serde(rename = "startTimeUnixNano")]
    start_time_unix_nano: String,
    #[serde(rename = "durationMs")]
    duration_ms: u64,
}

#[derive(Debug, Serialize)]
struct TempoTagNamesResponse {
    #[serde(rename = "tagNames")]
    tag_names: Vec<String>,
    metrics: TempoQueryMetrics,
}

#[derive(Debug, Serialize)]
struct TempoTagNamesV2Response {
    scopes: Vec<TempoTagScope>,
    metrics: TempoQueryMetrics,
}

#[derive(Debug, Serialize)]
struct TempoTagScope {
    name: String,
    tags: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TempoTagValuesResponse {
    #[serde(rename = "tagValues")]
    tag_values: Vec<String>,
    metrics: TempoQueryMetrics,
}

#[derive(Debug, Serialize)]
struct TempoTagValuesV2Response {
    #[serde(rename = "tagValues")]
    tag_values: Vec<TempoTypedTagValue>,
    metrics: TempoQueryMetrics,
}

#[derive(Debug, Serialize)]
struct TempoTypedTagValue {
    #[serde(rename = "type")]
    value_type: &'static str,
    value: String,
}

#[derive(Debug, Serialize)]
struct TempoQueryMetrics {
    #[serde(rename = "inspectedTraces", skip_serializing_if = "Option::is_none")]
    inspected_traces: Option<u64>,
    #[serde(rename = "inspectedBytes")]
    inspected_bytes: String,
    #[serde(rename = "totalBlocks", skip_serializing_if = "Option::is_none")]
    total_blocks: Option<u64>,
}

#[derive(Debug)]
struct ParsedTempoSearchQuery {
    search_request: TraceSearchRequest,
    limit: usize,
    substring_filters: Vec<(String, String)>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum OneOrManyStrings {
    One(String),
    Many(Vec<String>),
}

pub fn build_router(storage: MemoryStorageEngine) -> Router {
    build_router_with_limits(storage, ApiLimitsConfig::default())
}

pub fn build_router_with_limits(storage: MemoryStorageEngine, limits: ApiLimitsConfig) -> Router {
    build_router_with_storage_and_limits(Arc::new(storage), limits)
}

pub fn build_router_with_storage(storage: Arc<dyn StorageEngine>) -> Router {
    build_router_with_storage_and_limits(storage, ApiLimitsConfig::default())
}

pub fn build_router_with_storage_and_limits(
    storage: Arc<dyn StorageEngine>,
    limits: ApiLimitsConfig,
) -> Router {
    build_router_with_storage_auth_and_limits(storage, AuthConfig::default(), limits)
}

pub fn build_router_with_storage_and_trace_ingest_profile(
    storage: Arc<dyn StorageEngine>,
    trace_ingest_profile: TraceIngestProfile,
) -> Router {
    build_router_with_storage_auth_limits_and_trace_ingest_profile(
        storage,
        AuthConfig::default(),
        ApiLimitsConfig::default(),
        trace_ingest_profile,
    )
}

pub fn build_router_with_storage_auth_and_limits(
    storage: Arc<dyn StorageEngine>,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> Router {
    build_router_with_storage_auth_limits_and_trace_ingest_profile(
        storage,
        auth,
        limits,
        TraceIngestProfile::Default,
    )
}

pub fn build_router_with_storage_auth_limits_and_trace_ingest_profile(
    storage: Arc<dyn StorageEngine>,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
    trace_ingest_profile: TraceIngestProfile,
) -> Router {
    let state = Arc::new(StorageState {
        storage: storage.clone(),
        query: QueryService::new(storage),
        trace_ingest_profile,
    });

    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(storage_metrics))
        .merge(apply_optional_bearer_auth(
            apply_api_limits(
                Router::new()
                    .route("/api/v1/services", get(list_services_local))
                    .route("/api/v1/logs/search", get(search_logs_local))
                    .route("/api/v1/traces/search", get(search_traces_local))
                    .route("/api/v1/logs/ingest", post(ingest_logs_local))
                    .route("/api/v1/traces/ingest", post(ingest_traces_local))
                    .route("/v1/logs", post(ingest_logs_local))
                    .route("/v1/traces", post(ingest_traces_local))
                    .route(
                        "/insert/opentelemetry/v1/traces",
                        post(ingest_traces_local),
                    )
                    .route(
                        "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
                        post(ingest_logs_grpc_local),
                    )
                    .route(
                        "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
                        post(ingest_traces_grpc_local),
                    )
                    .route("/api/v1/traces/:trace_id", get(get_trace_local))
                    .route("/api/traces/:trace_id", get(get_trace_tempo_local))
                    .route("/api/v2/traces/:trace_id", get(get_trace_tempo_local))
                    .route("/api/search", get(search_traces_tempo_local))
                    .route("/api/search/tags", get(list_tempo_tags_local))
                    .route("/api/v2/search/tags", get(list_tempo_tags_v2_local))
                    .route(
                        "/api/search/tag/:tag_name/values",
                        get(list_tempo_tag_values_local),
                    )
                    .route(
                        "/api/v2/search/tag/:tag_name/values",
                        get(list_tempo_tag_values_v2_local),
                    )
                    .route(
                        "/select/jaeger/api/services",
                        get(list_services_jaeger_local),
                    )
                    .route(
                        "/select/jaeger/api/services/:service_name/operations",
                        get(list_operations_jaeger_local),
                    )
                    .route("/select/jaeger/api/traces", get(search_traces_jaeger_local))
                    .route(
                        "/select/jaeger/api/dependencies",
                        get(get_jaeger_dependencies_local),
                    )
                    .route(
                        "/select/jaeger/api/traces/:trace_id",
                        get(get_trace_jaeger_local),
                    ),
                &limits,
            ),
            auth.public_bearer_token().map(ToString::to_string),
        ))
        .with_state(state)
}

pub fn build_storage_router(storage: Arc<dyn StorageEngine>) -> Router {
    build_storage_router_with_limits(storage, ApiLimitsConfig::default())
}

pub fn build_storage_router_with_limits(
    storage: Arc<dyn StorageEngine>,
    limits: ApiLimitsConfig,
) -> Router {
    build_storage_router_with_auth_and_limits(storage, AuthConfig::default(), limits)
}

pub fn build_storage_router_with_auth_and_limits(
    storage: Arc<dyn StorageEngine>,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> Router {
    let state = Arc::new(StorageState {
        storage: storage.clone(),
        query: QueryService::new(storage),
        trace_ingest_profile: TraceIngestProfile::Default,
    });

    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(storage_metrics))
        .merge(apply_optional_bearer_auth(
            apply_api_limits(
                Router::new()
                    .route("/internal/v1/rows", post(append_rows_internal))
                    .route("/internal/v1/logs", post(append_logs_internal))
                    .route("/internal/v1/traces/index", get(list_trace_ids_local))
                    .route("/internal/v1/services", get(list_services_local))
                    .route("/internal/v1/tags", get(list_tags_local))
                    .route(
                        "/internal/v1/tags/:tag_name/values",
                        get(list_tag_values_local),
                    )
                    .route("/internal/v1/logs/search", get(search_logs_local))
                    .route("/internal/v1/traces/search", get(search_traces_local))
                    .route("/internal/v1/traces/:trace_id", get(get_trace_local)),
                &limits,
            ),
            auth.internal_bearer_token().map(ToString::to_string),
        ))
        .with_state(state)
}

pub fn build_insert_router(cluster: ClusterConfig) -> Router {
    build_insert_router_with_limits(cluster, ApiLimitsConfig::default())
}

pub fn build_insert_router_with_limits(cluster: ClusterConfig, limits: ApiLimitsConfig) -> Router {
    build_insert_router_with_auth_and_limits(cluster, AuthConfig::default(), limits)
}

pub fn build_insert_router_with_auth_and_limits(
    cluster: ClusterConfig,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> Router {
    build_insert_router_with_client_auth_and_limits(cluster, Client::new(), auth, limits)
}

pub fn build_insert_router_with_client_auth_and_limits(
    cluster: ClusterConfig,
    http_client: impl Into<ClusterHttpClient>,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> Router {
    let state = Arc::new(InsertState {
        health: NodeHealthTracker::new(cluster.failure_backoff()),
        cluster,
        http_client: http_client.into(),
        metrics: ClusterMetrics::default(),
        auth: auth.clone(),
    });

    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(insert_metrics))
        .merge(apply_optional_bearer_auth(
            apply_api_limits(
                Router::new()
                    .route("/api/v1/logs/ingest", post(ingest_logs_cluster))
                    .route("/api/v1/traces/ingest", post(ingest_traces_cluster))
                    .route("/v1/logs", post(ingest_logs_cluster))
                    .route("/v1/traces", post(ingest_traces_cluster))
                    .route(
                        "/insert/opentelemetry/v1/traces",
                        post(ingest_traces_cluster),
                    )
                    .route(
                        "/opentelemetry.proto.collector.logs.v1.LogsService/Export",
                        post(ingest_logs_grpc_cluster),
                    )
                    .route(
                        "/opentelemetry.proto.collector.trace.v1.TraceService/Export",
                        post(ingest_traces_grpc_cluster),
                    ),
                &limits,
            ),
            auth.public_bearer_token().map(ToString::to_string),
        ))
        .with_state(state)
}

pub fn build_select_router(cluster: ClusterConfig) -> Router {
    build_select_router_with_limits(cluster, ApiLimitsConfig::default())
}

pub fn build_select_router_with_limits(cluster: ClusterConfig, limits: ApiLimitsConfig) -> Router {
    build_select_router_with_auth_and_limits(cluster, AuthConfig::default(), limits)
}

pub fn build_select_router_with_auth_and_limits(
    cluster: ClusterConfig,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> Router {
    build_select_router_with_client_auth_and_limits(cluster, Client::new(), auth, limits)
}

pub fn build_select_router_with_client_auth_and_limits(
    cluster: ClusterConfig,
    http_client: impl Into<ClusterHttpClient>,
    auth: AuthConfig,
    limits: ApiLimitsConfig,
) -> Router {
    let state = Arc::new(SelectState {
        health: NodeHealthTracker::new(cluster.failure_backoff()),
        control: ControlPlaneState::new(cluster.failure_backoff()),
        cluster,
        http_client: http_client.into(),
        metrics: ClusterMetrics::default(),
        auth: auth.clone(),
    });

    Router::new()
        .route("/healthz", get(healthz))
        .route("/metrics", get(select_metrics))
        .merge(apply_optional_bearer_auth(
            apply_api_limits(
                Router::new()
                    .route("/api/v1/services", get(list_services_cluster))
                    .route("/api/v1/logs/search", get(search_logs_cluster))
                    .route("/api/v1/traces/search", get(search_traces_cluster))
                    .route("/api/v1/traces/:trace_id", get(get_trace_cluster))
                    .route("/api/traces/:trace_id", get(get_trace_tempo_cluster))
                    .route("/api/v2/traces/:trace_id", get(get_trace_tempo_cluster))
                    .route("/api/search", get(search_traces_tempo_cluster))
                    .route("/api/search/tags", get(list_tempo_tags_cluster))
                    .route("/api/v2/search/tags", get(list_tempo_tags_v2_cluster))
                    .route(
                        "/api/search/tag/:tag_name/values",
                        get(list_tempo_tag_values_cluster),
                    )
                    .route(
                        "/api/v2/search/tag/:tag_name/values",
                        get(list_tempo_tag_values_v2_cluster),
                    )
                    .route(
                        "/select/jaeger/api/services",
                        get(list_services_jaeger_cluster),
                    )
                    .route(
                        "/select/jaeger/api/services/:service_name/operations",
                        get(list_operations_jaeger_cluster),
                    )
                    .route(
                        "/select/jaeger/api/traces",
                        get(search_traces_jaeger_cluster),
                    )
                    .route(
                        "/select/jaeger/api/dependencies",
                        get(get_jaeger_dependencies_cluster),
                    )
                    .route(
                        "/select/jaeger/api/traces/:trace_id",
                        get(get_trace_jaeger_cluster),
                    ),
                &limits,
            ),
            auth.public_bearer_token().map(ToString::to_string),
        ))
        .merge(apply_optional_bearer_auth(
            apply_api_limits(
                Router::new()
                    .route("/admin/v1/cluster/members", get(list_cluster_members))
                    .route("/admin/v1/cluster/leader", get(get_cluster_leader))
                    .route("/admin/v1/cluster/state", get(get_cluster_state))
                    .route("/admin/v1/cluster/journal", get(get_cluster_journal))
                    .route(
                        "/admin/v1/cluster/journal/append",
                        post(append_cluster_journal),
                    )
                    .route("/admin/v1/cluster/rebalance", post(rebalance_cluster)),
                &limits,
            ),
            auth.admin_bearer_token().map(ToString::to_string),
        ))
        .with_state(state)
}

pub fn spawn_background_rebalance_task(
    select_base_url: String,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    spawn_background_rebalance_task_with_client_and_auth(
        select_base_url,
        interval,
        Client::new(),
        None,
    )
}

pub fn spawn_background_rebalance_task_with_auth(
    select_base_url: String,
    interval: Duration,
    bearer_token: Option<String>,
) -> tokio::task::JoinHandle<()> {
    spawn_background_rebalance_task_with_client_and_auth(
        select_base_url,
        interval,
        Client::new(),
        bearer_token,
    )
}

pub fn spawn_background_rebalance_task_with_client_and_auth(
    select_base_url: String,
    interval: Duration,
    client: impl Into<ClusterHttpClient>,
    bearer_token: Option<String>,
) -> tokio::task::JoinHandle<()> {
    let client = client.into();
    tokio::spawn(async move {
        let select_base_url = select_base_url.trim_end_matches('/').to_string();
        loop {
            tokio::time::sleep(interval).await;
            let leader_request = client.get(format!("{select_base_url}/admin/v1/cluster/leader"));
            let leader_response = match apply_bearer_auth(leader_request, bearer_token.as_deref())
                .send()
                .await
            {
                Ok(response) => response,
                Err(error) => {
                    warn!(
                        error = %error,
                        base_url = %select_base_url,
                        "background rebalance leader probe failed"
                    );
                    continue;
                }
            };
            let should_rebalance = match leader_response.json::<ClusterLeaderResponse>().await {
                Ok(leader) => leader.local || leader.leader.is_none(),
                Err(error) => {
                    warn!(
                        error = %error,
                        base_url = %select_base_url,
                        "background rebalance leader decode failed"
                    );
                    continue;
                }
            };
            if !should_rebalance {
                continue;
            }
            let request = client.post(format!("{select_base_url}/admin/v1/cluster/rebalance"));
            match apply_bearer_auth(request, bearer_token.as_deref())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => {
                    warn!(
                        status = %response.status(),
                        base_url = %select_base_url,
                        "background rebalance returned non-success status"
                    );
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        base_url = %select_base_url,
                        "background rebalance request failed"
                    );
                }
            }
        }
    })
}

#[allow(dead_code)]
pub fn spawn_background_control_refresh_task(
    select_base_url: String,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    spawn_background_control_refresh_task_with_client_and_auth(
        select_base_url,
        interval,
        Client::new(),
        None,
    )
}

#[allow(dead_code)]
pub fn spawn_background_control_refresh_task_with_auth(
    select_base_url: String,
    interval: Duration,
    bearer_token: Option<String>,
) -> tokio::task::JoinHandle<()> {
    spawn_background_control_refresh_task_with_client_and_auth(
        select_base_url,
        interval,
        Client::new(),
        bearer_token,
    )
}

#[allow(dead_code)]
pub fn spawn_background_control_refresh_task_with_client_and_auth(
    select_base_url: String,
    interval: Duration,
    client: impl Into<ClusterHttpClient>,
    bearer_token: Option<String>,
) -> tokio::task::JoinHandle<()> {
    let client = client.into();
    tokio::spawn(async move {
        let select_base_url = select_base_url.trim_end_matches('/').to_string();
        loop {
            tokio::time::sleep(interval).await;
            let request = client.get(format!("{select_base_url}/admin/v1/cluster/leader"));
            match apply_bearer_auth(request, bearer_token.as_deref())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => {
                    warn!(
                        status = %response.status(),
                        base_url = %select_base_url,
                        "background control refresh returned non-success status"
                    );
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        base_url = %select_base_url,
                        "background control refresh request failed"
                    );
                }
            }
        }
    })
}

pub fn spawn_background_membership_refresh_task(
    select_base_url: String,
    interval: Duration,
) -> tokio::task::JoinHandle<()> {
    spawn_background_membership_refresh_task_with_client_and_auth(
        select_base_url,
        interval,
        Client::new(),
        None,
    )
}

pub fn spawn_background_membership_refresh_task_with_auth(
    select_base_url: String,
    interval: Duration,
    bearer_token: Option<String>,
) -> tokio::task::JoinHandle<()> {
    spawn_background_membership_refresh_task_with_client_and_auth(
        select_base_url,
        interval,
        Client::new(),
        bearer_token,
    )
}

pub fn spawn_background_membership_refresh_task_with_client_and_auth(
    select_base_url: String,
    interval: Duration,
    client: impl Into<ClusterHttpClient>,
    bearer_token: Option<String>,
) -> tokio::task::JoinHandle<()> {
    let client = client.into();
    tokio::spawn(async move {
        let select_base_url = select_base_url.trim_end_matches('/').to_string();
        loop {
            tokio::time::sleep(interval).await;
            let request = client.get(format!("{select_base_url}/admin/v1/cluster/members"));
            match apply_bearer_auth(request, bearer_token.as_deref())
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {}
                Ok(response) => {
                    warn!(
                        status = %response.status(),
                        base_url = %select_base_url,
                        "background membership refresh returned non-success status"
                    );
                }
                Err(error) => {
                    warn!(
                        error = %error,
                        base_url = %select_base_url,
                        "background membership refresh request failed"
                    );
                }
            }
        }
    })
}

fn apply_api_limits<S>(router: Router<S>, limits: &ApiLimitsConfig) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let semaphore = Arc::new(Semaphore::new(limits.concurrency_limit));
    router
        .layer(DefaultBodyLimit::max(limits.max_request_body_bytes))
        .route_layer(middleware::from_fn_with_state(
            semaphore,
            enforce_concurrency_limit,
        ))
}

fn apply_optional_bearer_auth<S>(router: Router<S>, bearer_token: Option<String>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    let Some(bearer_token) = sanitize_bearer_token_option(bearer_token) else {
        return router;
    };

    router.route_layer(middleware::from_fn_with_state(
        Arc::new(bearer_token),
        enforce_bearer_token,
    ))
}

async fn enforce_concurrency_limit(
    State(semaphore): State<Arc<Semaphore>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let permit = semaphore
        .try_acquire_owned()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let response = next.run(request).await;
    drop(permit);
    Ok(response)
}

async fn enforce_bearer_token(
    State(expected_token): State<Arc<String>>,
    request: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    if has_valid_bearer_token(request.headers(), expected_token.as_str()) {
        Ok(next.run(request).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

async fn ingest_traces_local(
    State(state): State<Arc<StorageState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let protobuf = is_otlp_protobuf_request(&headers);
    let blocks = decode_otlp_http_trace_blocks(
        &headers,
        &body,
        state.storage.preferred_trace_ingest_shards(),
    )
    .map_err(|error| {
        warn!(protobuf, error = %error, "trace ingest decode failed");
        bad_request(error)
    })?;
    let ingested_rows: usize = blocks.iter().map(TraceBlock::row_count).sum();
    append_trace_blocks_with_profile(state.storage.clone(), blocks, state.trace_ingest_profile)
        .await
        .map_err(|error| {
            warn!(protobuf, ingested_rows, error = %error, "trace ingest append failed");
            internal_server_error(error)
        })?;
    debug!(ingested_rows, protobuf, "ingested trace rows");

    Ok(otlp_http_ingest_response(protobuf, ingested_rows))
}

async fn ingest_logs_local(
    State(state): State<Arc<StorageState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<IngestResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request = decode_otlp_http_logs_request(&headers, &body).map_err(bad_request)?;
    let rows = flatten_export_logs_request(&request).map_err(bad_request)?;
    let ingested_rows = rows.len();
    state
        .storage
        .append_logs(rows)
        .map_err(internal_server_error)?;
    info!(ingested_rows, "ingested log rows");

    Ok(Json(IngestResponse { ingested_rows }))
}

async fn ingest_traces_grpc_local(State(state): State<Arc<StorageState>>, body: Bytes) -> Response {
    let blocks =
        match decode_otlp_grpc_trace_blocks(&body, state.storage.preferred_trace_ingest_shards()) {
            Ok(blocks) => blocks,
            Err(error) => return grpc_error_response("3", error),
        };
    let ingested_rows: usize = blocks.iter().map(TraceBlock::row_count).sum();
    if let Err(error) =
        append_trace_blocks_with_profile(state.storage.clone(), blocks, state.trace_ingest_profile)
            .await
    {
        return grpc_error_response("13", error.to_string());
    }
    debug!(ingested_rows, "ingested trace rows via OTLP gRPC");
    grpc_ok_response()
}

async fn append_trace_blocks_with_profile(
    storage: Arc<dyn StorageEngine>,
    blocks: Vec<TraceBlock>,
    trace_ingest_profile: TraceIngestProfile,
) -> Result<(), StorageError> {
    match trace_ingest_profile {
        TraceIngestProfile::Default => append_trace_blocks_blocking(storage, blocks).await,
        TraceIngestProfile::Throughput => storage.append_trace_blocks(blocks),
    }
}

async fn append_trace_blocks_blocking(
    storage: Arc<dyn StorageEngine>,
    blocks: Vec<TraceBlock>,
) -> Result<(), StorageError> {
    match tokio::runtime::Handle::try_current()
        .map(|handle| handle.runtime_flavor())
        .ok()
    {
        Some(tokio::runtime::RuntimeFlavor::MultiThread) => {
            task::block_in_place(move || storage.append_trace_blocks(blocks))
        }
        _ => task::spawn_blocking(move || storage.append_trace_blocks(blocks))
            .await
            .map_err(|error| {
                StorageError::Message(format!("trace append task join error: {error}"))
            })?,
    }
}

async fn ingest_logs_grpc_local(State(state): State<Arc<StorageState>>, body: Bytes) -> Response {
    let request = match decode_otlp_grpc_logs_request(&body) {
        Ok(request) => request,
        Err(error) => return grpc_error_response("3", error),
    };
    let rows = match flatten_export_logs_request(&request) {
        Ok(rows) => rows,
        Err(error) => return grpc_error_response("3", error.to_string()),
    };
    let ingested_rows = rows.len();
    if let Err(error) = state.storage.append_logs(rows) {
        return grpc_error_response("13", error.to_string());
    }
    info!(ingested_rows, "ingested log rows via OTLP gRPC");
    grpc_ok_response()
}

async fn append_rows_internal(
    State(state): State<Arc<StorageState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<IngestResponse>, (StatusCode, Json<ErrorResponse>)> {
    let rows = decode_internal_rows_request(&headers, &body).map_err(bad_request)?;
    let ingested_rows = rows.len();
    state
        .storage
        .append_rows(rows)
        .map_err(internal_server_error)?;
    debug!(ingested_rows, "ingested replicated trace rows");

    Ok(Json(IngestResponse { ingested_rows }))
}

async fn append_logs_internal(
    State(state): State<Arc<StorageState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<IngestResponse>, (StatusCode, Json<ErrorResponse>)> {
    let rows = decode_internal_logs_request(&headers, &body).map_err(bad_request)?;
    let ingested_rows = rows.len();
    state
        .storage
        .append_logs(rows)
        .map_err(internal_server_error)?;
    debug!(ingested_rows, "ingested replicated log rows");

    Ok(Json(IngestResponse { ingested_rows }))
}

async fn ingest_traces_cluster(
    State(state): State<Arc<InsertState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let protobuf = is_otlp_protobuf_request(&headers);
    let rows = decode_otlp_http_trace_rows(&headers, &body).map_err(bad_request)?;
    let ingested_rows = rows.len();
    let write_plans =
        build_node_write_plans(state.as_ref(), rows).map_err(internal_server_error)?;

    let mut successful_writes = 0usize;
    let mut failed_nodes = Vec::new();
    let mut write_tasks = JoinSet::new();
    let internal_bearer_token = state.auth.internal_bearer_token().map(ToString::to_string);

    for NodeWritePlan {
        node,
        payload,
        row_count,
    } in write_plans
    {
        state.metrics.record_write_batch(row_count);
        let client = state.http_client.clone();
        let bearer_token = internal_bearer_token.clone();
        write_tasks.spawn(async move {
            let response = apply_bearer_auth(
                client.post(format!("{node}/internal/v1/rows")),
                bearer_token.as_deref(),
            )
            .header("content-type", INTERNAL_TRACE_ROWS_CONTENT_TYPE)
            .body(payload)
            .send()
            .await;

            match response {
                Ok(response) if response.status().is_success() => {
                    RemoteWriteOutcome::Success { node }
                }
                Ok(response) => RemoteWriteOutcome::Failure {
                    node,
                    error: response.status().to_string(),
                },
                Err(error) => RemoteWriteOutcome::Failure {
                    node,
                    error: error.to_string(),
                },
            }
        });
    }

    while let Some(result) = write_tasks.join_next().await {
        match result {
            Ok(RemoteWriteOutcome::Success { node }) => {
                state.health.mark_success(&node);
                successful_writes += 1;
            }
            Ok(RemoteWriteOutcome::Failure { node, error }) => {
                state.metrics.record_write_error();
                if state.health.mark_failure(&node, error.clone()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "remote write failed");
                failed_nodes.push(format!("{node}: {error}"));
            }
            Err(error) => {
                state.metrics.record_write_error();
                failed_nodes.push(format!("join-error: {error}"));
            }
        }
    }

    if successful_writes < state.cluster.write_quorum() {
        return Err(service_unavailable(format!(
            "write quorum not met: successes={}, required={}, failures={}",
            successful_writes,
            state.cluster.write_quorum(),
            failed_nodes.join(", ")
        )));
    }

    debug!(
        ingested_rows,
        replication_factor = state.cluster.replication_factor(),
        write_quorum = state.cluster.write_quorum(),
        successful_writes,
        "ingested and replicated trace rows"
    );
    Ok(otlp_http_ingest_response(protobuf, ingested_rows))
}

async fn ingest_logs_cluster(
    State(state): State<Arc<InsertState>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<IngestResponse>, (StatusCode, Json<ErrorResponse>)> {
    let request = decode_otlp_http_logs_request(&headers, &body).map_err(bad_request)?;
    let rows = flatten_export_logs_request(&request).map_err(bad_request)?;
    let ingested_rows = rows.len();
    replicate_log_rows_cluster(state.as_ref(), rows, ingested_rows).await
}

async fn ingest_traces_grpc_cluster(
    State(state): State<Arc<InsertState>>,
    body: Bytes,
) -> Response {
    let rows = match decode_otlp_grpc_trace_rows(&body) {
        Ok(rows) => rows,
        Err(error) => return grpc_error_response("3", error),
    };
    let ingested_rows = rows.len();
    let write_plans = match build_node_write_plans(state.as_ref(), rows) {
        Ok(write_plans) => write_plans,
        Err(error) => return grpc_error_response("13", error),
    };

    let mut successful_writes = 0usize;
    let mut failed_nodes = Vec::new();
    let mut write_tasks = JoinSet::new();
    let internal_bearer_token = state.auth.internal_bearer_token().map(ToString::to_string);

    for NodeWritePlan {
        node,
        payload,
        row_count,
    } in write_plans
    {
        state.metrics.record_write_batch(row_count);
        let client = state.http_client.clone();
        let bearer_token = internal_bearer_token.clone();
        write_tasks.spawn(async move {
            let response = apply_bearer_auth(
                client.post(format!("{node}/internal/v1/rows")),
                bearer_token.as_deref(),
            )
            .header("content-type", INTERNAL_TRACE_ROWS_CONTENT_TYPE)
            .body(payload)
            .send()
            .await;

            match response {
                Ok(response) if response.status().is_success() => {
                    RemoteWriteOutcome::Success { node }
                }
                Ok(response) => RemoteWriteOutcome::Failure {
                    node,
                    error: response.status().to_string(),
                },
                Err(error) => RemoteWriteOutcome::Failure {
                    node,
                    error: error.to_string(),
                },
            }
        });
    }

    while let Some(result) = write_tasks.join_next().await {
        match result {
            Ok(RemoteWriteOutcome::Success { node }) => {
                state.health.mark_success(&node);
                successful_writes += 1;
            }
            Ok(RemoteWriteOutcome::Failure { node, error }) => {
                state.metrics.record_write_error();
                if state.health.mark_failure(&node, error.clone()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "remote gRPC write failed");
                failed_nodes.push(format!("{node}: {error}"));
            }
            Err(error) => {
                state.metrics.record_write_error();
                failed_nodes.push(format!("join-error: {error}"));
            }
        }
    }

    if successful_writes < state.cluster.write_quorum() {
        return grpc_error_response(
            "14",
            format!(
                "write quorum not met: successes={}, required={}, failures={}",
                successful_writes,
                state.cluster.write_quorum(),
                failed_nodes.join(", ")
            ),
        );
    }

    debug!(
        ingested_rows,
        replication_factor = state.cluster.replication_factor(),
        write_quorum = state.cluster.write_quorum(),
        successful_writes,
        "ingested and replicated trace rows via OTLP gRPC"
    );
    grpc_ok_response()
}

async fn ingest_logs_grpc_cluster(State(state): State<Arc<InsertState>>, body: Bytes) -> Response {
    let request = match decode_otlp_grpc_logs_request(&body) {
        Ok(request) => request,
        Err(error) => return grpc_error_response("3", error),
    };
    let rows = match flatten_export_logs_request(&request) {
        Ok(rows) => rows,
        Err(error) => return grpc_error_response("3", error.to_string()),
    };
    let ingested_rows = rows.len();

    match replicate_log_rows_cluster(state.as_ref(), rows, ingested_rows).await {
        Ok(_) => grpc_ok_response(),
        Err((status, Json(error))) => {
            grpc_error_response(grpc_status_for_http_status(status), error.error)
        }
    }
}

async fn replicate_log_rows_cluster(
    state: &InsertState,
    rows: Vec<LogRow>,
    ingested_rows: usize,
) -> Result<Json<IngestResponse>, (StatusCode, Json<ErrorResponse>)> {
    let write_plans = build_node_log_write_plans(state, rows).map_err(internal_server_error)?;

    let mut successful_writes = 0usize;
    let mut failed_nodes = Vec::new();
    let mut write_tasks = JoinSet::new();
    let internal_bearer_token = state.auth.internal_bearer_token().map(ToString::to_string);

    for NodeLogWritePlan {
        node,
        payload,
        row_count,
    } in write_plans
    {
        state.metrics.record_write_batch(row_count);
        let client = state.http_client.clone();
        let bearer_token = internal_bearer_token.clone();
        write_tasks.spawn(async move {
            let response = apply_bearer_auth(
                client.post(format!("{node}/internal/v1/logs")),
                bearer_token.as_deref(),
            )
            .header("content-type", INTERNAL_LOG_ROWS_CONTENT_TYPE)
            .body(payload)
            .send()
            .await;

            match response {
                Ok(response) if response.status().is_success() => {
                    RemoteWriteOutcome::Success { node }
                }
                Ok(response) => RemoteWriteOutcome::Failure {
                    node,
                    error: response.status().to_string(),
                },
                Err(error) => RemoteWriteOutcome::Failure {
                    node,
                    error: error.to_string(),
                },
            }
        });
    }

    while let Some(result) = write_tasks.join_next().await {
        match result {
            Ok(RemoteWriteOutcome::Success { node }) => {
                state.health.mark_success(&node);
                successful_writes += 1;
            }
            Ok(RemoteWriteOutcome::Failure { node, error }) => {
                state.metrics.record_write_error();
                if state.health.mark_failure(&node, error.clone()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "remote log write failed");
                failed_nodes.push(format!("{node}: {error}"));
            }
            Err(error) => {
                state.metrics.record_write_error();
                failed_nodes.push(format!("join-error: {error}"));
            }
        }
    }

    if successful_writes < state.cluster.write_quorum() {
        return Err(service_unavailable(format!(
            "write quorum not met: successes={}, required={}, failures={}",
            successful_writes,
            state.cluster.write_quorum(),
            failed_nodes.join(", ")
        )));
    }

    Ok(Json(IngestResponse { ingested_rows }))
}

async fn get_trace_local(
    State(state): State<Arc<StorageState>>,
    Path(trace_id): Path<String>,
) -> Json<TraceResponse> {
    Json(build_trace_response(&state.query, trace_id))
}

async fn get_trace_cluster(
    State(state): State<Arc<SelectState>>,
    Path(trace_id): Path<String>,
) -> Result<Json<TraceResponse>, (StatusCode, Json<ErrorResponse>)> {
    let rows = fetch_trace_rows_cluster(state.as_ref(), &trace_id).await?;
    Ok(Json(build_trace_response_from_rows(trace_id, rows)))
}

async fn list_services_local(State(state): State<Arc<StorageState>>) -> Json<ServicesResponse> {
    Json(ServicesResponse {
        services: state.query.list_services(),
    })
}

async fn list_tags_local(State(state): State<Arc<StorageState>>) -> Json<TagNamesResponse> {
    Json(TagNamesResponse {
        tags: state.query.list_field_names(),
    })
}

async fn list_tag_values_local(
    State(state): State<Arc<StorageState>>,
    Path(tag_name): Path<String>,
) -> Json<TagValuesResponse> {
    Json(TagValuesResponse {
        values: state.query.list_field_values(&tag_name),
    })
}

async fn list_trace_ids_local(State(state): State<Arc<StorageState>>) -> Json<TraceIndexResponse> {
    Json(TraceIndexResponse {
        trace_ids: state.storage.list_trace_ids(),
    })
}

async fn list_services_cluster(
    State(state): State<Arc<SelectState>>,
) -> Result<Json<ServicesResponse>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(ServicesResponse {
        services: gather_services_cluster(state.as_ref()).await?,
    }))
}

async fn search_traces_local(
    State(state): State<Arc<StorageState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<TraceSearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(TraceSearchResponse {
        hits: state.query.search_traces(&query.as_request()?),
    }))
}

async fn search_logs_local(
    State(state): State<Arc<StorageState>>,
    Query(query): Query<LogSearchQuery>,
) -> Result<Json<LogSearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(LogSearchResponse {
        rows: state
            .query
            .search_logs(&query.as_request()?)
            .into_iter()
            .map(log_row_response_from_log_row)
            .collect(),
    }))
}

async fn search_traces_cluster(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<SearchQuery>,
) -> Result<Json<TraceSearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(TraceSearchResponse {
        hits: gather_trace_hits_cluster(state.as_ref(), &query).await?,
    }))
}

async fn search_logs_cluster(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<LogSearchQuery>,
) -> Result<Json<LogSearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(LogSearchResponse {
        rows: gather_log_rows_cluster(state.as_ref(), &query)
            .await?
            .into_iter()
            .map(log_row_response_from_log_row)
            .collect(),
    }))
}

async fn get_trace_tempo_local(
    State(state): State<Arc<StorageState>>,
    Path(trace_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let rows = state.query.get_trace(&trace_id);
    render_tempo_trace_response(&trace_id, rows, &headers)
}

async fn get_trace_tempo_cluster(
    State(state): State<Arc<SelectState>>,
    Path(trace_id): Path<String>,
    headers: HeaderMap,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    let rows = fetch_trace_rows_cluster(state.as_ref(), &trace_id).await?;
    render_tempo_trace_response(&trace_id, rows, &headers)
}

async fn search_traces_tempo_local(
    State(state): State<Arc<StorageState>>,
    Query(query): Query<TempoSearchQuery>,
) -> Result<Json<TempoSearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let traces = search_tempo_local(&state.query, &query)?;
    Ok(Json(build_tempo_search_response(
        traces,
        state.storage.stats().segment_count,
    )))
}

async fn search_traces_tempo_cluster(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<TempoSearchQuery>,
) -> Result<Json<TempoSearchResponse>, (StatusCode, Json<ErrorResponse>)> {
    let traces = search_tempo_cluster(state.as_ref(), &query).await?;
    Ok(Json(build_tempo_search_response(
        traces,
        state.cluster.storage_nodes().len() as u64,
    )))
}

async fn list_tempo_tags_local(
    State(state): State<Arc<StorageState>>,
    Query(query): Query<TempoTagQuery>,
) -> Json<TempoTagNamesResponse> {
    Json(TempoTagNamesResponse {
        tag_names: list_tempo_tags(&state.query, query.scope.as_deref(), query.limit),
        metrics: TempoQueryMetrics::empty(),
    })
}

async fn list_tempo_tags_cluster(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<TempoTagQuery>,
) -> Result<Json<TempoTagNamesResponse>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(TempoTagNamesResponse {
        tag_names: list_tempo_tags_cluster_all(state.as_ref(), query.scope.as_deref(), query.limit)
            .await?,
        metrics: TempoQueryMetrics::empty(),
    }))
}

async fn list_tempo_tags_v2_local(
    State(state): State<Arc<StorageState>>,
    Query(query): Query<TempoTagQuery>,
) -> Json<TempoTagNamesV2Response> {
    Json(TempoTagNamesV2Response {
        scopes: build_tempo_tag_scopes(&state.query, query.limit),
        metrics: TempoQueryMetrics::empty(),
    })
}

async fn list_tempo_tags_v2_cluster(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<TempoTagQuery>,
) -> Result<Json<TempoTagNamesV2Response>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(TempoTagNamesV2Response {
        scopes: build_tempo_tag_scopes_from_names(
            &gather_field_names_cluster(state.as_ref()).await?,
            query.limit,
        ),
        metrics: TempoQueryMetrics::empty(),
    }))
}

async fn list_tempo_tag_values_local(
    State(state): State<Arc<StorageState>>,
    Path(tag_name): Path<String>,
    Query(query): Query<TempoTagQuery>,
) -> Json<TempoTagValuesResponse> {
    Json(TempoTagValuesResponse {
        tag_values: list_tempo_tag_values(&state.query, &tag_name, query.limit),
        metrics: TempoQueryMetrics::empty(),
    })
}

async fn list_tempo_tag_values_cluster(
    State(state): State<Arc<SelectState>>,
    Path(tag_name): Path<String>,
    Query(query): Query<TempoTagQuery>,
) -> Result<Json<TempoTagValuesResponse>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(TempoTagValuesResponse {
        tag_values: gather_tempo_tag_values_cluster(state.as_ref(), &tag_name, query.limit).await?,
        metrics: TempoQueryMetrics::empty(),
    }))
}

async fn list_tempo_tag_values_v2_local(
    State(state): State<Arc<StorageState>>,
    Path(tag_name): Path<String>,
    Query(query): Query<TempoTagQuery>,
) -> Json<TempoTagValuesV2Response> {
    Json(TempoTagValuesV2Response {
        tag_values: list_tempo_tag_values(&state.query, &tag_name, query.limit)
            .into_iter()
            .map(|value| TempoTypedTagValue {
                value_type: "string",
                value,
            })
            .collect(),
        metrics: TempoQueryMetrics::empty(),
    })
}

async fn list_tempo_tag_values_v2_cluster(
    State(state): State<Arc<SelectState>>,
    Path(tag_name): Path<String>,
    Query(query): Query<TempoTagQuery>,
) -> Result<Json<TempoTagValuesV2Response>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(TempoTagValuesV2Response {
        tag_values: gather_tempo_tag_values_cluster(state.as_ref(), &tag_name, query.limit)
            .await?
            .into_iter()
            .map(|value| TempoTypedTagValue {
                value_type: "string",
                value,
            })
            .collect(),
        metrics: TempoQueryMetrics::empty(),
    }))
}

async fn list_cluster_members(
    State(state): State<Arc<SelectState>>,
) -> Json<ClusterMembersResponse> {
    refresh_cluster_membership(state.as_ref()).await;
    Json(ClusterMembersResponse {
        members: state.health.snapshot(&state.cluster),
    })
}

async fn get_cluster_leader(State(state): State<Arc<SelectState>>) -> Json<ClusterLeaderResponse> {
    let leader = refresh_control_plane(state.as_ref()).await;
    let view = state.control.snapshot();
    let local = state
        .cluster
        .local_control_node()
        .map(|node| leader.as_deref() == Some(node))
        .unwrap_or(false);
    Json(ClusterLeaderResponse {
        leader,
        local,
        epoch: view.epoch,
        peers: state
            .control
            .health
            .snapshot_nodes(state.cluster.control_nodes()),
    })
}

async fn get_cluster_state(
    State(state): State<Arc<SelectState>>,
) -> Json<ClusterControlStateResponse> {
    Json(snapshot_control_plane_state(state.as_ref()))
}

async fn get_cluster_journal(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<ControlJournalQuery>,
) -> Json<ControlJournalResponse> {
    Json(state.control.journal_after(query.after.unwrap_or(0)))
}

async fn append_cluster_journal(
    State(state): State<Arc<SelectState>>,
    Json(request): Json<ControlJournalAppendRequest>,
) -> Json<ControlJournalResponse> {
    state.control.merge_remote_journal_entries(request.entries);
    Json(state.control.journal_after(0))
}

async fn rebalance_cluster(
    State(state): State<Arc<SelectState>>,
) -> Result<Json<RebalanceResponse>, (StatusCode, Json<ErrorResponse>)> {
    let leader = refresh_control_plane(state.as_ref()).await;
    if let Some(local_control_node) = state.cluster.local_control_node() {
        if let Some(leader) = leader.as_deref() {
            if leader != local_control_node {
                return Err((
                    StatusCode::CONFLICT,
                    Json(ErrorResponse {
                        error: format!("rebalance is leader-gated; current leader is {leader}"),
                    }),
                ));
            }
        }
    }

    let ownership = gather_trace_ownership(state.as_ref()).await?;
    let scanned_traces = ownership.len();
    let mut repaired_replicas = 0usize;

    for (trace_id, owners) in ownership {
        let desired_nodes = state.cluster.placements(&trace_id);
        let missing_nodes: Vec<String> = desired_nodes
            .into_iter()
            .filter(|node| !owners.iter().any(|owner| owner == node))
            .map(ToString::to_string)
            .collect();

        if missing_nodes.is_empty() {
            continue;
        }

        let rows = fetch_trace_rows_from_nodes(state.as_ref(), &trace_id, &owners).await?;
        if rows.is_empty() {
            continue;
        }

        repaired_replicas += repair_trace_on_nodes(state.as_ref(), &missing_nodes, &rows).await;
    }

    if state
        .cluster
        .local_control_node()
        .map(|node| Some(node) == leader.as_deref())
        .unwrap_or(false)
    {
        let mut details = BTreeMap::new();
        details.insert("scanned_traces".to_string(), scanned_traces.to_string());
        details.insert(
            "repaired_replicas".to_string(),
            repaired_replicas.to_string(),
        );
        let entry = state.control.append_local_journal_entry(
            "rebalance_completed",
            state.cluster.local_control_node().map(ToString::to_string),
            details,
        );
        replicate_control_journal_entries(state.as_ref(), vec![entry]).await;
    }

    Ok(Json(RebalanceResponse {
        scanned_traces,
        repaired_replicas,
    }))
}

async fn refresh_cluster_membership(state: &SelectState) {
    for node in state.cluster.storage_nodes() {
        state.metrics.record_membership_probe_request();
        match state
            .http_client
            .get(format!("{node}/healthz"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                state.health.mark_success(node);
            }
            Ok(response) => {
                state.metrics.record_membership_probe_error();
                let error = format!("healthz returned {}", response.status());
                if state.health.mark_failure(node, error.clone()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "cluster membership probe failed");
            }
            Err(error) => {
                state.metrics.record_membership_probe_error();
                if state.health.mark_failure(node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "cluster membership probe failed");
            }
        }
    }
}

async fn refresh_control_plane(state: &SelectState) -> Option<String> {
    let Some(local_control_node) = state.cluster.local_control_node() else {
        return None;
    };

    let mut healthy_nodes = Vec::new();
    for node in state.cluster.control_nodes() {
        if node == local_control_node {
            state.control.health.mark_success(node);
            healthy_nodes.push(node.clone());
            continue;
        }

        state.metrics.record_control_probe_request();
        match state
            .http_client
            .get(format!("{node}/healthz"))
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                state.control.health.mark_success(node);
                healthy_nodes.push(node.clone());
            }
            Ok(response) => {
                state.metrics.record_control_probe_error();
                let error = format!("healthz returned {}", response.status());
                if state.control.health.mark_failure(node, error.clone()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "control-plane probe failed");
            }
            Err(error) => {
                state.metrics.record_control_probe_error();
                if state.control.health.mark_failure(node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "control-plane probe failed");
            }
        }
    }

    if healthy_nodes.is_empty() {
        healthy_nodes = state
            .control
            .health
            .healthy_nodes(state.cluster.control_nodes().iter().map(String::as_str));
    }

    let leader = state
        .cluster
        .elect_control_leader(healthy_nodes.iter().map(String::as_str));
    if state.control.set_leader(leader.clone()) {
        state.metrics.record_control_leader_change();
        if leader.as_deref() == Some(local_control_node) {
            let entry = state.control.append_local_journal_entry(
                "leader_elected",
                Some(local_control_node.to_string()),
                BTreeMap::new(),
            );
            replicate_control_journal_entries(state, vec![entry]).await;
        }
    }
    refresh_control_state_from_peers(state).await;
    let view = state.control.snapshot();
    view.leader
}

fn snapshot_control_plane_state(state: &SelectState) -> ClusterControlStateResponse {
    let view = state.control.snapshot();
    let leader = view.leader.clone();
    let local = state
        .cluster
        .local_control_node()
        .map(|node| leader.as_deref() == Some(node))
        .unwrap_or(false);
    ClusterControlStateResponse {
        node: state.cluster.local_control_node().map(ToString::to_string),
        leader,
        local,
        epoch: view.epoch,
        journal_head_index: state.control.journal_head_index(),
        members: state.health.snapshot(&state.cluster),
        peers: state
            .control
            .health
            .snapshot_nodes(state.cluster.control_nodes()),
    }
}

async fn refresh_control_state_from_peers(state: &SelectState) {
    let Some(local_control_node) = state.cluster.local_control_node() else {
        return;
    };
    let admin_bearer_token = state.auth.admin_or_internal_bearer_token();
    for node in state.cluster.control_nodes() {
        if node == local_control_node {
            continue;
        }
        state.metrics.record_control_state_sync_request();
        let response = match apply_bearer_auth(
            state
                .http_client
                .get(format!("{node}/admin/v1/cluster/state")),
            admin_bearer_token.as_deref(),
        )
        .send()
        .await
        {
            Ok(response) => response,
            Err(error) => {
                state.metrics.record_control_state_sync_error();
                warn!(node = %node, error = %error, "control-state sync failed");
                continue;
            }
        };

        if !response.status().is_success() {
            state.metrics.record_control_state_sync_error();
            warn!(
                node = %node,
                status = %response.status(),
                "control-state sync returned non-success status"
            );
            continue;
        }

        match response.json::<ClusterControlStateResponse>().await {
            Ok(snapshot) => {
                for member in snapshot.members {
                    state.health.merge_observation(
                        &member.node,
                        member.observed,
                        member.healthy,
                        member.quarantined,
                        member.consecutive_failures,
                        member.last_checked_unix_millis,
                        member.last_error,
                    );
                }
                for peer in snapshot.peers {
                    state.control.health.merge_observation(
                        &peer.node,
                        peer.observed,
                        peer.healthy,
                        peer.quarantined,
                        peer.consecutive_failures,
                        peer.last_checked_unix_millis,
                        peer.last_error,
                    );
                }
                if state
                    .control
                    .absorb_remote_state(snapshot.epoch, snapshot.leader)
                {
                    state.metrics.record_control_leader_change();
                }
                if snapshot.journal_head_index > state.control.journal_head_index() {
                    sync_control_journal_from_peer(state, node, state.control.journal_head_index())
                        .await;
                }
            }
            Err(error) => {
                state.metrics.record_control_state_sync_error();
                warn!(node = %node, error = %error, "failed to decode control-state snapshot");
            }
        }
    }
}

async fn sync_control_journal_from_peer(state: &SelectState, node: &str, after: u64) {
    let admin_bearer_token = state.auth.admin_or_internal_bearer_token();
    state.metrics.record_control_state_sync_request();
    let response = match apply_bearer_auth(
        state
            .http_client
            .get(format!("{node}/admin/v1/cluster/journal"))
            .query(&[("after", after)]),
        admin_bearer_token.as_deref(),
    )
    .send()
    .await
    {
        Ok(response) => response,
        Err(error) => {
            state.metrics.record_control_state_sync_error();
            warn!(node = %node, error = %error, "control-journal sync failed");
            return;
        }
    };

    if !response.status().is_success() {
        state.metrics.record_control_state_sync_error();
        warn!(
            node = %node,
            status = %response.status(),
            "control-journal sync returned non-success status"
        );
        return;
    }

    match response.json::<ControlJournalResponse>().await {
        Ok(journal) => {
            state.control.merge_remote_journal_entries(journal.entries);
        }
        Err(error) => {
            state.metrics.record_control_state_sync_error();
            warn!(node = %node, error = %error, "failed to decode control-journal response");
        }
    }
}

async fn replicate_control_journal_entries(state: &SelectState, entries: Vec<ControlJournalEntry>) {
    let Some(local_control_node) = state.cluster.local_control_node() else {
        return;
    };
    if entries.is_empty() {
        return;
    }

    let admin_bearer_token = state.auth.admin_or_internal_bearer_token();
    let request = ControlJournalAppendRequest { entries };
    for node in state.cluster.control_nodes() {
        if node == local_control_node {
            continue;
        }

        state.metrics.record_control_state_sync_request();
        let response = match apply_bearer_auth(
            state
                .http_client
                .post(format!("{node}/admin/v1/cluster/journal/append")),
            admin_bearer_token.as_deref(),
        )
        .json(&request)
        .send()
        .await
        {
            Ok(response) => response,
            Err(error) => {
                state.metrics.record_control_state_sync_error();
                warn!(
                    node = %node,
                    error = %error,
                    "control-journal replication failed"
                );
                continue;
            }
        };

        if !response.status().is_success() {
            state.metrics.record_control_state_sync_error();
            warn!(
                node = %node,
                status = %response.status(),
                "control-journal replication returned non-success status"
            );
        }
    }
}

async fn list_services_jaeger_local(
    State(state): State<Arc<StorageState>>,
) -> Json<JaegerResponse<Vec<String>>> {
    Json(jaeger_success_response(state.query.list_services()))
}

async fn list_services_jaeger_cluster(
    State(state): State<Arc<SelectState>>,
) -> Result<Json<JaegerResponse<Vec<String>>>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(jaeger_success_response(
        gather_services_cluster(state.as_ref()).await?,
    )))
}

async fn list_operations_jaeger_local(
    State(state): State<Arc<StorageState>>,
    Path(service_name): Path<String>,
) -> Json<JaegerResponse<Vec<String>>> {
    Json(jaeger_success_response(list_operations_for_query(
        &state.query,
        &service_name,
    )))
}

async fn list_operations_jaeger_cluster(
    State(state): State<Arc<SelectState>>,
    Path(service_name): Path<String>,
) -> Result<Json<JaegerResponse<Vec<String>>>, (StatusCode, Json<ErrorResponse>)> {
    Ok(Json(jaeger_success_response(
        list_operations_for_cluster(state.as_ref(), &service_name).await?,
    )))
}

async fn get_trace_jaeger_local(
    State(state): State<Arc<StorageState>>,
    Path(trace_id): Path<String>,
) -> (StatusCode, Json<JaegerResponse<Vec<JaegerTrace>>>) {
    let rows = state.query.get_trace(&trace_id);
    if let Some(trace) = build_jaeger_trace(rows) {
        (StatusCode::OK, Json(jaeger_success_response(vec![trace])))
    } else {
        (
            StatusCode::NOT_FOUND,
            Json(jaeger_not_found_trace_response()),
        )
    }
}

async fn get_trace_jaeger_cluster(
    State(state): State<Arc<SelectState>>,
    Path(trace_id): Path<String>,
) -> Result<(StatusCode, Json<JaegerResponse<Vec<JaegerTrace>>>), (StatusCode, Json<ErrorResponse>)>
{
    let rows = fetch_trace_rows_cluster(state.as_ref(), &trace_id).await?;
    if let Some(trace) = build_jaeger_trace(rows) {
        Ok((StatusCode::OK, Json(jaeger_success_response(vec![trace]))))
    } else {
        Ok((
            StatusCode::NOT_FOUND,
            Json(jaeger_not_found_trace_response()),
        ))
    }
}

async fn search_traces_jaeger_local(
    State(state): State<Arc<StorageState>>,
    Query(query): Query<JaegerTraceQuery>,
) -> Result<Json<JaegerResponse<Vec<JaegerTrace>>>, (StatusCode, Json<ErrorResponse>)> {
    let traces = search_jaeger_traces_local(&state.query, &query)?;
    Ok(Json(jaeger_success_response(traces)))
}

async fn search_traces_jaeger_cluster(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<JaegerTraceQuery>,
) -> Result<Json<JaegerResponse<Vec<JaegerTrace>>>, (StatusCode, Json<ErrorResponse>)> {
    let traces = search_jaeger_traces_cluster(state.as_ref(), &query).await?;
    Ok(Json(jaeger_success_response(traces)))
}

async fn get_jaeger_dependencies_local(
    State(state): State<Arc<StorageState>>,
    Query(query): Query<JaegerDependenciesQuery>,
) -> Result<Json<Vec<JaegerDependencyLink>>, (StatusCode, Json<ErrorResponse>)> {
    let request = query.trace_search_request();
    let traces = state
        .query
        .search_traces(&request)
        .into_iter()
        .map(|hit| state.query.get_trace(&hit.trace_id));
    Ok(Json(build_jaeger_dependencies(traces)))
}

async fn get_jaeger_dependencies_cluster(
    State(state): State<Arc<SelectState>>,
    Query(query): Query<JaegerDependenciesQuery>,
) -> Result<Json<Vec<JaegerDependencyLink>>, (StatusCode, Json<ErrorResponse>)> {
    let hits = gather_trace_hits_cluster(state.as_ref(), &query.search_query()).await?;
    let mut traces = Vec::with_capacity(hits.len());
    for hit in hits {
        traces.push(fetch_trace_rows_cluster(state.as_ref(), &hit.trace_id).await?);
    }
    Ok(Json(build_jaeger_dependencies(traces)))
}

async fn storage_metrics(
    State(state): State<Arc<StorageState>>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render_storage_metrics(&state.storage.stats()),
    )
}

async fn insert_metrics(
    State(state): State<Arc<InsertState>>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render_cluster_metrics(
            &state.metrics.snapshot(),
            &state.cluster,
            state.health.unhealthy_nodes(),
        ),
    )
}

async fn select_metrics(
    State(state): State<Arc<SelectState>>,
) -> ([(header::HeaderName, &'static str); 1], String) {
    let mut metrics = state.metrics.snapshot();
    metrics.control_epoch = state.control.snapshot().epoch;
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        render_cluster_metrics(&metrics, &state.cluster, state.health.unhealthy_nodes()),
    )
}

fn build_trace_response(query: &QueryService, trace_id: String) -> TraceResponse {
    build_trace_response_from_rows(trace_id.clone(), query.get_trace(&trace_id))
}

fn build_trace_response_from_rows(trace_id: String, rows: Vec<TraceSpanRow>) -> TraceResponse {
    TraceResponse {
        trace_id,
        rows: rows.into_iter().map(TraceRowResponse::from).collect(),
    }
}

fn log_row_response_from_log_row(row: LogRow) -> LogRowResponse {
    LogRowResponse {
        log_id: row.log_id,
        time_unix_nano: row.time_unix_nano,
        observed_time_unix_nano: row.observed_time_unix_nano,
        severity_number: row.severity_number,
        severity_text: row.severity_text,
        body: row.body,
        trace_id: row.trace_id,
        span_id: row.span_id,
        fields: row
            .fields
            .into_iter()
            .map(|field| (field.name.to_string(), field.value.to_string()))
            .collect(),
    }
}

fn log_row_from_response(row: LogRowResponse) -> LogRow {
    LogRow::new_unsorted_fields(
        row.log_id,
        row.time_unix_nano,
        row.observed_time_unix_nano,
        row.severity_number,
        row.severity_text,
        row.body,
        row.trace_id,
        row.span_id,
        row.fields
            .into_iter()
            .map(|(name, value)| vtcore::Field::new(name, value))
            .collect(),
    )
}

fn merge_trace_hit(hits_by_trace: &mut HashMap<String, TraceSearchHit>, incoming: TraceSearchHit) {
    hits_by_trace
        .entry(incoming.trace_id.clone())
        .and_modify(|existing| {
            existing.start_unix_nano = existing.start_unix_nano.min(incoming.start_unix_nano);
            existing.end_unix_nano = existing.end_unix_nano.max(incoming.end_unix_nano);

            let mut services: BTreeSet<String> = existing.services.iter().cloned().collect();
            services.extend(incoming.services.iter().cloned());
            existing.services = services.into_iter().collect();
        })
        .or_insert(incoming);
}

async fn fetch_trace_rows_cluster(
    state: &SelectState,
    trace_id: &str,
) -> Result<Vec<TraceSpanRow>, (StatusCode, Json<ErrorResponse>)> {
    let mut had_error = false;
    let mut successful_reads = 0usize;
    let mut repair_targets = Vec::new();
    let mut chosen_rows: Option<Vec<TraceSpanRow>> = None;
    let selected_nodes = state.health.select_nodes(
        state.cluster.placements(trace_id),
        state.cluster.read_quorum(),
    );
    state.metrics.record_read_skips(selected_nodes.skipped);
    let mut read_tasks = JoinSet::new();
    let internal_bearer_token = state.auth.internal_bearer_token().map(ToString::to_string);

    for node in selected_nodes.nodes {
        state.metrics.record_read_request();
        let client = state.http_client.clone();
        let trace_id = trace_id.to_string();
        let bearer_token = internal_bearer_token.clone();
        read_tasks.spawn(async move {
            let response = apply_bearer_auth(
                client.get(format!("{node}/internal/v1/traces/{trace_id}")),
                bearer_token.as_deref(),
            )
            .send()
            .await;

            let response = match response {
                Ok(response) => response,
                Err(error) => {
                    return RemoteTraceReadOutcome::Failure {
                        node,
                        error: error.to_string(),
                    };
                }
            };

            if !response.status().is_success() {
                return RemoteTraceReadOutcome::Failure {
                    node,
                    error: response.status().to_string(),
                };
            }

            match response.json::<TraceResponse>().await {
                Ok(trace_response) if !trace_response.rows.is_empty() => {
                    RemoteTraceReadOutcome::Rows {
                        node,
                        rows: trace_response
                            .rows
                            .into_iter()
                            .map(TraceSpanRow::from)
                            .collect(),
                    }
                }
                Ok(_) => RemoteTraceReadOutcome::Empty { node },
                Err(error) => RemoteTraceReadOutcome::Failure {
                    node,
                    error: error.to_string(),
                },
            }
        });
    }

    while let Some(result) = read_tasks.join_next().await {
        match result {
            Ok(RemoteTraceReadOutcome::Rows { node, rows }) => {
                state.health.mark_success(&node);
                successful_reads += 1;
                if let Some(existing) = &chosen_rows {
                    if !canonical_trace_rows_equal(existing, &rows) {
                        read_tasks.abort_all();
                        return Err(bad_gateway(format!(
                            "read quorum mismatch for trace {trace_id}"
                        )));
                    }
                } else {
                    chosen_rows = Some(rows);
                }
                if successful_reads >= state.cluster.read_quorum() {
                    read_tasks.abort_all();
                    break;
                }
            }
            Ok(RemoteTraceReadOutcome::Empty { node }) => {
                state.health.mark_success(&node);
                repair_targets.push(node);
            }
            Ok(RemoteTraceReadOutcome::Failure { node, error }) => {
                had_error = true;
                state.metrics.record_read_error();
                if state.health.mark_failure(&node, error.clone()) {
                    state.metrics.record_node_quarantine();
                }
                repair_targets.push(node.clone());
                warn!(node = %node, error = %error, "remote trace query failed");
            }
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                warn!(error = %error, "trace read task failed");
            }
        }
    }

    if chosen_rows.is_some() && successful_reads < state.cluster.read_quorum() {
        Err(bad_gateway(format!(
            "read quorum not met for trace {trace_id}: successes={}, required={}",
            successful_reads,
            state.cluster.read_quorum()
        )))
    } else if successful_reads < state.cluster.read_quorum() && had_error {
        Err(bad_gateway(format!(
            "read quorum not met for trace {trace_id}: successes={}, required={}",
            successful_reads,
            state.cluster.read_quorum()
        )))
    } else if let Some(rows) = chosen_rows {
        let _ = repair_trace_on_nodes(state, &repair_targets, &rows).await;
        Ok(rows)
    } else if successful_reads >= state.cluster.read_quorum() {
        Ok(Vec::new())
    } else if had_error {
        Err(bad_gateway(format!(
            "all replicas failed while querying trace {trace_id}"
        )))
    } else {
        Ok(Vec::new())
    }
}

async fn fetch_trace_rows_from_nodes(
    state: &SelectState,
    trace_id: &str,
    nodes: &[String],
) -> Result<Vec<TraceSpanRow>, (StatusCode, Json<ErrorResponse>)> {
    for node in nodes {
        state.metrics.record_read_request();
        let response = match state
            .http_client
            .get(format!("{node}/internal/v1/traces/{trace_id}"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                state.metrics.record_read_error();
                if state.health.mark_failure(node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "rebalance trace fetch failed");
                continue;
            }
        };

        if !response.status().is_success() {
            state.metrics.record_read_error();
            if state
                .health
                .mark_failure(node, response.status().to_string())
            {
                state.metrics.record_node_quarantine();
            }
            warn!(
                node = %node,
                status = %response.status(),
                "rebalance trace fetch returned non-success status"
            );
            continue;
        }

        state.health.mark_success(node);
        match response.json::<TraceResponse>().await {
            Ok(trace_response) if !trace_response.rows.is_empty() => {
                return Ok(trace_response
                    .rows
                    .into_iter()
                    .map(TraceSpanRow::from)
                    .collect());
            }
            Ok(_) => {}
            Err(error) => {
                state.metrics.record_read_error();
                warn!(
                    node = %node,
                    error = %error,
                    "failed to decode rebalance trace response"
                );
            }
        }
    }

    Err(bad_gateway(format!(
        "unable to fetch trace rows for rebalance: {trace_id}"
    )))
}

async fn repair_trace_on_nodes(
    state: &SelectState,
    nodes: &[String],
    rows: &[TraceSpanRow],
) -> usize {
    if nodes.is_empty() || rows.is_empty() {
        return 0;
    }

    let payload = match encode_internal_rows_payload(rows) {
        Ok(payload) => payload,
        Err(error) => {
            warn!(error = %error, "failed to encode rows for read repair");
            state.metrics.record_write_error();
            state.metrics.record_read_repair_error();
            return 0;
        }
    };
    let mut repaired = 0usize;
    for node in nodes {
        state.metrics.record_write_batch(rows.len());
        match state
            .http_client
            .post(format!("{node}/internal/v1/rows"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .header("content-type", INTERNAL_TRACE_ROWS_CONTENT_TYPE)
            .body(payload.clone())
            .send()
            .await
        {
            Ok(response) if response.status().is_success() => {
                state.health.mark_success(node);
                state.metrics.record_read_repair();
                repaired += 1;
            }
            Ok(response) => {
                state.metrics.record_write_error();
                state.metrics.record_read_repair_error();
                if state
                    .health
                    .mark_failure(node, response.status().to_string())
                {
                    state.metrics.record_node_quarantine();
                }
                warn!(
                    node = %node,
                    status = %response.status(),
                    "read repair returned non-success status"
                );
            }
            Err(error) => {
                state.metrics.record_write_error();
                state.metrics.record_read_repair_error();
                if state.health.mark_failure(node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "read repair failed");
            }
        }
    }
    repaired
}

fn canonical_trace_rows_equal(left: &[TraceSpanRow], right: &[TraceSpanRow]) -> bool {
    let mut left = left.to_vec();
    let mut right = right.to_vec();
    left.sort_by(|a, b| a.span_id.cmp(&b.span_id));
    right.sort_by(|a, b| a.span_id.cmp(&b.span_id));
    left == right
}

async fn gather_trace_ownership(
    state: &SelectState,
) -> Result<HashMap<String, Vec<String>>, (StatusCode, Json<ErrorResponse>)> {
    let mut ownership: HashMap<String, Vec<String>> = HashMap::new();
    let selected_nodes = state
        .health
        .select_nodes(state.cluster.storage_nodes().iter().map(String::as_str), 1);
    state.metrics.record_read_skips(selected_nodes.skipped);
    let mut successful_nodes = 0usize;
    let mut had_error = false;

    for node in selected_nodes.nodes {
        state.metrics.record_read_request();
        let response = match state
            .http_client
            .get(format!("{node}/internal/v1/traces/index"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                if state.health.mark_failure(&node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "trace ownership query failed");
                continue;
            }
        };

        if !response.status().is_success() {
            had_error = true;
            state.metrics.record_read_error();
            if state
                .health
                .mark_failure(&node, response.status().to_string())
            {
                state.metrics.record_node_quarantine();
            }
            warn!(
                node = %node,
                status = %response.status(),
                "trace ownership query returned non-success status"
            );
            continue;
        }

        match response.json::<TraceIndexResponse>().await {
            Ok(trace_index) => {
                state.health.mark_success(&node);
                successful_nodes += 1;
                for trace_id in trace_index.trace_ids {
                    ownership.entry(trace_id).or_default().push(node.clone());
                }
            }
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                warn!(node = %node, error = %error, "failed to decode trace ownership response");
            }
        }
    }

    if successful_nodes == 0 && had_error {
        Err(bad_gateway(
            "all storage nodes failed while fetching trace ownership",
        ))
    } else {
        Ok(ownership)
    }
}

async fn gather_services_cluster(
    state: &SelectState,
) -> Result<Vec<String>, (StatusCode, Json<ErrorResponse>)> {
    let mut services = BTreeSet::new();
    let mut successful_nodes = 0usize;
    let mut had_error = false;
    let selected_nodes = state
        .health
        .select_nodes(state.cluster.storage_nodes().iter().map(String::as_str), 1);
    state.metrics.record_read_skips(selected_nodes.skipped);

    for node in selected_nodes.nodes {
        state.metrics.record_read_request();
        let response = match state
            .http_client
            .get(format!("{node}/internal/v1/services"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                if state.health.mark_failure(&node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "remote services query failed");
                continue;
            }
        };

        if !response.status().is_success() {
            had_error = true;
            state.metrics.record_read_error();
            if state
                .health
                .mark_failure(&node, response.status().to_string())
            {
                state.metrics.record_node_quarantine();
            }
            warn!(
                node = %node,
                status = %response.status(),
                "remote services query returned non-success status"
            );
            continue;
        }

        match response.json::<ServicesResponse>().await {
            Ok(remote_services) => {
                state.health.mark_success(&node);
                successful_nodes += 1;
                services.extend(remote_services.services);
            }
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                warn!(node = %node, error = %error, "failed to decode services response");
            }
        }
    }

    if successful_nodes == 0 && had_error {
        Err(bad_gateway(
            "all storage nodes failed while listing services",
        ))
    } else {
        Ok(services.into_iter().collect())
    }
}

async fn gather_trace_hits_cluster(
    state: &SelectState,
    query: &SearchQuery,
) -> Result<Vec<TraceSearchHit>, (StatusCode, Json<ErrorResponse>)> {
    let mut hits_by_trace: HashMap<String, TraceSearchHit> = HashMap::new();
    let mut successful_nodes = 0usize;
    let mut had_error = false;
    let selected_nodes = state
        .health
        .select_nodes(state.cluster.storage_nodes().iter().map(String::as_str), 1);
    state.metrics.record_read_skips(selected_nodes.skipped);

    for node in selected_nodes.nodes {
        state.metrics.record_read_request();
        let mut query_params = vec![
            (
                "start_unix_nano".to_string(),
                query.start_unix_nano.to_string(),
            ),
            ("end_unix_nano".to_string(), query.end_unix_nano.to_string()),
            ("limit".to_string(), query.limit.unwrap_or(20).to_string()),
        ];
        if let Some(service_name) = &query.service_name {
            query_params.push(("service_name".to_string(), service_name.clone()));
        }
        if let Some(operation_name) = &query.operation_name {
            query_params.push(("operation_name".to_string(), operation_name.clone()));
        }
        for field_filter in &query.field_filter {
            query_params.push(("field_filter".to_string(), field_filter.clone()));
        }

        let response = match state
            .http_client
            .get(format!("{node}/internal/v1/traces/search"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .query(&query_params)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                if state.health.mark_failure(&node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "remote search query failed");
                continue;
            }
        };

        if !response.status().is_success() {
            had_error = true;
            state.metrics.record_read_error();
            if state
                .health
                .mark_failure(&node, response.status().to_string())
            {
                state.metrics.record_node_quarantine();
            }
            warn!(
                node = %node,
                status = %response.status(),
                "remote search query returned non-success status"
            );
            continue;
        }

        match response.json::<TraceSearchResponse>().await {
            Ok(remote_hits) => {
                state.health.mark_success(&node);
                successful_nodes += 1;
                for hit in remote_hits.hits {
                    merge_trace_hit(&mut hits_by_trace, hit);
                }
            }
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                warn!(node = %node, error = %error, "failed to decode search response");
            }
        }
    }

    if successful_nodes == 0 && had_error {
        return Err(bad_gateway(
            "all storage nodes failed while searching traces",
        ));
    }

    let mut hits: Vec<TraceSearchHit> = hits_by_trace.into_values().collect();
    hits.sort_by(|left, right| {
        right
            .end_unix_nano
            .cmp(&left.end_unix_nano)
            .then_with(|| left.trace_id.cmp(&right.trace_id))
    });
    hits.truncate(query.limit.unwrap_or(20));

    Ok(hits)
}

async fn gather_log_rows_cluster(
    state: &SelectState,
    query: &LogSearchQuery,
) -> Result<Vec<LogRow>, (StatusCode, Json<ErrorResponse>)> {
    let mut rows_by_log_id: HashMap<String, LogRow> = HashMap::new();
    let mut successful_nodes = 0usize;
    let mut had_error = false;
    let selected_nodes = state
        .health
        .select_nodes(state.cluster.storage_nodes().iter().map(String::as_str), 1);
    state.metrics.record_read_skips(selected_nodes.skipped);

    for node in selected_nodes.nodes {
        state.metrics.record_read_request();
        let mut query_params = vec![
            (
                "start_unix_nano".to_string(),
                query.start_unix_nano.to_string(),
            ),
            ("end_unix_nano".to_string(), query.end_unix_nano.to_string()),
            ("limit".to_string(), query.limit.unwrap_or(20).to_string()),
        ];
        if let Some(service_name) = &query.service_name {
            query_params.push(("service_name".to_string(), service_name.clone()));
        }
        if let Some(severity_text) = &query.severity_text {
            query_params.push(("severity_text".to_string(), severity_text.clone()));
        }
        for field_filter in &query.field_filter {
            query_params.push(("field_filter".to_string(), field_filter.clone()));
        }

        let response = match state
            .http_client
            .get(format!("{node}/internal/v1/logs/search"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .query(&query_params)
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                if state.health.mark_failure(&node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                warn!(node = %node, error = %error, "remote log search failed");
                continue;
            }
        };

        if !response.status().is_success() {
            had_error = true;
            state.metrics.record_read_error();
            if state
                .health
                .mark_failure(&node, response.status().to_string())
            {
                state.metrics.record_node_quarantine();
            }
            warn!(
                node = %node,
                status = %response.status(),
                "remote log search returned non-success status"
            );
            continue;
        }

        match response.json::<LogSearchResponse>().await {
            Ok(remote_rows) => {
                state.health.mark_success(&node);
                successful_nodes += 1;
                for row in remote_rows.rows.into_iter().map(log_row_from_response) {
                    rows_by_log_id.entry(row.log_id.clone()).or_insert(row);
                }
            }
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                warn!(node = %node, error = %error, "failed to decode log search response");
            }
        }
    }

    if successful_nodes == 0 && had_error {
        return Err(bad_gateway("all storage nodes failed while searching logs"));
    }

    let mut rows: Vec<LogRow> = rows_by_log_id.into_values().collect();
    rows.sort_by(|left, right| {
        right
            .time_unix_nano
            .cmp(&left.time_unix_nano)
            .then_with(|| left.log_id.cmp(&right.log_id))
    });
    rows.truncate(query.limit.unwrap_or(20));
    Ok(rows)
}

fn list_operations_for_query(query: &QueryService, service_name: &str) -> Vec<String> {
    let hits = query.search_traces(&TraceSearchRequest {
        start_unix_nano: i64::MIN,
        end_unix_nano: i64::MAX,
        service_name: Some(service_name.to_string()),
        operation_name: None,
        field_filters: Vec::new(),
        limit: usize::MAX,
    });

    let mut operations = BTreeSet::new();
    for hit in hits {
        for row in query.get_trace(&hit.trace_id) {
            if row.service_name() == Some(service_name) {
                operations.insert(row.name.clone());
            }
        }
    }

    operations.into_iter().collect()
}

async fn list_operations_for_cluster(
    state: &SelectState,
    service_name: &str,
) -> Result<Vec<String>, (StatusCode, Json<ErrorResponse>)> {
    let hits = gather_trace_hits_cluster(
        state,
        &SearchQuery {
            start_unix_nano: i64::MIN,
            end_unix_nano: i64::MAX,
            service_name: Some(service_name.to_string()),
            operation_name: None,
            field_filter: Vec::new(),
            limit: Some(usize::MAX),
        },
    )
    .await?;

    let mut operations = BTreeSet::new();
    for hit in hits {
        let rows = fetch_trace_rows_cluster(state, &hit.trace_id).await?;
        for row in rows {
            if row.service_name() == Some(service_name) {
                operations.insert(row.name);
            }
        }
    }

    Ok(operations.into_iter().collect())
}

fn search_jaeger_traces_local(
    query: &QueryService,
    jaeger_query: &JaegerTraceQuery,
) -> Result<Vec<JaegerTrace>, (StatusCode, Json<ErrorResponse>)> {
    let service_name = jaeger_query.required_service_name()?;
    let requested_limit = jaeger_query.checked_limit()?;
    let tag_filters = jaeger_query.parsed_tag_filters()?;
    let min_duration_nanos = jaeger_query.min_duration_nanos()?;
    let max_duration_nanos = jaeger_query.max_duration_nanos()?;
    let fetch_limit = jaeger_query.fetch_limit(
        requested_limit,
        &tag_filters,
        min_duration_nanos,
        max_duration_nanos,
    );
    let search_request =
        jaeger_query.trace_search_request(service_name.clone(), fetch_limit, &tag_filters)?;
    let hits = query.search_traces(&search_request);

    let mut traces = Vec::new();
    for hit in hits {
        let rows = query.get_trace(&hit.trace_id);
        if !trace_matches_jaeger_query(
            &rows,
            &service_name,
            jaeger_query.operation.as_deref(),
            &tag_filters,
            min_duration_nanos,
            max_duration_nanos,
        ) {
            continue;
        }
        if let Some(trace) = build_jaeger_trace(rows) {
            traces.push(trace);
            if traces.len() >= requested_limit {
                break;
            }
        }
    }

    Ok(traces)
}

async fn search_jaeger_traces_cluster(
    state: &SelectState,
    jaeger_query: &JaegerTraceQuery,
) -> Result<Vec<JaegerTrace>, (StatusCode, Json<ErrorResponse>)> {
    let service_name = jaeger_query.required_service_name()?;
    let requested_limit = jaeger_query.checked_limit()?;
    let tag_filters = jaeger_query.parsed_tag_filters()?;
    let min_duration_nanos = jaeger_query.min_duration_nanos()?;
    let max_duration_nanos = jaeger_query.max_duration_nanos()?;
    let fetch_limit = jaeger_query.fetch_limit(
        requested_limit,
        &tag_filters,
        min_duration_nanos,
        max_duration_nanos,
    );
    let search_query = jaeger_query.search_query(service_name.clone(), fetch_limit, &tag_filters)?;
    let hits = gather_trace_hits_cluster(state, &search_query).await?;

    let mut traces = Vec::new();
    for hit in hits {
        let rows = fetch_trace_rows_cluster(state, &hit.trace_id).await?;
        if !trace_matches_jaeger_query(
            &rows,
            &service_name,
            jaeger_query.operation.as_deref(),
            &tag_filters,
            min_duration_nanos,
            max_duration_nanos,
        ) {
            continue;
        }
        if let Some(trace) = build_jaeger_trace(rows) {
            traces.push(trace);
            if traces.len() >= requested_limit {
                break;
            }
        }
    }

    Ok(traces)
}

fn trace_matches_jaeger_query(
    rows: &[TraceSpanRow],
    service_name: &str,
    operation_name: Option<&str>,
    tag_filters: &[(String, String)],
    min_duration_nanos: Option<i64>,
    max_duration_nanos: Option<i64>,
) -> bool {
    rows.iter().any(|row| {
        row.service_name() == Some(service_name)
            && operation_name
                .map(|operation| row.name == operation)
                .unwrap_or(true)
            && row_matches_jaeger_tags(row, tag_filters)
            && min_duration_nanos
                .map(|minimum| row.duration_nanos() >= minimum)
                .unwrap_or(true)
            && max_duration_nanos
                .map(|maximum| row.duration_nanos() <= maximum)
                .unwrap_or(true)
    })
}

fn row_matches_jaeger_tags(row: &TraceSpanRow, tag_filters: &[(String, String)]) -> bool {
    tag_filters.iter().all(|(field_name, expected)| match field_name.as_str() {
        "name" => row.name == *expected,
        "duration" => row.duration_nanos().to_string() == *expected,
        _ => row
            .field_value(field_name)
            .as_deref()
            .map(|actual| actual == expected.as_str())
            .unwrap_or(false),
    })
}

fn build_jaeger_dependencies(
    traces: impl IntoIterator<Item = Vec<TraceSpanRow>>,
) -> Vec<JaegerDependencyLink> {
    let mut calls_by_edge: HashMap<(String, String), u64> = HashMap::new();

    for rows in traces {
        let mut service_by_span_id = HashMap::new();
        for row in &rows {
            if let Some(service_name) = row.service_name() {
                service_by_span_id.insert(row.span_id.clone(), service_name.to_string());
            }
        }

        for row in &rows {
            let Some(child_service) = row.service_name() else {
                continue;
            };
            let Some(parent_span_id) = row.parent_span_id.as_deref().filter(|value| !value.is_empty()) else {
                continue;
            };
            let Some(parent_service) = service_by_span_id.get(parent_span_id) else {
                continue;
            };
            if parent_service == child_service {
                continue;
            }
            *calls_by_edge
                .entry((parent_service.clone(), child_service.to_string()))
                .or_default() += 1;
        }
    }

    let mut dependencies: Vec<_> = calls_by_edge
        .into_iter()
        .map(|((parent, child), call_count)| JaegerDependencyLink {
            parent,
            child,
            call_count,
        })
        .collect();
    dependencies.sort_by(|left, right| {
        right
            .call_count
            .cmp(&left.call_count)
            .then_with(|| left.parent.cmp(&right.parent))
            .then_with(|| left.child.cmp(&right.child))
    });
    dependencies
}

fn build_jaeger_trace(rows: Vec<TraceSpanRow>) -> Option<JaegerTrace> {
    let trace_id = rows.first()?.trace_id.clone();
    let mut processes = BTreeMap::new();
    let mut process_ids_by_service: HashMap<String, String> = HashMap::new();
    let mut next_process_id = 1usize;
    let mut spans = Vec::with_capacity(rows.len());

    for row in rows {
        let service_name = row.service_name().unwrap_or("-").to_string();
        let process_id = if let Some(process_id) = process_ids_by_service.get(&service_name) {
            process_id.clone()
        } else {
            let process_id = format!("p{}", next_process_id);
            next_process_id += 1;
            process_ids_by_service.insert(service_name.clone(), process_id.clone());
            processes.insert(
                process_id.clone(),
                JaegerProcess {
                    service_name: service_name.clone(),
                    tags: process_tags_from_row(&row),
                },
            );
            process_id
        };

        if let Some(process) = processes.get_mut(&process_id) {
            merge_jaeger_tags(&mut process.tags, process_tags_from_row(&row));
        }

        let references = row
            .parent_span_id
            .as_ref()
            .map(|parent_span_id| JaegerReference {
                ref_type: "CHILD_OF".to_string(),
                span_id: parent_span_id.clone(),
                trace_id: row.trace_id.clone(),
            })
            .into_iter()
            .collect();

        spans.push(JaegerSpan {
            duration: row.duration_nanos() / 1_000,
            logs: Vec::new(),
            operation_name: row.name.clone(),
            process_id,
            references,
            span_id: row.span_id.clone(),
            start_time: row.start_unix_nano / 1_000,
            tags: span_tags_from_row(&row),
            trace_id: row.trace_id.clone(),
            warnings: None,
        });
    }

    Some(JaegerTrace {
        processes,
        spans,
        trace_id,
        warnings: None,
    })
}

fn merge_jaeger_tags(existing: &mut Vec<JaegerTag>, incoming: Vec<JaegerTag>) {
    let mut tags: BTreeSet<JaegerTag> = existing.iter().cloned().collect();
    tags.extend(incoming);
    *existing = tags.into_iter().collect();
}

fn process_tags_from_row(row: &TraceSpanRow) -> Vec<JaegerTag> {
    let mut tags: BTreeSet<JaegerTag> = BTreeSet::new();
    for field in &row.fields {
        let Some(key) = field.name.strip_prefix("resource_attr:") else {
            continue;
        };
        if key.is_empty() || key == "service.name" {
            continue;
        }
        tags.insert(jaeger_tag(key.to_string(), field.value.to_string()));
    }
    tags.into_iter().collect()
}

fn span_tags_from_row(row: &TraceSpanRow) -> Vec<JaegerTag> {
    let mut tags: BTreeSet<JaegerTag> = BTreeSet::new();
    for field in &row.fields {
        if should_skip_jaeger_span_tag(&field.name) {
            continue;
        }

        let key = field
            .name
            .strip_prefix("span_attr:")
            .unwrap_or(field.name.as_ref())
            .to_string();
        if key.is_empty() {
            continue;
        }
        tags.insert(jaeger_tag(key, field.value.to_string()));
    }
    tags.into_iter().collect()
}

fn should_skip_jaeger_span_tag(field_name: &str) -> bool {
    matches!(
        field_name,
        "_time"
            | "duration"
            | "end_time_unix_nano"
            | "name"
            | "parent_span_id"
            | "span_id"
            | "start_time_unix_nano"
            | "trace_id"
    ) || field_name.starts_with("resource_attr:")
}

fn jaeger_tag(key: String, value: String) -> JaegerTag {
    JaegerTag {
        key,
        value_type: "string",
        value,
    }
}

fn jaeger_success_response<T>(data: Vec<T>) -> JaegerResponse<Vec<T>> {
    let total = data.len();
    JaegerResponse {
        data,
        errors: None,
        limit: 0,
        offset: 0,
        total,
    }
}

fn jaeger_not_found_trace_response() -> JaegerResponse<Vec<JaegerTrace>> {
    JaegerResponse {
        data: Vec::new(),
        errors: Some(vec![JaegerResponseError {
            code: StatusCode::NOT_FOUND.as_u16(),
            msg: "trace not found".to_string(),
        }]),
        limit: 0,
        offset: 0,
        total: 0,
    }
}

fn render_tempo_trace_response(
    trace_id: &str,
    rows: Vec<TraceSpanRow>,
    headers: &HeaderMap,
) -> Result<Response, (StatusCode, Json<ErrorResponse>)> {
    if rows.is_empty() {
        return Err((
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("trace not found: {trace_id}"),
            }),
        ));
    }

    let export_request = export_request_from_rows(&rows);
    if accepts_protobuf(headers) {
        let payload = encode_export_trace_service_request_protobuf(&export_request)
            .map_err(internal_server_error)?;
        Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/protobuf")],
            payload,
        )
            .into_response())
    } else {
        Ok(Json(export_request).into_response())
    }
}

fn build_tempo_search_response(
    traces: Vec<TempoSearchTrace>,
    total_blocks: u64,
) -> TempoSearchResponse {
    TempoSearchResponse {
        metrics: TempoQueryMetrics {
            inspected_traces: Some(traces.len() as u64),
            inspected_bytes: "0".to_string(),
            total_blocks: Some(total_blocks),
        },
        traces,
    }
}

fn search_tempo_local(
    query: &QueryService,
    tempo_query: &TempoSearchQuery,
) -> Result<Vec<TempoSearchTrace>, (StatusCode, Json<ErrorResponse>)> {
    let parsed = tempo_query.parse()?;
    let hits = query.search_traces(&parsed.search_request);
    let mut traces = Vec::new();
    for hit in hits {
        let rows = query.get_trace(&hit.trace_id);
        if !parsed.matches_rows(&rows) {
            continue;
        }
        if let Some(trace) = build_tempo_search_trace(rows) {
            traces.push(trace);
            if traces.len() >= parsed.limit {
                break;
            }
        }
    }
    Ok(traces)
}

async fn search_tempo_cluster(
    state: &SelectState,
    tempo_query: &TempoSearchQuery,
) -> Result<Vec<TempoSearchTrace>, (StatusCode, Json<ErrorResponse>)> {
    let parsed = tempo_query.parse()?;
    let hits = gather_trace_hits_cluster(
        state,
        &SearchQuery {
            start_unix_nano: parsed.search_request.start_unix_nano,
            end_unix_nano: parsed.search_request.end_unix_nano,
            service_name: parsed.search_request.service_name.clone(),
            operation_name: parsed.search_request.operation_name.clone(),
            field_filter: parsed
                .search_request
                .field_filters
                .iter()
                .map(|filter| format!("{}={}", filter.name, filter.value))
                .collect(),
            limit: Some(parsed.search_request.limit),
        },
    )
    .await?;

    let mut traces = Vec::new();
    for hit in hits {
        let rows = fetch_trace_rows_cluster(state, &hit.trace_id).await?;
        if !parsed.matches_rows(&rows) {
            continue;
        }
        if let Some(trace) = build_tempo_search_trace(rows) {
            traces.push(trace);
            if traces.len() >= parsed.limit {
                break;
            }
        }
    }
    Ok(traces)
}

fn build_tempo_search_trace(mut rows: Vec<TraceSpanRow>) -> Option<TempoSearchTrace> {
    if rows.is_empty() {
        return None;
    }
    rows.sort_by_key(|row| row.start_unix_nano);
    let trace_id = rows.first()?.trace_id.clone();
    let root_span = rows
        .iter()
        .find(|row| row.parent_span_id.is_none())
        .unwrap_or(&rows[0]);
    let start_unix_nano = rows
        .iter()
        .map(|row| row.start_unix_nano)
        .min()
        .unwrap_or(root_span.start_unix_nano);
    let end_unix_nano = rows
        .iter()
        .map(|row| row.end_unix_nano)
        .max()
        .unwrap_or(root_span.end_unix_nano);

    Some(TempoSearchTrace {
        trace_id,
        root_service_name: root_span.service_name().unwrap_or("-").to_string(),
        root_trace_name: root_span.name.clone(),
        start_time_unix_nano: start_unix_nano.to_string(),
        duration_ms: end_unix_nano.saturating_sub(start_unix_nano).max(0) as u64 / 1_000_000,
    })
}

fn list_tempo_tags(query: &QueryService, scope: Option<&str>, limit: Option<usize>) -> Vec<String> {
    let all_tags: Vec<String> = query
        .list_field_names()
        .into_iter()
        .filter_map(|field_name| tempo_tag_name_for_internal(&field_name, scope))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    apply_limit(all_tags, limit)
}

async fn list_tempo_tags_cluster_all(
    state: &SelectState,
    scope: Option<&str>,
    limit: Option<usize>,
) -> Result<Vec<String>, (StatusCode, Json<ErrorResponse>)> {
    let field_names = gather_field_names_cluster(state).await?;
    let tags = field_names
        .into_iter()
        .filter_map(|field_name| tempo_tag_name_for_internal(&field_name, scope))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok(apply_limit(tags, limit))
}

fn build_tempo_tag_scopes(query: &QueryService, limit: Option<usize>) -> Vec<TempoTagScope> {
    build_tempo_tag_scopes_from_names(&query.list_field_names(), limit)
}

fn build_tempo_tag_scopes_from_names(
    field_names: &[String],
    limit: Option<usize>,
) -> Vec<TempoTagScope> {
    let mut scoped: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for field_name in field_names {
        if let Some((scope, tag)) = tempo_tag_scope_and_name(field_name) {
            scoped.entry(scope.to_string()).or_default().insert(tag);
        }
    }
    scoped
        .into_iter()
        .map(|(name, tags)| TempoTagScope {
            name,
            tags: apply_limit(tags.into_iter().collect(), limit),
        })
        .collect()
}

fn list_tempo_tag_values(
    query: &QueryService,
    tag_name: &str,
    limit: Option<usize>,
) -> Vec<String> {
    let internal_field_name = internal_field_name_for_tempo_tag(tag_name);
    apply_limit(query.list_field_values(&internal_field_name), limit)
}

async fn gather_tempo_tag_values_cluster(
    state: &SelectState,
    tag_name: &str,
    limit: Option<usize>,
) -> Result<Vec<String>, (StatusCode, Json<ErrorResponse>)> {
    let values =
        gather_field_values_cluster(state, &internal_field_name_for_tempo_tag(tag_name)).await?;
    Ok(apply_limit(values, limit))
}

async fn gather_field_names_cluster(
    state: &SelectState,
) -> Result<Vec<String>, (StatusCode, Json<ErrorResponse>)> {
    let mut tags = BTreeSet::new();
    let mut successful_nodes = 0usize;
    let mut had_error = false;
    let selected_nodes = state
        .health
        .select_nodes(state.cluster.storage_nodes().iter().map(String::as_str), 1);
    state.metrics.record_read_skips(selected_nodes.skipped);

    for node in selected_nodes.nodes {
        state.metrics.record_read_request();
        let response = match state
            .http_client
            .get(format!("{node}/internal/v1/tags"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                if state.health.mark_failure(&node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                continue;
            }
        };

        if !response.status().is_success() {
            had_error = true;
            state.metrics.record_read_error();
            if state
                .health
                .mark_failure(&node, response.status().to_string())
            {
                state.metrics.record_node_quarantine();
            }
            continue;
        }

        match response.json::<TagNamesResponse>().await {
            Ok(remote_tags) => {
                state.health.mark_success(&node);
                successful_nodes += 1;
                tags.extend(remote_tags.tags);
            }
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                warn!(node = %node, error = %error, "failed to decode tags response");
            }
        }
    }

    if successful_nodes == 0 && had_error {
        Err(bad_gateway("all storage nodes failed while listing tags"))
    } else {
        Ok(tags.into_iter().collect())
    }
}

async fn gather_field_values_cluster(
    state: &SelectState,
    field_name: &str,
) -> Result<Vec<String>, (StatusCode, Json<ErrorResponse>)> {
    let mut values = BTreeSet::new();
    let mut successful_nodes = 0usize;
    let mut had_error = false;
    let selected_nodes = state
        .health
        .select_nodes(state.cluster.storage_nodes().iter().map(String::as_str), 1);
    state.metrics.record_read_skips(selected_nodes.skipped);

    for node in selected_nodes.nodes {
        state.metrics.record_read_request();
        let response = match state
            .http_client
            .get(format!("{node}/internal/v1/tags/{field_name}/values"))
            .bearer_auth_opt(state.auth.internal_bearer_token())
            .send()
            .await
        {
            Ok(response) => response,
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                if state.health.mark_failure(&node, error.to_string()) {
                    state.metrics.record_node_quarantine();
                }
                continue;
            }
        };

        if !response.status().is_success() {
            had_error = true;
            state.metrics.record_read_error();
            if state
                .health
                .mark_failure(&node, response.status().to_string())
            {
                state.metrics.record_node_quarantine();
            }
            continue;
        }

        match response.json::<TagValuesResponse>().await {
            Ok(remote_values) => {
                state.health.mark_success(&node);
                successful_nodes += 1;
                values.extend(remote_values.values);
            }
            Err(error) => {
                had_error = true;
                state.metrics.record_read_error();
                warn!(node = %node, error = %error, "failed to decode tag values response");
            }
        }
    }

    if successful_nodes == 0 && had_error {
        Err(bad_gateway(
            "all storage nodes failed while listing tag values",
        ))
    } else {
        Ok(values.into_iter().collect())
    }
}

fn accepts_protobuf(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains("application/protobuf"))
        .unwrap_or(false)
}

fn internal_field_name_for_tempo_tag(tag_name: &str) -> String {
    let tag_name = tag_name.trim().trim_start_matches('.');
    if let Some(rest) = tag_name.strip_prefix("resource.") {
        return format!("resource_attr:{rest}");
    }
    if let Some(rest) = tag_name.strip_prefix("span.") {
        return format!("span_attr:{rest}");
    }

    match tag_name {
        "service.name" | "trace:rootService" => "resource_attr:service.name".to_string(),
        "name" | "trace:rootName" => "name".to_string(),
        "duration" | "trace:duration" | "traceDuration" => "duration".to_string(),
        "status" | "status.code" | "statusCode" => "status_code".to_string(),
        "statusMessage" | "status_message" | "status.message" => "status_message".to_string(),
        other => format!("span_attr:{other}"),
    }
}

fn tempo_tag_name_for_internal(field_name: &str, scope: Option<&str>) -> Option<String> {
    let (field_scope, tag_name) = tempo_tag_scope_and_name(field_name)?;
    let requested_scope = scope.unwrap_or("all");
    if requested_scope != "all" && requested_scope != field_scope {
        return None;
    }
    Some(tag_name)
}

fn tempo_tag_scope_and_name(field_name: &str) -> Option<(&'static str, String)> {
    if let Some(rest) = field_name.strip_prefix("resource_attr:") {
        return Some(("resource", rest.to_string()));
    }
    if let Some(rest) = field_name.strip_prefix("span_attr:") {
        return Some(("span", rest.to_string()));
    }
    match field_name {
        "name" => Some(("intrinsic", "name".to_string())),
        "duration" => Some(("intrinsic", "trace:duration".to_string())),
        "status_code" => Some(("intrinsic", "statusCode".to_string())),
        "status_message" => Some(("intrinsic", "statusMessage".to_string())),
        _ => None,
    }
}

fn apply_limit<T>(mut values: Vec<T>, limit: Option<usize>) -> Vec<T> {
    if let Some(limit) = limit {
        values.truncate(limit);
    }
    values
}

fn render_storage_metrics(stats: &StorageStatsSnapshot) -> String {
    [
        "# TYPE vt_build_info gauge".to_string(),
        format!(
            "vt_build_info{{version=\"{}\",target_arch=\"{}\",target_os=\"{}\"}} 1",
            env!("CARGO_PKG_VERSION"),
            std::env::consts::ARCH,
            std::env::consts::OS
        ),
        "# TYPE vt_rows_ingested_total counter".to_string(),
        format!("vt_rows_ingested_total {}", stats.rows_ingested),
        "# TYPE vt_traces_tracked gauge".to_string(),
        format!("vt_traces_tracked {}", stats.traces_tracked),
        "# TYPE vt_storage_retained_trace_blocks gauge".to_string(),
        format!(
            "vt_storage_retained_trace_blocks {}",
            stats.retained_trace_blocks
        ),
        "# TYPE vt_storage_persisted_bytes gauge".to_string(),
        format!("vt_storage_persisted_bytes {}", stats.persisted_bytes),
        "# TYPE vt_storage_segments gauge".to_string(),
        format!("vt_storage_segments {}", stats.segment_count),
        "# TYPE vt_storage_trace_head_segments gauge".to_string(),
        format!(
            "vt_storage_trace_head_segments {}",
            stats.trace_head_segments
        ),
        "# TYPE vt_storage_trace_head_rows gauge".to_string(),
        format!("vt_storage_trace_head_rows {}", stats.trace_head_rows),
        "# TYPE vt_storage_typed_field_columns gauge".to_string(),
        format!(
            "vt_storage_typed_field_columns {}",
            stats.typed_field_columns
        ),
        "# TYPE vt_storage_string_field_columns gauge".to_string(),
        format!(
            "vt_storage_string_field_columns {}",
            stats.string_field_columns
        ),
        "# TYPE vt_trace_window_lookups_total counter".to_string(),
        format!(
            "vt_trace_window_lookups_total {}",
            stats.trace_window_lookups
        ),
        "# TYPE vt_row_queries_total counter".to_string(),
        format!("vt_row_queries_total {}", stats.row_queries),
        "# TYPE vt_storage_segment_read_batches_total counter".to_string(),
        format!(
            "vt_storage_segment_read_batches_total {}",
            stats.segment_read_batches
        ),
        "# TYPE vt_storage_part_selective_decodes_total counter".to_string(),
        format!(
            "vt_storage_part_selective_decodes_total {}",
            stats.part_selective_decodes
        ),
        "# TYPE vt_storage_fsync_operations_total counter".to_string(),
        format!(
            "vt_storage_fsync_operations_total {}",
            stats.fsync_operations
        ),
        "# TYPE vt_storage_trace_group_commit_flushes_total counter".to_string(),
        format!(
            "vt_storage_trace_group_commit_flushes_total {}",
            stats.trace_group_commit_flushes
        ),
        "# TYPE vt_storage_trace_group_commit_rows_total counter".to_string(),
        format!(
            "vt_storage_trace_group_commit_rows_total {}",
            stats.trace_group_commit_rows
        ),
        "# TYPE vt_storage_trace_group_commit_bytes_total counter".to_string(),
        format!(
            "vt_storage_trace_group_commit_bytes_total {}",
            stats.trace_group_commit_bytes
        ),
        "# TYPE vt_storage_trace_batch_queue_depth gauge".to_string(),
        format!(
            "vt_storage_trace_batch_queue_depth {}",
            stats.trace_batch_queue_depth
        ),
        "# TYPE vt_storage_trace_batch_max_queue_depth gauge".to_string(),
        format!(
            "vt_storage_trace_batch_max_queue_depth {}",
            stats.trace_batch_max_queue_depth
        ),
        "# TYPE vt_storage_trace_batch_flushes_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_flushes_total {}",
            stats.trace_batch_flushes
        ),
        "# TYPE vt_storage_trace_batch_rows_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_rows_total {}",
            stats.trace_batch_rows
        ),
        "# TYPE vt_storage_trace_batch_input_blocks_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_input_blocks_total {}",
            stats.trace_batch_input_blocks
        ),
        "# TYPE vt_storage_trace_batch_output_blocks_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_output_blocks_total {}",
            stats.trace_batch_output_blocks
        ),
        "# TYPE vt_storage_trace_batch_wait_micros_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_wait_micros_total {}",
            stats.trace_batch_wait_micros_total
        ),
        "# TYPE vt_storage_trace_batch_wait_micros_max gauge".to_string(),
        format!(
            "vt_storage_trace_batch_wait_micros_max {}",
            stats.trace_batch_wait_micros_max
        ),
        "# TYPE vt_storage_trace_live_update_queue_depth gauge".to_string(),
        format!(
            "vt_storage_trace_live_update_queue_depth {}",
            stats.trace_live_update_queue_depth
        ),
        "# TYPE vt_storage_trace_seal_queue_depth gauge".to_string(),
        format!(
            "vt_storage_trace_seal_queue_depth {}",
            stats.trace_seal_queue_depth
        ),
        "# TYPE vt_storage_trace_seal_completed_total counter".to_string(),
        format!(
            "vt_storage_trace_seal_completed_total {}",
            stats.trace_seal_completed
        ),
        "# TYPE vt_storage_trace_seal_rows_total counter".to_string(),
        format!("vt_storage_trace_seal_rows_total {}", stats.trace_seal_rows),
        "# TYPE vt_storage_trace_seal_bytes_total counter".to_string(),
        format!(
            "vt_storage_trace_seal_bytes_total {}",
            stats.trace_seal_bytes
        ),
        "# TYPE vt_storage_trace_batch_flush_due_to_rows_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_flush_due_to_rows_total {}",
            stats.trace_batch_flush_due_to_rows
        ),
        "# TYPE vt_storage_trace_batch_flush_due_to_blocks_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_flush_due_to_blocks_total {}",
            stats.trace_batch_flush_due_to_blocks
        ),
        "# TYPE vt_storage_trace_batch_flush_due_to_wait_total counter".to_string(),
        format!(
            "vt_storage_trace_batch_flush_due_to_wait_total {}",
            stats.trace_batch_flush_due_to_wait
        ),
    ]
    .join("\n")
        + "\n"
}

fn render_cluster_metrics(
    metrics: &ClusterMetricsSnapshot,
    cluster: &ClusterConfig,
    unhealthy_nodes: usize,
) -> String {
    let healthy_nodes = cluster
        .storage_nodes()
        .len()
        .saturating_sub(unhealthy_nodes);
    [
        "# TYPE vt_cluster_storage_nodes gauge".to_string(),
        format!("vt_cluster_storage_nodes {}", cluster.storage_nodes().len()),
        "# TYPE vt_cluster_replication_factor gauge".to_string(),
        format!(
            "vt_cluster_replication_factor {}",
            cluster.replication_factor()
        ),
        "# TYPE vt_cluster_write_quorum gauge".to_string(),
        format!("vt_cluster_write_quorum {}", cluster.write_quorum()),
        "# TYPE vt_cluster_read_quorum gauge".to_string(),
        format!("vt_cluster_read_quorum {}", cluster.read_quorum()),
        "# TYPE vt_cluster_healthy_nodes gauge".to_string(),
        format!("vt_cluster_healthy_nodes {}", healthy_nodes),
        "# TYPE vt_cluster_unhealthy_nodes gauge".to_string(),
        format!("vt_cluster_unhealthy_nodes {}", unhealthy_nodes),
        "# TYPE vt_remote_write_batches_total counter".to_string(),
        format!(
            "vt_remote_write_batches_total {}",
            metrics.remote_write_batches
        ),
        "# TYPE vt_remote_write_rows_total counter".to_string(),
        format!("vt_remote_write_rows_total {}", metrics.remote_write_rows),
        "# TYPE vt_remote_write_errors_total counter".to_string(),
        format!(
            "vt_remote_write_errors_total {}",
            metrics.remote_write_errors
        ),
        "# TYPE vt_remote_write_skips_total counter".to_string(),
        format!("vt_remote_write_skips_total {}", metrics.remote_write_skips),
        "# TYPE vt_remote_read_requests_total counter".to_string(),
        format!(
            "vt_remote_read_requests_total {}",
            metrics.remote_read_requests
        ),
        "# TYPE vt_remote_read_errors_total counter".to_string(),
        format!("vt_remote_read_errors_total {}", metrics.remote_read_errors),
        "# TYPE vt_remote_read_skips_total counter".to_string(),
        format!("vt_remote_read_skips_total {}", metrics.remote_read_skips),
        "# TYPE vt_cluster_node_quarantines_total counter".to_string(),
        format!(
            "vt_cluster_node_quarantines_total {}",
            metrics.node_quarantines
        ),
        "# TYPE vt_read_repairs_total counter".to_string(),
        format!("vt_read_repairs_total {}", metrics.read_repairs),
        "# TYPE vt_read_repair_errors_total counter".to_string(),
        format!("vt_read_repair_errors_total {}", metrics.read_repair_errors),
        "# TYPE vt_cluster_membership_probes_total counter".to_string(),
        format!(
            "vt_cluster_membership_probes_total {}",
            metrics.membership_probe_requests
        ),
        "# TYPE vt_cluster_membership_probe_errors_total counter".to_string(),
        format!(
            "vt_cluster_membership_probe_errors_total {}",
            metrics.membership_probe_errors
        ),
        "# TYPE vt_cluster_control_probe_requests_total counter".to_string(),
        format!(
            "vt_cluster_control_probe_requests_total {}",
            metrics.control_probe_requests
        ),
        "# TYPE vt_cluster_control_probe_errors_total counter".to_string(),
        format!(
            "vt_cluster_control_probe_errors_total {}",
            metrics.control_probe_errors
        ),
        "# TYPE vt_cluster_control_state_sync_requests_total counter".to_string(),
        format!(
            "vt_cluster_control_state_sync_requests_total {}",
            metrics.control_state_sync_requests
        ),
        "# TYPE vt_cluster_control_state_sync_errors_total counter".to_string(),
        format!(
            "vt_cluster_control_state_sync_errors_total {}",
            metrics.control_state_sync_errors
        ),
        "# TYPE vt_cluster_control_leader_changes_total counter".to_string(),
        format!(
            "vt_cluster_control_leader_changes_total {}",
            metrics.control_leader_changes
        ),
        "# TYPE vt_cluster_control_epoch gauge".to_string(),
        format!("vt_cluster_control_epoch {}", metrics.control_epoch),
    ]
    .join("\n")
        + "\n"
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StandardOtlpTraceRequest {
    #[serde(default)]
    resource_spans: Vec<StandardOtlpResourceSpans>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StandardOtlpResourceSpans {
    #[serde(default)]
    resource_attributes: Vec<StandardOtlpKeyValue>,
    resource: Option<StandardOtlpResource>,
    #[serde(default)]
    scope_spans: Vec<StandardOtlpScopeSpans>,
}

#[derive(Debug, Deserialize)]
struct StandardOtlpResource {
    #[serde(default)]
    attributes: Vec<StandardOtlpKeyValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StandardOtlpScopeSpans {
    scope_name: Option<String>,
    scope_version: Option<String>,
    #[serde(default)]
    scope_attributes: Vec<StandardOtlpKeyValue>,
    scope: Option<StandardOtlpScope>,
    #[serde(default)]
    spans: Vec<StandardOtlpSpanRecord>,
}

#[derive(Debug, Deserialize)]
struct StandardOtlpScope {
    name: Option<String>,
    version: Option<String>,
    #[serde(default)]
    attributes: Vec<StandardOtlpKeyValue>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StandardOtlpSpanRecord {
    trace_id: String,
    span_id: String,
    #[serde(default, deserialize_with = "deserialize_optional_string_or_empty_as_none")]
    parent_span_id: Option<String>,
    name: String,
    #[serde(deserialize_with = "deserialize_i64_from_string_or_number")]
    start_time_unix_nano: i64,
    #[serde(deserialize_with = "deserialize_i64_from_string_or_number")]
    end_time_unix_nano: i64,
    #[serde(default)]
    attributes: Vec<StandardOtlpKeyValue>,
    #[serde(default)]
    status: Option<StandardOtlpStatus>,
}

#[derive(Debug, Deserialize)]
struct StandardOtlpStatus {
    code: i32,
    message: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StandardOtlpKeyValue {
    key: String,
    value: StandardOtlpAnyValue,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StandardOtlpAnyValue {
    string_value: Option<String>,
    bool_value: Option<bool>,
    #[serde(default, deserialize_with = "deserialize_optional_i64_from_string_or_number")]
    int_value: Option<i64>,
    double_value: Option<f64>,
    bytes_value: Option<String>,
    array_value: Option<serde_json::Value>,
    kvlist_value: Option<serde_json::Value>,
}

impl StandardOtlpTraceRequest {
    fn into_internal(self) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: self
                .resource_spans
                .into_iter()
                .map(StandardOtlpResourceSpans::into_internal)
                .collect(),
        }
    }
}

impl StandardOtlpResourceSpans {
    fn into_internal(self) -> ResourceSpans {
        let mut resource_attributes = self.resource_attributes;
        if let Some(resource) = self.resource {
            resource_attributes.extend(resource.attributes);
        }

        ResourceSpans {
            resource_attributes: resource_attributes
                .into_iter()
                .map(StandardOtlpKeyValue::into_internal)
                .collect(),
            scope_spans: self
                .scope_spans
                .into_iter()
                .map(StandardOtlpScopeSpans::into_internal)
                .collect(),
        }
    }
}

impl StandardOtlpScopeSpans {
    fn into_internal(self) -> ScopeSpans {
        let mut scope_attributes = self.scope_attributes;
        let mut scope_name = self.scope_name;
        let mut scope_version = self.scope_version;
        if let Some(scope) = self.scope {
            if scope_name.is_none() {
                scope_name = scope.name;
            }
            if scope_version.is_none() {
                scope_version = scope.version;
            }
            scope_attributes.extend(scope.attributes);
        }

        ScopeSpans {
            scope_name,
            scope_version,
            scope_attributes: scope_attributes
                .into_iter()
                .map(StandardOtlpKeyValue::into_internal)
                .collect(),
            spans: self
                .spans
                .into_iter()
                .map(StandardOtlpSpanRecord::into_internal)
                .collect(),
        }
    }
}

impl StandardOtlpSpanRecord {
    fn into_internal(self) -> SpanRecord {
        SpanRecord {
            trace_id: self.trace_id,
            span_id: self.span_id,
            parent_span_id: self.parent_span_id,
            name: self.name,
            start_time_unix_nano: self.start_time_unix_nano,
            end_time_unix_nano: self.end_time_unix_nano,
            attributes: self
                .attributes
                .into_iter()
                .map(StandardOtlpKeyValue::into_internal)
                .collect(),
            status: self.status.map(StandardOtlpStatus::into_internal),
        }
    }
}

impl StandardOtlpStatus {
    fn into_internal(self) -> OtlpStatus {
        OtlpStatus {
            code: self.code,
            message: self.message.unwrap_or_default(),
        }
    }
}

impl StandardOtlpKeyValue {
    fn into_internal(self) -> KeyValue {
        KeyValue::new(self.key, self.value.into_internal())
    }
}

impl StandardOtlpAnyValue {
    fn into_internal(self) -> AttributeValue {
        if let Some(value) = self.string_value {
            return AttributeValue::String(value);
        }
        if let Some(value) = self.bool_value {
            return AttributeValue::Bool(value);
        }
        if let Some(value) = self.int_value {
            return AttributeValue::I64(value);
        }
        if let Some(value) = self.double_value {
            return AttributeValue::F64(value);
        }
        if let Some(value) = self.bytes_value {
            return AttributeValue::String(value);
        }
        if let Some(value) = self.array_value {
            return AttributeValue::String(value.to_string());
        }
        if let Some(value) = self.kvlist_value {
            return AttributeValue::String(value.to_string());
        }
        AttributeValue::String(String::new())
    }
}

fn bad_request(error: impl ToString) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

fn bad_gateway(error: impl ToString) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_GATEWAY,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

fn service_unavailable(error: impl ToString) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

fn internal_server_error(error: impl ToString) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

fn decode_otlp_http_trace_rows(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Vec<TraceSpanRow>, String> {
    if is_otlp_protobuf_request(headers) {
        decode_trace_rows_protobuf(body).map_err(|error| error.to_string())
    } else {
        let request = decode_otlp_http_trace_request_json(body)?;
        flatten_export_request(&request).map_err(|error| error.to_string())
    }
}

fn decode_otlp_http_trace_block(headers: &HeaderMap, body: &[u8]) -> Result<TraceBlock, String> {
    if is_otlp_protobuf_request(headers) {
        decode_trace_block_protobuf(body).map_err(|error| error.to_string())
    } else {
        let request = decode_otlp_http_trace_request_json(body)?;
        let rows = flatten_export_request(&request).map_err(|error| error.to_string())?;
        Ok(TraceBlock::from_rows(rows))
    }
}

fn decode_otlp_http_trace_blocks(
    headers: &HeaderMap,
    body: &[u8],
    trace_shards: usize,
) -> Result<Vec<TraceBlock>, String> {
    if is_otlp_protobuf_request(headers) && trace_shards > 1 {
        decode_trace_blocks_protobuf_sharded(body, trace_shards).map_err(|error| error.to_string())
    } else {
        decode_otlp_http_trace_block(headers, body).map(|block| vec![block])
    }
}

fn decode_otlp_http_logs_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<ExportLogsServiceRequest, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    if is_protobuf_content_type(content_type) {
        decode_export_logs_service_request_protobuf(body).map_err(|error| error.to_string())
    } else {
        serde_json::from_slice(body).map_err(|error| error.to_string())
    }
}

fn decode_otlp_http_trace_request_json(body: &[u8]) -> Result<ExportTraceServiceRequest, String> {
    match serde_json::from_slice::<ExportTraceServiceRequest>(body) {
        Ok(request) => Ok(request),
        Err(primary_error) => serde_json::from_slice::<StandardOtlpTraceRequest>(body)
            .map(StandardOtlpTraceRequest::into_internal)
            .map_err(|compat_error| {
                format!(
                    "failed to decode OTLP JSON as internal or standard format: primary={primary_error}; compat={compat_error}"
                )
            }),
    }
}

fn decode_otlp_grpc_trace_rows(body: &[u8]) -> Result<Vec<TraceSpanRow>, String> {
    let payload = decode_grpc_unary_message(body)?;
    decode_trace_rows_protobuf(payload).map_err(|error| error.to_string())
}

fn decode_otlp_grpc_trace_blocks(
    body: &[u8],
    trace_shards: usize,
) -> Result<Vec<TraceBlock>, String> {
    let payload = decode_grpc_unary_message(body)?;
    if trace_shards > 1 {
        decode_trace_blocks_protobuf_sharded(payload, trace_shards)
            .map_err(|error| error.to_string())
    } else {
        decode_trace_block_protobuf(payload)
            .map(|block| vec![block])
            .map_err(|error| error.to_string())
    }
}

fn decode_otlp_grpc_logs_request(body: &[u8]) -> Result<ExportLogsServiceRequest, String> {
    let payload = decode_grpc_unary_message(body)?;
    decode_export_logs_service_request_protobuf(payload).map_err(|error| error.to_string())
}

fn decode_internal_rows_request(
    headers: &HeaderMap,
    body: &[u8],
) -> Result<Vec<TraceSpanRow>, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    if is_internal_trace_rows_content_type(content_type) {
        decode_trace_rows(body).map_err(|error| error.to_string())
    } else {
        let request: RowsIngestRequest =
            serde_json::from_slice(body).map_err(|error| error.to_string())?;
        Ok(request.rows)
    }
}

fn encode_internal_rows_payload(rows: &[TraceSpanRow]) -> Result<Vec<u8>, String> {
    Ok(encode_trace_rows(rows))
}

fn decode_internal_logs_request(headers: &HeaderMap, body: &[u8]) -> Result<Vec<LogRow>, String> {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("application/json");

    if is_internal_log_rows_content_type(content_type) {
        decode_log_rows(body).map_err(|error| error.to_string())
    } else {
        let request: LogsIngestRequest =
            serde_json::from_slice(body).map_err(|error| error.to_string())?;
        Ok(request.rows)
    }
}

fn build_node_write_plans(
    state: &InsertState,
    rows: Vec<TraceSpanRow>,
) -> Result<Vec<NodeWritePlan>, String> {
    let mut rows_by_trace: HashMap<String, Vec<TraceSpanRow>> = HashMap::new();
    for row in rows {
        rows_by_trace
            .entry(row.trace_id.clone())
            .or_default()
            .push(row);
    }

    let mut batches_by_node: HashMap<String, Vec<SharedEncodedTraceBatch>> = HashMap::new();
    for (trace_id, trace_rows) in rows_by_trace {
        let encoded_rows = Arc::new(trace_rows.iter().map(encode_trace_row).collect::<Vec<_>>());
        let selected_nodes = state.health.select_nodes(
            state.cluster.placements(&trace_id),
            state.cluster.write_quorum(),
        );
        state.metrics.record_write_skips(selected_nodes.skipped);

        let batch = SharedEncodedTraceBatch {
            row_count: encoded_rows.len(),
            encoded_rows,
        };
        for node in selected_nodes.nodes {
            batches_by_node.entry(node).or_default().push(batch.clone());
        }
    }

    let mut write_plans = Vec::with_capacity(batches_by_node.len());
    for (node, batches) in batches_by_node {
        let row_count = batches.iter().map(|batch| batch.row_count).sum();
        let payload = encode_trace_rows_from_encoded_rows(
            batches
                .iter()
                .flat_map(|batch| batch.encoded_rows.iter().map(Vec::as_slice)),
        );
        write_plans.push(NodeWritePlan {
            node,
            payload: Bytes::from(payload),
            row_count,
        });
    }

    Ok(write_plans)
}

fn build_node_log_write_plans(
    state: &InsertState,
    rows: Vec<LogRow>,
) -> Result<Vec<NodeLogWritePlan>, String> {
    let mut rows_by_key: HashMap<String, Vec<LogRow>> = HashMap::new();
    for row in rows {
        let placement_key = row.trace_id.clone().unwrap_or_else(|| row.log_id.clone());
        rows_by_key.entry(placement_key).or_default().push(row);
    }

    let mut batches_by_node: HashMap<String, Vec<SharedEncodedLogBatch>> = HashMap::new();
    for (placement_key, log_rows) in rows_by_key {
        let encoded_rows: Arc<Vec<Vec<u8>>> =
            Arc::new(log_rows.iter().map(encode_log_row).collect());
        let batch = SharedEncodedLogBatch {
            row_count: log_rows.len(),
            encoded_rows,
        };
        for node in state.cluster.placements(&placement_key) {
            batches_by_node
                .entry(node.to_string())
                .or_default()
                .push(batch.clone());
        }
    }

    let mut write_plans = Vec::with_capacity(batches_by_node.len());
    for (node, batches) in batches_by_node {
        let row_count = batches.iter().map(|batch| batch.row_count).sum();
        let payload = encode_log_rows_from_encoded_rows(
            batches
                .iter()
                .flat_map(|batch| batch.encoded_rows.iter().map(Vec::as_slice)),
        );
        write_plans.push(NodeLogWritePlan {
            node,
            payload: Bytes::from(payload),
            row_count,
        });
    }

    Ok(write_plans)
}

fn decode_grpc_unary_message(body: &[u8]) -> Result<&[u8], String> {
    if body.len() < 5 {
        return Err("invalid gRPC frame: body shorter than 5-byte prefix".to_string());
    }
    if body[0] != 0 {
        return Err("compressed gRPC payloads are not supported".to_string());
    }

    let message_len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    if body.len() != 5 + message_len {
        return Err(format!(
            "invalid gRPC frame length: expected {}, got {}",
            5 + message_len,
            body.len()
        ));
    }
    Ok(&body[5..])
}

fn encode_grpc_unary_message(payload: &[u8]) -> Vec<u8> {
    let mut framed = Vec::with_capacity(5 + payload.len());
    framed.push(0);
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload);
    framed
}

fn grpc_ok_response() -> Response {
    grpc_response("0", None, &[])
}

fn grpc_error_response(status: &str, message: impl Into<String>) -> Response {
    let message = message.into();
    warn!(grpc_status = status, error = %message, "OTLP gRPC request failed");
    grpc_response(status, Some(message.as_str()), &[])
}

fn grpc_status_for_http_status(status: StatusCode) -> &'static str {
    match status {
        StatusCode::BAD_REQUEST => "3",
        StatusCode::UNAUTHORIZED => "16",
        StatusCode::BAD_GATEWAY | StatusCode::SERVICE_UNAVAILABLE => "14",
        _ => "13",
    }
}

fn grpc_response(status: &str, message: Option<&str>, payload: &[u8]) -> Response {
    let mut response = Response::new(axum::body::Body::from(encode_grpc_unary_message(payload)));
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/grpc"),
    );
    headers.insert(
        HeaderName::from_static("grpc-status"),
        HeaderValue::from_str(status).unwrap_or_else(|_| HeaderValue::from_static("13")),
    );
    if let Some(message) = message {
        if let Ok(value) = HeaderValue::from_str(&message.replace('\n', " ")) {
            headers.insert(HeaderName::from_static("grpc-message"), value);
        }
    }
    response
}

fn is_protobuf_content_type(content_type: &str) -> bool {
    matches!(
        content_type
            .split(';')
            .next()
            .unwrap_or(content_type)
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "application/x-protobuf" | "application/protobuf" | "application/vnd.google.protobuf"
    )
}

fn is_otlp_protobuf_request(headers: &HeaderMap) -> bool {
    headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(is_protobuf_content_type)
        .unwrap_or(false)
}

fn otlp_http_ingest_response(protobuf: bool, ingested_rows: usize) -> Response {
    if protobuf {
        StatusCode::OK.into_response()
    } else {
        Json(IngestResponse { ingested_rows }).into_response()
    }
}

fn is_internal_trace_rows_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .eq_ignore_ascii_case(INTERNAL_TRACE_ROWS_CONTENT_TYPE)
}

fn is_internal_log_rows_content_type(content_type: &str) -> bool {
    content_type
        .split(';')
        .next()
        .unwrap_or(content_type)
        .trim()
        .eq_ignore_ascii_case(INTERNAL_LOG_ROWS_CONTENT_TYPE)
}

fn system_time_to_unix_millis(value: SystemTime) -> Option<u64> {
    value
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
}

fn unix_millis_to_system_time(value: u64) -> Option<SystemTime> {
    UNIX_EPOCH.checked_add(Duration::from_millis(value))
}

fn apply_bearer_auth(
    request: reqwest::RequestBuilder,
    bearer_token: Option<&str>,
) -> reqwest::RequestBuilder {
    match bearer_token {
        Some(bearer_token) => request.bearer_auth(bearer_token),
        None => request,
    }
}

trait RequestBuilderExt {
    fn bearer_auth_opt(self, bearer_token: Option<&str>) -> Self;
}

impl RequestBuilderExt for reqwest::RequestBuilder {
    fn bearer_auth_opt(self, bearer_token: Option<&str>) -> Self {
        apply_bearer_auth(self, bearer_token)
    }
}

fn has_valid_bearer_token(headers: &HeaderMap, expected_token: &str) -> bool {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(|value| value.trim() == expected_token)
        .unwrap_or(false)
}

fn sanitize_bearer_token(value: impl Into<String>) -> Option<String> {
    sanitize_bearer_token_option(Some(value.into()))
}

fn sanitize_bearer_token_option(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let value = value.trim().to_string();
        if value.is_empty() {
            None
        } else {
            Some(value)
        }
    })
}

fn deserialize_string_vec<'de, D>(deserializer: D) -> Result<Vec<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(
        match Option::<OneOrManyStrings>::deserialize(deserializer)? {
            None => Vec::new(),
            Some(OneOrManyStrings::One(value)) => vec![value],
            Some(OneOrManyStrings::Many(values)) => values,
        },
    )
}

fn deserialize_i64_from_string_or_number<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    parse_json_i64_value(value).map_err(serde::de::Error::custom)
}

fn deserialize_optional_i64_from_string_or_number<'de, D>(
    deserializer: D,
) -> Result<Option<i64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<serde_json::Value>::deserialize(deserializer)?;
    value
        .map(parse_json_i64_value)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn deserialize_optional_string_or_empty_as_none<'de, D>(
    deserializer: D,
) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Ok(Option::<String>::deserialize(deserializer)?
        .and_then(|value| {
            let trimmed = value.trim().to_string();
            (!trimmed.is_empty()).then_some(trimmed)
        }))
}

fn parse_json_i64_value(value: serde_json::Value) -> Result<i64, String> {
    match value {
        serde_json::Value::Number(value) => value
            .as_i64()
            .ok_or_else(|| "expected signed integer".to_string()),
        serde_json::Value::String(value) => value
            .trim()
            .parse::<i64>()
            .map_err(|error| format!("invalid integer value: {error}")),
        other => Err(format!("unsupported integer value: {other}")),
    }
}

fn parse_field_filter(value: &str) -> Result<FieldFilter, (StatusCode, Json<ErrorResponse>)> {
    let (name, value) = value
        .split_once('=')
        .ok_or_else(|| bad_request(format!("invalid field_filter: {value}")))?;
    let name = name.trim();
    let value = value.trim();
    if name.is_empty() || value.is_empty() {
        return Err(bad_request(format!("invalid field_filter: {value}")));
    }
    Ok(FieldFilter {
        name: name.to_string(),
        value: value.to_string(),
    })
}

fn parse_tempo_tags_filter(
    value: &str,
) -> Result<Vec<(String, String)>, (StatusCode, Json<ErrorResponse>)> {
    let mut filters = Vec::new();
    for part in value.split_whitespace() {
        let (name, expected) = part
            .split_once('=')
            .ok_or_else(|| bad_request(format!("invalid tags filter: {value}")))?;
        let name = internal_field_name_for_tempo_tag(name.trim());
        let expected = expected.trim().trim_matches('"');
        if expected.is_empty() {
            return Err(bad_request(format!("invalid tags filter: {value}")));
        }
        filters.push((name, expected.to_string()));
    }
    Ok(filters)
}

fn parse_jaeger_tags_filter(
    value: Option<&str>,
) -> Result<Vec<(String, String)>, (StatusCode, Json<ErrorResponse>)> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(Vec::new());
    };

    let decoded: BTreeMap<String, serde_json::Value> =
        serde_json::from_str(value).map_err(|error| bad_request(format!("invalid tags: {error}")))?;
    decoded
        .into_iter()
        .map(|(name, value)| {
            let normalized_value = match value {
                serde_json::Value::String(value) => value,
                serde_json::Value::Bool(value) => value.to_string(),
                serde_json::Value::Number(value) => value.to_string(),
                serde_json::Value::Null => {
                    return Err(bad_request("invalid tags: null values are not supported"));
                }
                other => other.to_string(),
            };
            Ok((
                internal_field_name_for_tempo_tag(&name),
                normalized_value,
            ))
        })
        .collect()
}

fn parse_jaeger_duration(value: &str) -> Result<i64, (StatusCode, Json<ErrorResponse>)> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(bad_request("invalid duration: empty value"));
    }

    let (number, multiplier) = if let Some(value) = trimmed.strip_suffix("ns") {
        (value, 1_f64)
    } else if let Some(value) = trimmed.strip_suffix("us") {
        (value, 1_000_f64)
    } else if let Some(value) = trimmed.strip_suffix("µs") {
        (value, 1_000_f64)
    } else if let Some(value) = trimmed.strip_suffix("ms") {
        (value, 1_000_000_f64)
    } else if let Some(value) = trimmed.strip_suffix('s') {
        (value, 1_000_000_000_f64)
    } else if let Some(value) = trimmed.strip_suffix('m') {
        (value, 60_f64 * 1_000_000_000_f64)
    } else if let Some(value) = trimmed.strip_suffix('h') {
        (value, 60_f64 * 60_f64 * 1_000_000_000_f64)
    } else {
        (trimmed, 1_000_f64)
    };

    let numeric_value = number
        .trim()
        .parse::<f64>()
        .map_err(|_| bad_request(format!("invalid duration: {value}")))?;
    if !numeric_value.is_finite() || numeric_value < 0.0 {
        return Err(bad_request(format!("invalid duration: {value}")));
    }

    let duration = (numeric_value * multiplier).round();
    if duration > i64::MAX as f64 {
        return Err(bad_request(format!("invalid duration: {value}")));
    }
    Ok(duration as i64)
}

fn parse_simple_traceql_filters(
    value: &str,
) -> Result<Vec<FieldFilter>, (StatusCode, Json<ErrorResponse>)> {
    let trimmed = value.trim().trim_start_matches('{').trim_end_matches('}');
    if trimmed.is_empty() {
        return Ok(Vec::new());
    }

    trimmed
        .split("&&")
        .map(|condition| {
            let (name, value) = condition
                .split_once('=')
                .ok_or_else(|| bad_request(format!("unsupported TraceQL query: {condition}")))?;
            let name = name.trim().trim_start_matches('.');
            let value = value.trim().trim_matches('"');
            if name.is_empty() || value.is_empty() {
                return Err(bad_request(format!(
                    "unsupported TraceQL query: {condition}"
                )));
            }
            Ok(FieldFilter {
                name: internal_field_name_for_tempo_tag(name),
                value: value.to_string(),
            })
        })
        .collect()
}

fn tempo_field_value(row: &TraceSpanRow, field_name: &str) -> Option<String> {
    match field_name {
        "name" => Some(row.name.clone()),
        "duration" => Some(row.duration_nanos().to_string()),
        _ => row.field_value(field_name).map(|value| value.into_owned()),
    }
}

impl SearchQuery {
    fn as_request(&self) -> Result<TraceSearchRequest, (StatusCode, Json<ErrorResponse>)> {
        Ok(TraceSearchRequest {
            start_unix_nano: self.start_unix_nano,
            end_unix_nano: self.end_unix_nano,
            service_name: self.service_name.clone(),
            operation_name: self.operation_name.clone(),
            field_filters: self
                .field_filter
                .iter()
                .map(|value| parse_field_filter(value))
                .collect::<Result<Vec<_>, _>>()?,
            limit: self.limit.unwrap_or(20),
        })
    }
}

impl LogSearchQuery {
    fn as_request(&self) -> Result<LogSearchRequest, (StatusCode, Json<ErrorResponse>)> {
        Ok(LogSearchRequest {
            start_unix_nano: self.start_unix_nano,
            end_unix_nano: self.end_unix_nano,
            service_name: self.service_name.clone(),
            severity_text: self.severity_text.clone(),
            field_filters: self
                .field_filter
                .iter()
                .map(|value| parse_field_filter(value))
                .collect::<Result<Vec<_>, _>>()?,
            limit: self.limit.unwrap_or(20),
        })
    }
}

impl TempoQueryMetrics {
    fn empty() -> Self {
        Self {
            inspected_traces: None,
            inspected_bytes: "0".to_string(),
            total_blocks: None,
        }
    }
}

impl ParsedTempoSearchQuery {
    fn matches_rows(&self, rows: &[TraceSpanRow]) -> bool {
        self.substring_filters.iter().all(|(field_name, expected)| {
            let expected = expected.to_ascii_lowercase();
            rows.iter()
                .filter_map(|row| tempo_field_value(row, field_name))
                .any(|value| value.to_ascii_lowercase().contains(&expected))
        })
    }
}

impl TempoSearchQuery {
    fn parse(&self) -> Result<ParsedTempoSearchQuery, (StatusCode, Json<ErrorResponse>)> {
        let limit = self.limit.unwrap_or(20);
        let start_unix_nano = self
            .start
            .map(|value| value.saturating_mul(1_000_000_000))
            .unwrap_or(0);
        let end_unix_nano = self
            .end
            .map(|value| value.saturating_mul(1_000_000_000))
            .unwrap_or(i64::MAX);

        if let Some(traceql_query) = self.q.as_ref().filter(|value| !value.trim().is_empty()) {
            let field_filters = parse_simple_traceql_filters(traceql_query)?;
            return Ok(ParsedTempoSearchQuery {
                search_request: TraceSearchRequest {
                    start_unix_nano,
                    end_unix_nano,
                    service_name: None,
                    operation_name: None,
                    field_filters,
                    limit: usize::MAX,
                },
                limit,
                substring_filters: Vec::new(),
            });
        }

        let substring_filters = self
            .tags
            .as_deref()
            .map(parse_tempo_tags_filter)
            .transpose()?
            .unwrap_or_default();

        Ok(ParsedTempoSearchQuery {
            search_request: TraceSearchRequest {
                start_unix_nano,
                end_unix_nano,
                service_name: None,
                operation_name: None,
                field_filters: Vec::new(),
                limit: usize::MAX,
            },
            limit,
            substring_filters,
        })
    }
}

impl JaegerTraceQuery {
    fn required_service_name(&self) -> Result<String, (StatusCode, Json<ErrorResponse>)> {
        self.service
            .as_ref()
            .filter(|value| !value.is_empty())
            .cloned()
            .ok_or_else(|| bad_request("service name is required"))
    }

    fn checked_limit(&self) -> Result<usize, (StatusCode, Json<ErrorResponse>)> {
        let limit = self.limit.unwrap_or(20);
        if limit > MAX_JAEGER_LIMIT {
            return Err(bad_request(format!(
                "limit should be not higher than {MAX_JAEGER_LIMIT}"
            )));
        }
        Ok(limit)
    }

    fn parsed_tag_filters(&self) -> Result<Vec<(String, String)>, (StatusCode, Json<ErrorResponse>)>
    {
        parse_jaeger_tags_filter(self.tags.as_deref())
    }

    fn min_duration_nanos(&self) -> Result<Option<i64>, (StatusCode, Json<ErrorResponse>)> {
        self.min_duration
            .as_deref()
            .map(parse_jaeger_duration)
            .transpose()
    }

    fn max_duration_nanos(&self) -> Result<Option<i64>, (StatusCode, Json<ErrorResponse>)> {
        self.max_duration
            .as_deref()
            .map(parse_jaeger_duration)
            .transpose()
    }

    fn trace_search_request(
        &self,
        service_name: String,
        fetch_limit: usize,
        tag_filters: &[(String, String)],
    ) -> Result<TraceSearchRequest, (StatusCode, Json<ErrorResponse>)> {
        Ok(TraceSearchRequest {
            start_unix_nano: self.start_unix_nano(),
            end_unix_nano: self.end_unix_nano(),
            service_name: Some(service_name),
            operation_name: self.operation.clone(),
            field_filters: tag_filters
                .iter()
                .map(|(name, value)| FieldFilter {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            limit: fetch_limit,
        })
    }

    fn search_query(
        &self,
        service_name: String,
        fetch_limit: usize,
        tag_filters: &[(String, String)],
    ) -> Result<SearchQuery, (StatusCode, Json<ErrorResponse>)> {
        Ok(SearchQuery {
            start_unix_nano: self.start_unix_nano(),
            end_unix_nano: self.end_unix_nano(),
            service_name: Some(service_name),
            operation_name: self.operation.clone(),
            field_filter: tag_filters
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect(),
            limit: Some(fetch_limit),
        })
    }

    fn fetch_limit(
        &self,
        requested_limit: usize,
        tag_filters: &[(String, String)],
        min_duration_nanos: Option<i64>,
        max_duration_nanos: Option<i64>,
    ) -> usize {
        if !self.requires_post_filtering(tag_filters, min_duration_nanos, max_duration_nanos) {
            return requested_limit;
        }

        requested_limit
            .saturating_mul(JAEGER_POST_FILTER_FETCH_MULTIPLIER)
            .clamp(requested_limit, MAX_JAEGER_TRACE_FETCH_LIMIT)
    }

    fn requires_post_filtering(
        &self,
        tag_filters: &[(String, String)],
        min_duration_nanos: Option<i64>,
        max_duration_nanos: Option<i64>,
    ) -> bool {
        self.operation.is_some()
            || !tag_filters.is_empty()
            || min_duration_nanos.is_some()
            || max_duration_nanos.is_some()
    }

    fn start_unix_nano(&self) -> i64 {
        self.start
            .map(|value| value.saturating_mul(1_000))
            .unwrap_or(0)
    }

    fn end_unix_nano(&self) -> i64 {
        self.end
            .map(|value| value.saturating_mul(1_000))
            .unwrap_or(i64::MAX)
    }
}

impl JaegerDependenciesQuery {
    fn trace_scan_limit(&self) -> usize {
        MAX_JAEGER_DEPENDENCIES_TRACE_SCAN_LIMIT
    }

    fn end_unix_nano(&self) -> i64 {
        self.end_ts
            .unwrap_or_else(|| system_time_to_unix_millis(SystemTime::now()).unwrap_or(0) as i64)
            .saturating_mul(1_000_000)
    }

    fn start_unix_nano(&self) -> i64 {
        self.end_unix_nano().saturating_sub(
            self.lookback
                .unwrap_or(DEFAULT_JAEGER_DEPENDENCIES_LOOKBACK_MILLIS)
                .saturating_mul(1_000_000),
        )
    }

    fn trace_search_request(&self) -> TraceSearchRequest {
        TraceSearchRequest {
            start_unix_nano: self.start_unix_nano(),
            end_unix_nano: self.end_unix_nano(),
            service_name: None,
            operation_name: None,
            field_filters: Vec::new(),
            limit: self.trace_scan_limit(),
        }
    }

    fn search_query(&self) -> SearchQuery {
        SearchQuery {
            start_unix_nano: self.start_unix_nano(),
            end_unix_nano: self.end_unix_nano(),
            service_name: None,
            operation_name: None,
            field_filter: Vec::new(),
            limit: Some(self.trace_scan_limit()),
        }
    }
}

impl From<TraceSpanRow> for TraceRowResponse {
    fn from(row: TraceSpanRow) -> Self {
        let fields = row
            .fields
            .iter()
            .map(|field| (field.name.to_string(), field.value.to_string()))
            .collect();

        Self {
            trace_id: row.trace_id,
            span_id: row.span_id,
            parent_span_id: row.parent_span_id,
            name: row.name,
            start_unix_nano: row.start_unix_nano,
            end_unix_nano: row.end_unix_nano,
            time_unix_nano: row.time_unix_nano,
            fields,
        }
    }
}

impl From<TraceRowResponse> for TraceSpanRow {
    fn from(row: TraceRowResponse) -> Self {
        Self {
            trace_id: row.trace_id,
            span_id: row.span_id,
            parent_span_id: row.parent_span_id,
            name: row.name,
            start_unix_nano: row.start_unix_nano,
            end_unix_nano: row.end_unix_nano,
            time_unix_nano: row.time_unix_nano,
            fields: row
                .fields
                .into_iter()
                .map(|(name, value)| vtcore::Field {
                    name: name.into(),
                    value: value.into(),
                })
            .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jaeger_dependencies_query_applies_hard_trace_scan_cap() {
        let query = JaegerDependenciesQuery {
            end_ts: Some(1_717_000_000_000),
            lookback: Some(DEFAULT_JAEGER_DEPENDENCIES_LOOKBACK_MILLIS),
        };

        assert!(query.trace_search_request().limit < usize::MAX);
        assert!(query.search_query().limit.unwrap_or(usize::MAX) < usize::MAX);
    }
}
