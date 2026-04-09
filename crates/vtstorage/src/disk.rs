use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufWriter, Cursor, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::mpsc,
    sync::Arc,
    thread,
    time::{Duration, Instant},
};

use parking_lot::{Mutex, RwLock};
use roaring::RoaringBitmap;
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use vtcore::{
    decode_log_row, decode_trace_row, encode_log_row, encode_trace_block_rows_packed, LogRow,
    LogSearchRequest, TraceBlock, TraceBlockBuilder, TraceSearchHit, TraceSearchRequest,
    TraceSpanRow, TraceWindow,
};

use crate::log_state::LogIndexedState;
use crate::{StorageEngine, StorageError, StorageStatsSnapshot, TraceBatchPayloadMode};

const SEGMENTS_DIRNAME: &str = "segments";
const SEGMENT_PREFIX: &str = "segment-";
const LEGACY_ROWS_SUFFIX: &str = ".rows";
const WAL_SUFFIX: &str = ".wal";
const PART_SUFFIX: &str = ".part";
const META_SUFFIX: &str = ".meta.json";
const META_BINARY_SUFFIX: &str = ".meta.bin";
const RECOVERY_MANIFEST_FILENAME: &str = "trace-index.manifest";
const RECOVERY_SHARD_FILENAME_PREFIX: &str = "trace-index-shard-";
const RECOVERY_SHARD_FILENAME_SUFFIX: &str = ".bin";
const SEGMENT_META_BINARY_MAGIC: &[u8] = b"VTMETA2";
const SEGMENT_META_BINARY_VERSION: u8 = 2;
const RECOVERY_MANIFEST_MAGIC: &[u8] = b"VTRMAN1";
const RECOVERY_MANIFEST_VERSION: u8 = 3;
const RECOVERY_SHARD_MAGIC: &[u8] = b"VTRSHD1";
const RECOVERY_SHARD_VERSION: u8 = 3;
const SNAPSHOT_COMPRESSION_ZSTD: u8 = 1;
const SNAPSHOT_ZSTD_LEVEL: i32 = 3;
const META_COMPRESSION_ZSTD: u8 = 1;
const META_ZSTD_LEVEL: i32 = 3;
const WAL_MAGIC: &[u8] = b"VTWAL1";
const WAL_VERSION_JSON: u8 = 1;
const WAL_VERSION_BINARY: u8 = 2;
const WAL_VERSION_BATCHED_BINARY: u8 = 3;
const LOG_WAL_MAGIC: &[u8] = b"VTLOGW1";
const LOG_WAL_VERSION_BINARY: u8 = 1;
const PART_MAGIC: &[u8] = b"VTPART1";
const PART_VERSION: u8 = 2;
const LENGTH_PREFIX_BYTES: u64 = 4;
const WAL_CHECKSUM_BYTES: u64 = 4;
const WAL_BATCH_ROW_COUNT_BYTES: u64 = 4;
const WAL_HEADER_BYTES: u64 = (WAL_MAGIC.len() + 1) as u64;
const LOG_WAL_HEADER_BYTES: u64 = (LOG_WAL_MAGIC.len() + 1) as u64;
const DEFAULT_TARGET_SEGMENT_SIZE_BYTES: u64 = 8 * 1024 * 1024;
const COMPACTION_TARGET_MULTIPLIER: u64 = 16;
const TRACE_WAL_WRITER_CAPACITY_BYTES: usize = 1024 * 1024;
const DEFAULT_TRACE_COMBINER_WAIT: Duration = Duration::ZERO;
const DEFAULT_TRACE_GROUP_COMMIT_WAIT: Duration = Duration::ZERO;
const TRACE_COMBINER_WAIT_POLL_SLICE: Duration = Duration::from_micros(25);
const PART_FIELD_TYPE_STRING: u8 = 0;
const PART_FIELD_TYPE_I64: u8 = 1;
const PART_FIELD_TYPE_BOOL: u8 = 2;

type TraceRef = u32;
type StringRef = u32;
type SmallArcPairList = SmallVec<[(Arc<str>, Arc<str>); 8]>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskSyncPolicy {
    None,
    Data,
}

impl Default for DiskSyncPolicy {
    fn default() -> Self {
        Self::None
    }
}

impl DiskSyncPolicy {
    fn requires_flush(self) -> bool {
        !matches!(self, Self::None)
    }

    fn requires_sync(self) -> bool {
        matches!(self, Self::Data)
    }
}

#[derive(Debug, Clone)]
pub struct DiskStorageConfig {
    target_segment_size_bytes: u64,
    sync_policy: DiskSyncPolicy,
    trace_shards: usize,
    trace_combiner_wait: Duration,
    trace_group_commit_wait: Duration,
    trace_wal_enabled: bool,
    trace_wal_writer_capacity_bytes: usize,
    trace_deferred_wal_writes: bool,
    trace_seal_worker_count: usize,
}

impl Default for DiskStorageConfig {
    fn default() -> Self {
        Self {
            target_segment_size_bytes: DEFAULT_TARGET_SEGMENT_SIZE_BYTES,
            sync_policy: DiskSyncPolicy::None,
            trace_shards: default_trace_shards(),
            trace_combiner_wait: DEFAULT_TRACE_COMBINER_WAIT,
            trace_group_commit_wait: DEFAULT_TRACE_GROUP_COMMIT_WAIT,
            trace_wal_enabled: true,
            trace_wal_writer_capacity_bytes: TRACE_WAL_WRITER_CAPACITY_BYTES,
            trace_deferred_wal_writes: false,
            trace_seal_worker_count: 1,
        }
    }
}

impl DiskStorageConfig {
    pub fn with_target_segment_size_bytes(mut self, target_segment_size_bytes: u64) -> Self {
        self.target_segment_size_bytes = target_segment_size_bytes.max(1);
        self
    }

    pub fn with_sync_policy(mut self, sync_policy: DiskSyncPolicy) -> Self {
        self.sync_policy = sync_policy;
        self
    }

    pub fn with_trace_shards(mut self, trace_shards: usize) -> Self {
        self.trace_shards = trace_shards.max(1);
        self
    }

    pub fn with_trace_combiner_wait(mut self, trace_combiner_wait: Duration) -> Self {
        self.trace_combiner_wait = trace_combiner_wait;
        self
    }

    pub fn with_trace_group_commit_wait(mut self, trace_group_commit_wait: Duration) -> Self {
        self.trace_group_commit_wait = trace_group_commit_wait;
        self
    }

    pub fn with_trace_wal_enabled(mut self, trace_wal_enabled: bool) -> Self {
        self.trace_wal_enabled = trace_wal_enabled;
        self
    }

    pub fn with_trace_wal_writer_capacity_bytes(
        mut self,
        trace_wal_writer_capacity_bytes: usize,
    ) -> Self {
        self.trace_wal_writer_capacity_bytes = trace_wal_writer_capacity_bytes.max(1);
        self
    }

    pub fn with_trace_deferred_wal_writes(mut self, trace_deferred_wal_writes: bool) -> Self {
        self.trace_deferred_wal_writes = trace_deferred_wal_writes;
        self
    }

    pub fn with_trace_seal_worker_count(mut self, trace_seal_worker_count: usize) -> Self {
        self.trace_seal_worker_count = trace_seal_worker_count.max(1);
        self
    }
}

#[derive(Debug)]
pub struct DiskStorageEngine {
    root_path: PathBuf,
    segments_path: PathBuf,
    segment_paths: Arc<RwLock<FxHashMap<u64, PathBuf>>>,
    trace_shards: Vec<Arc<RwLock<DiskTraceShardState>>>,
    append_combiners: Vec<Mutex<DiskTraceAppendCombinerState>>,
    append_combiner_stats: Vec<DiskTraceAppendCombinerStats>,
    active_segments: Vec<Mutex<Option<ActiveSegment>>>,
    sealing_segments: Vec<Arc<RwLock<Vec<Arc<HeadSegmentSnapshot>>>>>,
    log_state: RwLock<LogIndexedState>,
    log_writer: Mutex<BufWriter<File>>,
    next_segment_id: AtomicU64,
    config: DiskStorageConfig,
    persisted_bytes: Arc<AtomicU64>,
    trace_window_lookups: AtomicU64,
    row_queries: AtomicU64,
    segment_read_batches: AtomicU64,
    part_selective_decodes: AtomicU64,
    fsync_operations: Arc<AtomicU64>,
    head_metrics: Arc<DiskTraceHeadMetrics>,
    field_column_counts: Arc<DiskFieldColumnCounts>,
}

#[derive(Debug, Clone, Default)]
struct DiskTraceShardState {
    trace_ids: StringTable,
    strings: StringTable,
    all_trace_refs: RoaringBitmap,
    windows_by_trace: FxHashMap<TraceRef, TraceWindowBounds>,
    persisted_segments: FxHashMap<u64, PersistedSegmentSummary>,
    services_by_trace: FxHashMap<TraceRef, Vec<StringRef>>,
    trace_refs_by_service: FxHashMap<StringRef, RoaringBitmap>,
    trace_refs_by_operation: FxHashMap<StringRef, RoaringBitmap>,
    trace_refs_by_field_name_value: FxHashMap<StringRef, FxHashMap<StringRef, RoaringBitmap>>,
    field_values_by_name: FxHashMap<StringRef, Vec<StringRef>>,
    row_refs_by_trace: FxHashMap<TraceRef, Vec<SegmentRowLocation>>,
    rows_ingested: u64,
}

#[derive(Debug, Clone, Default)]
struct StringTable {
    ids_by_value: FxHashMap<Arc<str>, u32>,
    values_by_id: Vec<Arc<str>>,
}

#[derive(Debug, Clone, Copy)]
struct TraceWindowBounds {
    start_unix_nano: i64,
    end_unix_nano: i64,
}

#[derive(Debug, Clone)]
struct PersistedSegmentSummary {
    window: TraceWindowBounds,
    services: Vec<StringRef>,
    operations: Vec<StringRef>,
    field_values_by_name: FxHashMap<StringRef, Vec<StringRef>>,
    traces: Vec<PersistedTraceSummary>,
}

#[derive(Debug, Clone)]
struct PersistedTraceSummary {
    trace_ref: TraceRef,
    window: TraceWindowBounds,
    services: Vec<StringRef>,
    operations: Vec<StringRef>,
    field_values_by_name: FxHashMap<StringRef, Vec<StringRef>>,
}

impl StringTable {
    fn intern(&mut self, value: &str) -> u32 {
        if let Some(existing) = self.ids_by_value.get(value) {
            return *existing;
        }

        let id = self.values_by_id.len() as u32;
        let owned = Arc::<str>::from(value);
        self.values_by_id.push(owned.clone());
        self.ids_by_value.insert(owned, id);
        id
    }

    fn lookup(&self, value: &str) -> Option<u32> {
        self.ids_by_value.get(value).copied()
    }

    fn resolve(&self, id: u32) -> Option<&str> {
        self.values_by_id
            .get(id as usize)
            .map(|value| value.as_ref())
    }

    fn from_values(values: Vec<String>) -> Self {
        let mut ids_by_value = FxHashMap::default();
        let mut values_by_id = Vec::with_capacity(values.len());
        for (id, value) in values.into_iter().enumerate() {
            let value = Arc::<str>::from(value);
            ids_by_value.insert(value.clone(), id as u32);
            values_by_id.push(value);
        }
        Self {
            ids_by_value,
            values_by_id,
        }
    }
}

impl TraceWindowBounds {
    fn new(start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            start_unix_nano,
            end_unix_nano,
        }
    }

    fn observe(&mut self, start_unix_nano: i64, end_unix_nano: i64) {
        if start_unix_nano < self.start_unix_nano {
            self.start_unix_nano = start_unix_nano;
        }
        if end_unix_nano > self.end_unix_nano {
            self.end_unix_nano = end_unix_nano;
        }
    }
}

#[derive(Debug, Default)]
struct RecoveredDiskState {
    segment_paths: FxHashMap<u64, PathBuf>,
    trace_shards: Vec<DiskTraceShardState>,
    persisted_bytes: u64,
    typed_field_columns: u64,
    string_field_columns: u64,
    next_segment_id: u64,
    skip_startup_compaction: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RecoveryCoveredSegment {
    segment_id: u64,
    kind: RecoveryCoveredSegmentKind,
}

impl RecoveryCoveredSegment {
    fn expected_path(self, segments_path: &Path) -> PathBuf {
        self.kind.expected_path(segments_path, self.segment_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryCoveredSegmentKind {
    Part,
    Wal,
    LegacyRows,
}

impl RecoveryCoveredSegmentKind {
    fn expected_path(self, segments_path: &Path, segment_id: u64) -> PathBuf {
        match self {
            Self::Part => segment_part_path(segments_path, segment_id),
            Self::Wal => segment_wal_path(segments_path, segment_id),
            Self::LegacyRows => segment_row_file_path(segments_path, segment_id),
        }
    }

    fn to_u8(self) -> u8 {
        match self {
            Self::Part => 0,
            Self::Wal => 1,
            Self::LegacyRows => 2,
        }
    }

    fn from_u8(value: u8) -> Result<Self, StorageError> {
        match value {
            0 => Ok(Self::Part),
            1 => Ok(Self::Wal),
            2 => Ok(Self::LegacyRows),
            _ => Err(invalid_data("unsupported recovery covered segment kind").into()),
        }
    }
}

#[derive(Debug)]
struct RecoverySnapshot {
    covered_segments: Vec<RecoveryCoveredSegment>,
    trace_shards: Vec<DiskTraceShardState>,
    persisted_bytes: u64,
    typed_field_columns: u64,
    string_field_columns: u64,
    next_segment_id: u64,
}

impl RecoverySnapshot {
    fn into_recovered_state(self, segments_path: &Path) -> RecoveredDiskState {
        let mut segment_paths = FxHashMap::default();
        for covered_segment in self.covered_segments {
            segment_paths.insert(
                covered_segment.segment_id,
                covered_segment.expected_path(segments_path),
            );
        }
        RecoveredDiskState {
            segment_paths,
            trace_shards: self.trace_shards,
            persisted_bytes: self.persisted_bytes,
            typed_field_columns: self.typed_field_columns,
            string_field_columns: self.string_field_columns,
            next_segment_id: self.next_segment_id,
            skip_startup_compaction: false,
        }
    }
}

#[derive(Debug)]
struct RecoveryManifest {
    covered_segments: Vec<RecoveryCoveredSegment>,
    shards: Vec<RecoveryShardManifestEntry>,
    persisted_bytes: u64,
    typed_field_columns: u64,
    string_field_columns: u64,
    next_segment_id: u64,
}

#[derive(Debug, Clone)]
struct RecoveryShardManifestEntry {
    shard_index: usize,
    checksum: u32,
    compressed_bytes: u64,
    uncompressed_bytes: u64,
}

#[derive(Debug)]
struct ActiveSegment {
    segment_id: u64,
    wal_path: PathBuf,
    wal_sink: TraceWalSink,
    accumulator: SegmentAccumulator,
    cached_row_length_prefixes: Vec<u8>,
    cached_payload_ranges: Vec<std::ops::Range<usize>>,
    cached_payload_bytes: Vec<u8>,
}

#[derive(Debug)]
enum TraceWalSink {
    Immediate(BufWriter<File>),
    Deferred(Vec<u8>),
    Disabled,
}

#[derive(Debug)]
struct HeadSegmentSnapshot {
    shard_index: usize,
    segment_id: u64,
    wal_path: PathBuf,
    wal_enabled: bool,
    staged_wal_bytes: Vec<u8>,
    accumulator: SegmentAccumulator,
    cached_payload_ranges: Vec<std::ops::Range<usize>>,
    cached_payload_bytes: Vec<u8>,
}

#[derive(Debug, Default)]
struct DiskTraceHeadMetrics {
    group_commit_flushes: AtomicU64,
    group_commit_rows: AtomicU64,
    group_commit_bytes: AtomicU64,
    seal_queue_depth: AtomicU64,
    seal_completed: AtomicU64,
    seal_rows: AtomicU64,
    seal_bytes: AtomicU64,
}

#[derive(Debug, Default)]
struct DiskFieldColumnCounts {
    typed: AtomicU64,
    string: AtomicU64,
}

fn persist_snapshot_wal(snapshot: &HeadSegmentSnapshot) -> Result<(), StorageError> {
    if snapshot.staged_wal_bytes.is_empty() {
        return Ok(());
    }
    let mut wal_file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&snapshot.wal_path)?;
    wal_file.write_all(&snapshot.staged_wal_bytes)?;
    wal_file.flush()?;
    Ok(())
}

#[derive(Debug, Default)]
struct SegmentAccumulator {
    row_count: u64,
    persisted_bytes: u64,
    min_time_unix_nano: Option<i64>,
    max_time_unix_nano: Option<i64>,
    trace_ids: StringTable,
    strings: StringTable,
    traces: FxHashMap<TraceRef, SegmentTraceAccumulator>,
}

#[derive(Debug)]
struct SegmentTraceAccumulator {
    window: TraceWindowBounds,
    services: Vec<StringRef>,
    operations: Vec<StringRef>,
    fields: Vec<(StringRef, StringRef)>,
    rows: Vec<SegmentRowLocation>,
}

impl SegmentTraceAccumulator {
    fn new(start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            window: TraceWindowBounds::new(start_unix_nano, end_unix_nano),
            services: Vec::new(),
            operations: Vec::new(),
            fields: Vec::new(),
            rows: Vec::new(),
        }
    }
}

#[derive(Debug, Default)]
struct TraceAppendRowsResult {
    bytes_written: u64,
}

#[derive(Default)]
struct BatchInternCache<'a> {
    trace_ids: FxHashMap<&'a str, TraceRef>,
    strings: FxHashMap<&'a str, StringRef>,
}

impl<'a> BatchInternCache<'a> {
    fn trace_ref(&mut self, table: &mut StringTable, value: &'a str) -> TraceRef {
        if let Some(existing) = self.trace_ids.get(value) {
            return *existing;
        }
        let trace_ref = table.intern(value);
        self.trace_ids.insert(value, trace_ref);
        trace_ref
    }

    fn string_ref(&mut self, table: &mut StringTable, value: &'a str) -> StringRef {
        if let Some(existing) = self.strings.get(value) {
            return *existing;
        }
        let string_ref = table.intern(value);
        self.strings.insert(value, string_ref);
        string_ref
    }
}

#[derive(Debug)]
struct DiskTraceAppendRequest {
    trace_blocks: Vec<TraceBlock>,
    input_blocks: usize,
    row_count: usize,
    enqueued_at: Instant,
    result_tx: mpsc::Sender<Result<(), String>>,
}

#[derive(Debug, Default)]
struct DiskTraceAppendCombinerState {
    pending: VecDeque<DiskTraceAppendRequest>,
    combining: bool,
}

#[derive(Debug, Default)]
struct DiskTraceAppendCombinerStats {
    queue_depth: AtomicU64,
    max_queue_depth: AtomicU64,
    flushes: AtomicU64,
    rows: AtomicU64,
    input_blocks: AtomicU64,
    output_blocks: AtomicU64,
    wait_micros_total: AtomicU64,
    wait_micros_max: AtomicU64,
    flush_due_to_wait: AtomicU64,
}

impl DiskTraceAppendCombinerStats {
    fn record_enqueue(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        let _ =
            self.max_queue_depth
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (depth > current).then_some(depth)
                });
    }

    fn record_dequeue(&self, count: usize) {
        self.queue_depth.fetch_sub(count as u64, Ordering::Relaxed);
    }

    fn record_flush(&self, rows: usize, input_blocks: usize, wait_micros: u64) {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.input_blocks
            .fetch_add(input_blocks as u64, Ordering::Relaxed);
        self.output_blocks.fetch_add(1, Ordering::Relaxed);
        self.wait_micros_total
            .fetch_add(wait_micros, Ordering::Relaxed);
        let _ =
            self.wait_micros_max
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (wait_micros > current).then_some(wait_micros)
                });
        self.flush_due_to_wait.fetch_add(1, Ordering::Relaxed);
    }
}

fn saturating_micros(value: u128) -> u64 {
    value.min(u64::MAX as u128) as u64
}

#[derive(Debug)]
struct PreparedBlockRowMetadata {
    trace_id: Arc<str>,
    shard_index: usize,
    start_unix_nano: i64,
    end_unix_nano: i64,
    shared_group_index: Option<usize>,
    service: Option<Arc<str>>,
    operation: Option<Arc<str>>,
    indexed_fields: SmallArcPairList,
}

#[derive(Debug, Default)]
struct PreparedSharedGroupMetadata {
    service: Option<Arc<str>>,
    indexed_fields: SmallArcPairList,
}

#[derive(Debug)]
struct PreparedTraceBlockAppend {
    shard_index: usize,
    prepared_shared_groups: Vec<PreparedSharedGroupMetadata>,
    prepared_rows: Vec<PreparedBlockRowMetadata>,
    encoded_row_bytes: Vec<u8>,
    encoded_row_ranges: Vec<std::ops::Range<usize>>,
}

impl PreparedTraceBlockAppend {
    fn new(block: TraceBlock, trace_shards: usize) -> Self {
        let prepared_shared_groups = prepare_shared_group_metadata(&block);
        let prepared_rows =
            prepare_block_row_metadata(&block, &prepared_shared_groups, trace_shards);
        let shard_index = prepared_rows
            .first()
            .map_or(0, |prepared_row| prepared_row.shard_index);
        debug_assert!(prepared_rows
            .iter()
            .all(|prepared_row| prepared_row.shard_index == shard_index));
        let (encoded_row_bytes, encoded_row_ranges) = encode_trace_block_rows_packed(&block);
        Self {
            shard_index,
            prepared_shared_groups,
            prepared_rows,
            encoded_row_bytes,
            encoded_row_ranges,
        }
    }
}

#[derive(Debug)]
struct PreparedTraceShardBatch {
    shard_index: usize,
    prepared_shared_groups: Vec<PreparedSharedGroupMetadata>,
    prepared_rows: Vec<PreparedBlockRowMetadata>,
    encoded_row_bytes: Vec<u8>,
    encoded_row_ranges: Vec<std::ops::Range<usize>>,
}

impl PreparedTraceShardBatch {
    fn from_prepared_blocks(prepared_blocks: Vec<PreparedTraceBlockAppend>) -> Self {
        let mut prepared_blocks = prepared_blocks.into_iter();
        let first = prepared_blocks
            .next()
            .expect("prepared shard batch requires at least one block");
        let mut batch = Self {
            shard_index: first.shard_index,
            prepared_shared_groups: first.prepared_shared_groups,
            prepared_rows: first.prepared_rows,
            encoded_row_bytes: first.encoded_row_bytes,
            encoded_row_ranges: first.encoded_row_ranges,
        };

        for block in prepared_blocks {
            debug_assert_eq!(block.shard_index, batch.shard_index);
            let shared_group_base = batch.prepared_shared_groups.len();
            let encoded_byte_base = batch.encoded_row_bytes.len();
            batch
                .prepared_shared_groups
                .extend(block.prepared_shared_groups.into_iter());
            batch
                .prepared_rows
                .extend(block.prepared_rows.into_iter().map(|mut prepared_row| {
                    if let Some(shared_group_index) = prepared_row.shared_group_index {
                        prepared_row.shared_group_index =
                            Some(shared_group_index + shared_group_base);
                    }
                    prepared_row
                }));
            batch.encoded_row_ranges.extend(
                block.encoded_row_ranges.into_iter().map(|range| {
                    (range.start + encoded_byte_base)..(range.end + encoded_byte_base)
                }),
            );
            batch.encoded_row_bytes.extend(block.encoded_row_bytes);
        }

        batch
    }
}

fn coalesce_trace_blocks_for_shard(blocks: Vec<TraceBlock>) -> TraceBlock {
    let mut blocks = blocks.into_iter();
    let first = blocks
        .next()
        .expect("coalesced shard block requires at least one input block");
    let mut total_rows = first.row_count();
    let mut total_fields = first.stored_field_count();
    let mut remaining = Vec::new();
    for block in blocks {
        total_rows += block.row_count();
        total_fields += block.stored_field_count();
        remaining.push(block);
    }
    if remaining.is_empty() {
        return first;
    }

    let mut builder = TraceBlockBuilder::with_capacity(total_rows);
    builder.reserve_fields(total_fields);
    builder.extend_block(first);
    for block in remaining {
        builder.extend_block(block);
    }
    builder.finish()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentMeta {
    segment_id: u64,
    segment_file: String,
    row_count: u64,
    persisted_bytes: u64,
    min_time_unix_nano: i64,
    max_time_unix_nano: i64,
    #[serde(default)]
    typed_field_columns: u64,
    #[serde(default)]
    string_field_columns: u64,
    services: Vec<String>,
    traces: Vec<SegmentTraceMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentTraceMeta {
    trace_id: String,
    start_unix_nano: i64,
    end_unix_nano: i64,
    services: Vec<String>,
    #[serde(default)]
    operations: Vec<String>,
    #[serde(default)]
    fields: HashMap<String, Vec<String>>,
    rows: Vec<SegmentRowLocation>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
struct SegmentRowLocation {
    segment_id: u32,
    row_index: u32,
    start_unix_nano: i64,
    end_unix_nano: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ColumnarPart {
    row_count: u32,
    trace_ids: DictionaryStringColumn,
    span_ids: DictionaryStringColumn,
    parent_span_ids: DictionaryOptionalStringColumn,
    names: DictionaryStringColumn,
    start_unix_nanos: Vec<i64>,
    end_unix_nanos: Vec<i64>,
    time_unix_nanos: Vec<i64>,
    field_columns: Vec<PartFieldColumn>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PartFieldColumn {
    name: String,
    values: PartFieldColumnValues,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum PartFieldColumnValues {
    String(DictionaryOptionalStringColumn),
    I64(OptionalI64Column),
    Bool(OptionalBoolColumn),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DictionaryStringColumn {
    dictionary: Vec<String>,
    indexes: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DictionaryOptionalStringColumn {
    dictionary: Vec<String>,
    indexes: Vec<Option<u32>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OptionalI64Column {
    values: Vec<Option<i64>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OptionalBoolColumn {
    values: Vec<Option<bool>>,
}

#[derive(Debug, Default)]
struct SegmentFileSet {
    wal_path: Option<PathBuf>,
    part_path: Option<PathBuf>,
    legacy_rows_path: Option<PathBuf>,
    meta_json_path: Option<PathBuf>,
    meta_bin_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct CompactableSegment {
    segment_id: u64,
    data_path: PathBuf,
    metadata_paths: Vec<PathBuf>,
    total_bytes: u64,
}

impl DiskStorageEngine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Self::open_with_config(path, DiskStorageConfig::default())
    }

    pub fn open_with_config(
        path: impl AsRef<Path>,
        config: DiskStorageConfig,
    ) -> Result<Self, StorageError> {
        let root_path = path.as_ref().to_path_buf();
        fs::create_dir_all(&root_path)?;

        let segments_path = root_path.join(SEGMENTS_DIRNAME);
        fs::create_dir_all(&segments_path)?;

        let trace_shards = config.trace_shards.max(1);
        let mut recovered = recover_state(
            &root_path,
            &segments_path,
            trace_shards,
            config.trace_seal_worker_count,
        )?;
        let log_path = root_path.join("logs.wal");
        let (log_state, log_persisted_bytes) = recover_log_state(&log_path)?;
        recovered.persisted_bytes += log_persisted_bytes;
        let mut next_segment_id = recovered.next_segment_id;
        if !recovered.skip_startup_compaction
            && compact_persisted_segments(
                &segments_path,
                &mut next_segment_id,
                compaction_target_segment_size_bytes(&config),
            )?
        {
            let compacted = recover_state(
                &root_path,
                &segments_path,
                trace_shards,
                config.trace_seal_worker_count,
            )?;
            recovered = compacted;
            recovered.persisted_bytes += log_persisted_bytes;
            next_segment_id = next_segment_id.max(recovered.next_segment_id);
        }
        persist_recovery_manifest_file(&root_path, &recovered)?;
        let active_segments = (0..trace_shards)
            .map(|_| Mutex::new(None))
            .collect::<Vec<_>>();
        let sealing_segments = (0..trace_shards)
            .map(|_| Arc::new(RwLock::new(Vec::new())))
            .collect::<Vec<_>>();
        let append_combiners = (0..trace_shards)
            .map(|_| Mutex::new(DiskTraceAppendCombinerState::default()))
            .collect::<Vec<_>>();
        let append_combiner_stats = (0..trace_shards)
            .map(|_| DiskTraceAppendCombinerStats::default())
            .collect::<Vec<_>>();
        let segment_paths = Arc::new(RwLock::new(recovered.segment_paths));
        let trace_shards = recovered
            .trace_shards
            .into_iter()
            .map(|shard| Arc::new(RwLock::new(shard)))
            .collect::<Vec<_>>();
        let persisted_bytes = Arc::new(AtomicU64::new(recovered.persisted_bytes));
        let fsync_operations = Arc::new(AtomicU64::new(0));
        let head_metrics = Arc::new(DiskTraceHeadMetrics::default());
        let field_column_counts = Arc::new(DiskFieldColumnCounts {
            typed: AtomicU64::new(recovered.typed_field_columns),
            string: AtomicU64::new(recovered.string_field_columns),
        });
        let log_writer = BufWriter::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&log_path)?,
        );

        Ok(Self {
            root_path,
            segments_path,
            segment_paths,
            trace_shards,
            append_combiners,
            append_combiner_stats,
            active_segments,
            sealing_segments,
            log_state: RwLock::new(log_state),
            log_writer: Mutex::new(log_writer),
            next_segment_id: AtomicU64::new(next_segment_id),
            config,
            persisted_bytes,
            trace_window_lookups: AtomicU64::new(0),
            row_queries: AtomicU64::new(0),
            segment_read_batches: AtomicU64::new(0),
            part_selective_decodes: AtomicU64::new(0),
            fsync_operations,
            head_metrics,
            field_column_counts,
        })
    }

    pub fn data_path(&self) -> &Path {
        &self.root_path
    }

    fn ensure_active_segment<'a>(
        &self,
        active_segment: &'a mut Option<ActiveSegment>,
    ) -> Result<&'a mut ActiveSegment, StorageError> {
        if active_segment.is_none() {
            let next_segment_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
            *active_segment = Some(ActiveSegment::open(
                &self.segments_path,
                next_segment_id,
                self.config.trace_wal_enabled,
                self.config.trace_wal_writer_capacity_bytes,
                self.config.trace_deferred_wal_writes,
            )?);
        }
        Ok(active_segment
            .as_mut()
            .expect("active segment should be initialized"))
    }

    fn group_commit_active_segment(
        &self,
        active_segment: &mut ActiveSegment,
        row_count: usize,
        bytes_written: u64,
    ) -> Result<(), StorageError> {
        if self.config.sync_policy.requires_flush() {
            active_segment.flush()?;
        }
        self.head_metrics
            .group_commit_flushes
            .fetch_add(1, Ordering::Relaxed);
        self.head_metrics
            .group_commit_rows
            .fetch_add(row_count as u64, Ordering::Relaxed);
        self.head_metrics
            .group_commit_bytes
            .fetch_add(bytes_written, Ordering::Relaxed);
        if self.config.sync_policy.requires_sync() {
            active_segment.sync_data()?;
            self.fsync_operations.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    fn persist_rotated_head_segment(
        &self,
        snapshot: HeadSegmentSnapshot,
    ) -> Result<(), StorageError> {
        let data_path = if snapshot.wal_enabled {
            persist_snapshot_wal(&snapshot)?;
            snapshot.wal_path.clone()
        } else {
            let rows_path = segment_row_file_path(&self.segments_path, snapshot.segment_id);
            let (_, synced) = write_batched_binary_row_file(
                &rows_path,
                &snapshot.cached_payload_bytes,
                &snapshot.cached_payload_ranges,
                self.config.sync_policy,
            )?;
            if synced {
                self.fsync_operations.fetch_add(1, Ordering::Relaxed);
            }
            rows_path
        };
        let meta = snapshot.build_meta(&data_path)?;
        let typed_field_columns = meta.typed_field_columns;
        let string_field_columns = meta.string_field_columns;
        self.segment_paths
            .write()
            .insert(snapshot.segment_id, data_path);
        {
            let mut shard_state = self.trace_shards[snapshot.shard_index].write();
            shard_state.observe_persisted_segment(meta);
        }
        self.field_column_counts
            .typed
            .fetch_add(typed_field_columns, Ordering::Relaxed);
        self.field_column_counts
            .string
            .fetch_add(string_field_columns, Ordering::Relaxed);
        Ok(())
    }

    fn maybe_roll_active_segment(
        &self,
        shard_index: usize,
        active_segment_slot: &mut Option<ActiveSegment>,
    ) -> Result<(), StorageError> {
        let should_seal = active_segment_slot.as_ref().is_some_and(|active_segment| {
            !active_segment.is_empty()
                && active_segment.persisted_bytes() >= self.config.target_segment_size_bytes
        });
        if !should_seal {
            return Ok(());
        }
        let mut sealed_head = active_segment_slot
            .take()
            .expect("active segment should exist when sealing");
        sealed_head.flush()?;
        let snapshot = sealed_head.into_snapshot(shard_index);
        self.persist_rotated_head_segment(snapshot)?;
        let next_segment_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        *active_segment_slot = Some(ActiveSegment::open(
            &self.segments_path,
            next_segment_id,
            self.config.trace_wal_enabled,
            self.config.trace_wal_writer_capacity_bytes,
            self.config.trace_deferred_wal_writes,
        )?);
        Ok(())
    }

    fn flush_log_writer(&self) -> Result<(), StorageError> {
        let mut writer = self.log_writer.lock();
        writer.flush()?;
        if self.config.sync_policy.requires_sync() {
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    fn persist_recovery_snapshot(&self) -> Result<(), StorageError> {
        let segment_paths = self.segment_paths.read().clone();
        let trace_shards = self
            .trace_shards
            .iter()
            .map(|shard| shard.read().clone())
            .collect::<Vec<_>>();
        let recovered = RecoveredDiskState {
            segment_paths,
            trace_shards,
            persisted_bytes: self.persisted_bytes.load(Ordering::Relaxed),
            typed_field_columns: self.field_column_counts.typed.load(Ordering::Relaxed),
            string_field_columns: self.field_column_counts.string.load(Ordering::Relaxed),
            next_segment_id: self.next_segment_id.load(Ordering::Relaxed),
            skip_startup_compaction: false,
        };
        persist_recovery_manifest_file(&self.root_path, &recovered)
    }

    fn trace_shard_index(&self, trace_id: &str) -> usize {
        trace_shard_index(trace_id, self.trace_shards.len())
    }

    fn append_prepared_trace_rows(
        &self,
        prepared_rows: &[PreparedBlockRowMetadata],
        prepared_shared_groups: &[PreparedSharedGroupMetadata],
        encoded_row_bytes: &[u8],
        encoded_row_ranges: &[std::ops::Range<usize>],
        active_segment: &mut ActiveSegment,
    ) -> Result<TraceAppendRowsResult, StorageError> {
        active_segment.append_block_rows(
            prepared_rows,
            prepared_shared_groups,
            encoded_row_bytes,
            encoded_row_ranges,
            0,
            prepared_rows.len(),
        )
    }

    fn append_prepared_trace_shard_batch(
        &self,
        prepared_batch: &PreparedTraceShardBatch,
        active_segment: &mut ActiveSegment,
    ) -> Result<TraceAppendRowsResult, StorageError> {
        self.append_prepared_trace_rows(
            &prepared_batch.prepared_rows,
            &prepared_batch.prepared_shared_groups,
            &prepared_batch.encoded_row_bytes,
            &prepared_batch.encoded_row_ranges,
            active_segment,
        )
    }

    fn append_prepared_trace_shard_batches_once(
        &self,
        shard_index: usize,
        prepared_batches: Vec<PreparedTraceShardBatch>,
    ) -> Result<(), StorageError> {
        debug_assert!(!prepared_batches.is_empty());
        let mut active_segment_slot = self.active_segments[shard_index].lock();
        let active_segment = self.ensure_active_segment(&mut active_segment_slot)?;
        let mut total_rows = 0usize;
        let mut total_bytes = 0u64;
        for prepared_batch in &prepared_batches {
            debug_assert_eq!(prepared_batch.shard_index, shard_index);
            total_rows += prepared_batch.prepared_rows.len();
            let append_result =
                self.append_prepared_trace_shard_batch(prepared_batch, active_segment)?;
            total_bytes += append_result.bytes_written;
        }
        self.persisted_bytes
            .fetch_add(total_bytes, Ordering::Relaxed);
        self.group_commit_active_segment(active_segment, total_rows, total_bytes)?;
        self.maybe_roll_active_segment(shard_index, &mut active_segment_slot)?;
        Ok(())
    }

    fn submit_trace_shard_blocks(
        &self,
        shard_index: usize,
        trace_blocks: Vec<TraceBlock>,
    ) -> Result<(), StorageError> {
        if trace_blocks.is_empty() {
            return Ok(());
        }
        let input_blocks = trace_blocks.len();
        let row_count = trace_blocks.iter().map(TraceBlock::row_count).sum();
        self.submit_trace_shard_append_request(shard_index, trace_blocks, input_blocks, row_count)
    }

    fn submit_trace_shard_append_request(
        &self,
        shard_index: usize,
        trace_blocks: Vec<TraceBlock>,
        input_blocks: usize,
        row_count: usize,
    ) -> Result<(), StorageError> {
        let (result_tx, result_rx) = mpsc::channel();
        let request = DiskTraceAppendRequest {
            trace_blocks,
            input_blocks,
            row_count,
            enqueued_at: Instant::now(),
            result_tx,
        };
        self.append_combiner_stats[shard_index].record_enqueue();
        let should_combine = {
            let mut state = self.append_combiners[shard_index].lock();
            state.pending.push_back(request);
            if state.combining {
                false
            } else {
                state.combining = true;
                true
            }
        };
        if should_combine {
            self.run_trace_append_combiner(shard_index);
        }
        result_rx
            .recv()
            .map_err(|error| {
                StorageError::Message(format!("disk trace combiner dropped: {error}"))
            })?
            .map_err(StorageError::Message)
    }

    fn run_trace_append_combiner(&self, shard_index: usize) {
        loop {
            let Some(requests) = self.take_trace_append_requests(shard_index) else {
                return;
            };
            if self.config.trace_combiner_wait == Duration::ZERO
                && self.config.trace_group_commit_wait == Duration::ZERO
            {
                let (prepared_batch, responders) =
                    self.prepare_trace_append_batch_fast(shard_index, requests);
                let result = self
                    .append_prepared_trace_shard_batches_once(shard_index, vec![prepared_batch])
                    .map_err(|error| error.to_string());
                for responder in responders {
                    let _ = responder.send(result.clone());
                }
                continue;
            }
            let (prepared_batches, responders) =
                self.collect_prepared_trace_shard_batches_for_commit(shard_index, requests);

            let result = self
                .append_prepared_trace_shard_batches_once(shard_index, prepared_batches)
                .map_err(|error| error.to_string());
            for responder in responders {
                let _ = responder.send(result.clone());
            }
        }
    }

    fn prepare_trace_append_batch_fast(
        &self,
        shard_index: usize,
        requests: Vec<DiskTraceAppendRequest>,
    ) -> (PreparedTraceShardBatch, Vec<mpsc::Sender<Result<(), String>>>) {
        let mut earliest_enqueue = requests[0].enqueued_at;
        let mut input_blocks = 0usize;
        let mut row_count = 0usize;
        let mut responders = Vec::new();
        let mut prepared_blocks = Vec::new();
        let mut pending_requests = requests;

        loop {
            let mut trace_blocks = Vec::new();
            for request in pending_requests {
                earliest_enqueue = earliest_enqueue.min(request.enqueued_at);
                input_blocks += request.input_blocks;
                row_count += request.row_count;
                trace_blocks.extend(request.trace_blocks);
                responders.push(request.result_tx);
            }
            prepared_blocks.reserve(trace_blocks.len());
            for block in trace_blocks {
                prepared_blocks.push(PreparedTraceBlockAppend::new(
                    block,
                    self.trace_shards.len(),
                ));
            }

            pending_requests = self.take_pending_trace_append_requests(shard_index);
            if pending_requests.is_empty() {
                break;
            }
        }

        let wait_micros = saturating_micros(earliest_enqueue.elapsed().as_micros());
        self.append_combiner_stats[shard_index].record_flush(row_count, input_blocks, wait_micros);

        (
            PreparedTraceShardBatch::from_prepared_blocks(prepared_blocks),
            responders,
        )
    }

    fn collect_prepared_trace_shard_batches_for_commit(
        &self,
        shard_index: usize,
        requests: Vec<DiskTraceAppendRequest>,
    ) -> (
        Vec<PreparedTraceShardBatch>,
        Vec<mpsc::Sender<Result<(), String>>>,
    ) {
        let mut prepared_batches = Vec::new();
        let mut responders = Vec::new();
        let mut pending_requests = requests;
        let mut wait_for_additional_batch = self.config.trace_group_commit_wait > Duration::ZERO;
        let mut commit_deadline = None;

        loop {
            let (prepared_batch, batch_responders, earliest_enqueue) =
                self.prepare_trace_append_batch(shard_index, pending_requests);
            if commit_deadline.is_none() {
                commit_deadline = Some(earliest_enqueue + self.config.trace_group_commit_wait);
            }
            prepared_batches.push(prepared_batch);
            responders.extend(batch_responders);
            pending_requests = if wait_for_additional_batch {
                wait_for_additional_batch = false;
                self.take_pending_trace_append_requests_until(
                    shard_index,
                    commit_deadline.expect("commit deadline initialized"),
                )
            } else {
                self.take_pending_trace_append_requests(shard_index)
            };
            if pending_requests.is_empty() {
                break;
            }
        }

        (prepared_batches, responders)
    }

    fn prepare_trace_append_batch(
        &self,
        shard_index: usize,
        requests: Vec<DiskTraceAppendRequest>,
    ) -> (
        PreparedTraceShardBatch,
        Vec<mpsc::Sender<Result<(), String>>>,
        Instant,
    ) {
        let mut earliest_enqueue = requests[0].enqueued_at;
        let mut input_blocks = 0usize;
        let mut row_count = 0usize;
        let mut responders = Vec::new();
        let mut pending_requests = requests;
        let mut trace_blocks = Vec::new();
        let combine_deadline = earliest_enqueue + self.config.trace_combiner_wait;

        loop {
            for request in pending_requests {
                earliest_enqueue = earliest_enqueue.min(request.enqueued_at);
                input_blocks += request.input_blocks;
                row_count += request.row_count;
                trace_blocks.extend(request.trace_blocks);
                responders.push(request.result_tx);
            }
            pending_requests =
                self.take_pending_trace_append_requests_until(shard_index, combine_deadline);
            if pending_requests.is_empty() {
                break;
            }
        }
        let wait_micros = saturating_micros(earliest_enqueue.elapsed().as_micros());
        self.append_combiner_stats[shard_index].record_flush(row_count, input_blocks, wait_micros);

        (
            PreparedTraceShardBatch::from_prepared_blocks(vec![PreparedTraceBlockAppend::new(
                coalesce_trace_blocks_for_shard(trace_blocks),
                self.trace_shards.len(),
            )]),
            responders,
            earliest_enqueue,
        )
    }

    fn take_trace_append_requests(
        &self,
        shard_index: usize,
    ) -> Option<Vec<DiskTraceAppendRequest>> {
        let mut state = self.append_combiners[shard_index].lock();
        if state.pending.is_empty() {
            state.combining = false;
            return None;
        }
        let mut requests = Vec::with_capacity(state.pending.len());
        while let Some(request) = state.pending.pop_front() {
            requests.push(request);
        }
        self.append_combiner_stats[shard_index].record_dequeue(requests.len());
        Some(requests)
    }

    fn take_pending_trace_append_requests(
        &self,
        shard_index: usize,
    ) -> Vec<DiskTraceAppendRequest> {
        let mut state = self.append_combiners[shard_index].lock();
        let mut requests = Vec::with_capacity(state.pending.len());
        while let Some(request) = state.pending.pop_front() {
            requests.push(request);
        }
        if !requests.is_empty() {
            self.append_combiner_stats[shard_index].record_dequeue(requests.len());
        }
        requests
    }

    fn take_pending_trace_append_requests_until(
        &self,
        shard_index: usize,
        deadline: Instant,
    ) -> Vec<DiskTraceAppendRequest> {
        loop {
            let requests = self.take_pending_trace_append_requests(shard_index);
            if !requests.is_empty() {
                return requests;
            }
            let now = Instant::now();
            if now >= deadline {
                return Vec::new();
            }
            let remaining = deadline.saturating_duration_since(now);
            if remaining > TRACE_COMBINER_WAIT_POLL_SLICE {
                thread::sleep(TRACE_COMBINER_WAIT_POLL_SLICE);
            } else if remaining > Duration::ZERO {
                thread::yield_now();
            }
        }
    }

    fn head_trace_window_bounds(
        &self,
        shard_index: usize,
        trace_id: &str,
    ) -> Option<TraceWindowBounds> {
        let mut bounds = None;
        {
            let active_segment = self.active_segments[shard_index].lock();
            if let Some(active_segment) = active_segment.as_ref() {
                if let Some(active_bounds) = active_segment.trace_window_bounds(trace_id) {
                    bounds = Some(active_bounds);
                }
            }
        }
        for snapshot in self.sealing_segments[shard_index].read().iter() {
            if let Some(snapshot_bounds) = snapshot.trace_window_bounds(trace_id) {
                bounds.get_or_insert(snapshot_bounds).observe(
                    snapshot_bounds.start_unix_nano,
                    snapshot_bounds.end_unix_nano,
                );
            }
        }
        bounds
    }

    fn head_rows_for_trace(
        &self,
        shard_index: usize,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        let mut rows = Vec::new();
        {
            let active_segment = self.active_segments[shard_index].lock();
            if let Some(active_segment) = active_segment.as_ref() {
                if let Ok(mut active_rows) =
                    active_segment.read_rows_for_trace(trace_id, start_unix_nano, end_unix_nano)
                {
                    rows.append(&mut active_rows);
                }
            }
        }
        for snapshot in self.sealing_segments[shard_index].read().iter() {
            if let Ok(mut snapshot_rows) =
                snapshot.read_rows_for_trace(trace_id, start_unix_nano, end_unix_nano)
            {
                rows.append(&mut snapshot_rows);
            }
        }
        rows
    }

    fn head_stats(&self) -> (u64, u64, BTreeSet<String>) {
        let mut segments = 0u64;
        let mut rows = 0u64;
        let mut trace_ids = BTreeSet::new();

        for shard_index in 0..self.trace_shards.len() {
            {
                let active_segment = self.active_segments[shard_index].lock();
                if let Some(active_segment) = active_segment.as_ref() {
                    if !active_segment.is_empty() {
                        segments += 1;
                        rows += active_segment.row_count();
                        trace_ids.extend(active_segment.list_trace_ids());
                    }
                }
            }
            for snapshot in self.sealing_segments[shard_index].read().iter() {
                segments += 1;
                rows += snapshot.row_count();
                trace_ids.extend(snapshot.list_trace_ids());
            }
        }

        (segments, rows, trace_ids)
    }

    fn head_list_services(&self) -> BTreeSet<String> {
        let mut services = BTreeSet::new();
        for shard_index in 0..self.trace_shards.len() {
            {
                let active_segment = self.active_segments[shard_index].lock();
                if let Some(active_segment) = active_segment.as_ref() {
                    services.extend(active_segment.list_services());
                }
            }
            for snapshot in self.sealing_segments[shard_index].read().iter() {
                services.extend(snapshot.list_services());
            }
        }
        services
    }

    fn head_list_field_names(&self) -> BTreeSet<String> {
        let mut field_names = BTreeSet::new();
        for shard_index in 0..self.trace_shards.len() {
            {
                let active_segment = self.active_segments[shard_index].lock();
                if let Some(active_segment) = active_segment.as_ref() {
                    field_names.extend(active_segment.list_field_names());
                }
            }
            for snapshot in self.sealing_segments[shard_index].read().iter() {
                field_names.extend(snapshot.list_field_names());
            }
        }
        field_names
    }

    fn head_list_field_values(&self, field_name: &str) -> BTreeSet<String> {
        let mut field_values = BTreeSet::new();
        for shard_index in 0..self.trace_shards.len() {
            {
                let active_segment = self.active_segments[shard_index].lock();
                if let Some(active_segment) = active_segment.as_ref() {
                    field_values.extend(active_segment.list_field_values(field_name));
                }
            }
            for snapshot in self.sealing_segments[shard_index].read().iter() {
                field_values.extend(snapshot.list_field_values(field_name));
            }
        }
        field_values
    }

    fn head_search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        let mut hits_by_trace = FxHashMap::default();
        for shard_index in 0..self.trace_shards.len() {
            {
                let active_segment = self.active_segments[shard_index].lock();
                if let Some(active_segment) = active_segment.as_ref() {
                    for hit in active_segment.search_traces(request) {
                        merge_trace_search_hit(&mut hits_by_trace, hit);
                    }
                }
            }
            for snapshot in self.sealing_segments[shard_index].read().iter() {
                for hit in snapshot.search_traces(request) {
                    merge_trace_search_hit(&mut hits_by_trace, hit);
                }
            }
        }
        hits_by_trace.into_values().collect()
    }
}

impl Drop for DiskStorageEngine {
    fn drop(&mut self) {
        for (shard_index, active_segment) in self.active_segments.iter().enumerate() {
            let mut active_segment = active_segment.lock();
            if let Some(mut active_segment) = active_segment.take() {
                if active_segment.is_empty() {
                    continue;
                }
                let _ = active_segment.flush();
                if self.config.sync_policy.requires_sync() {
                    if active_segment.sync_data().is_ok() {
                        self.fsync_operations.fetch_add(1, Ordering::Relaxed);
                    }
                }
                let snapshot = active_segment.into_snapshot(shard_index);
                let _ = self.persist_rotated_head_segment(snapshot);
            }
        }
        let _ = self.flush_log_writer();
        let _ = self.persist_recovery_snapshot();
    }
}

impl StorageEngine for DiskStorageEngine {
    fn append_trace_block(&self, block: TraceBlock) -> Result<(), StorageError> {
        if block.is_empty() {
            return Ok(());
        }

        for (shard_index, shard_blocks) in
            route_trace_blocks_for_append(vec![block], self.trace_shards.len())
        {
            self.submit_trace_shard_blocks(shard_index, shard_blocks)?;
        }
        Ok(())
    }

    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        for (shard_index, shard_blocks) in
            route_trace_blocks_for_append(blocks, self.trace_shards.len())
        {
            self.submit_trace_shard_blocks(shard_index, shard_blocks)?;
        }
        Ok(())
    }

    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }
        self.append_trace_block(TraceBlock::from_rows(rows))
    }

    fn append_logs(&self, rows: Vec<LogRow>) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }

        let bytes_written = {
            let mut writer = self.log_writer.lock();
            let bytes_written = append_logs_to_file(&mut writer, &rows)?;
            if self.config.sync_policy.requires_sync() {
                writer.flush()?;
                writer.get_ref().sync_data()?;
                self.fsync_operations.fetch_add(1, Ordering::Relaxed);
            }
            bytes_written
        };

        self.persisted_bytes
            .fetch_add(bytes_written, Ordering::Relaxed);
        self.log_state.write().ingest_rows(rows);
        Ok(())
    }

    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        self.trace_window_lookups.fetch_add(1, Ordering::Relaxed);
        let shard_index = self.trace_shard_index(trace_id);
        let mut bounds = self.trace_shards[shard_index]
            .read()
            .trace_window_bounds(trace_id);
        if let Some(head_bounds) = self.head_trace_window_bounds(shard_index, trace_id) {
            bounds
                .get_or_insert(head_bounds)
                .observe(head_bounds.start_unix_nano, head_bounds.end_unix_nano);
        }
        bounds.map(|bounds| {
            TraceWindow::new(
                trace_id.to_string(),
                bounds.start_unix_nano,
                bounds.end_unix_nano,
            )
        })
    }

    fn list_trace_ids(&self) -> Vec<String> {
        let mut trace_ids = BTreeSet::new();
        for shard in &self.trace_shards {
            trace_ids.extend(shard.read().list_trace_ids());
        }
        let (_, _, head_trace_ids) = self.head_stats();
        trace_ids.extend(head_trace_ids);
        trace_ids.into_iter().collect()
    }

    fn list_services(&self) -> Vec<String> {
        let mut services = BTreeSet::new();
        for trace_shard in &self.trace_shards {
            services.extend(trace_shard.read().list_services());
        }
        services.extend(self.head_list_services());
        services.into_iter().collect()
    }

    fn list_field_names(&self) -> Vec<String> {
        let mut field_names = BTreeSet::new();
        for trace_shard in &self.trace_shards {
            field_names.extend(trace_shard.read().list_field_names());
        }
        field_names.extend(self.head_list_field_names());
        field_names.into_iter().collect()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        let mut field_values = BTreeSet::new();
        for trace_shard in &self.trace_shards {
            field_values.extend(trace_shard.read().list_field_values(field_name));
        }
        field_values.extend(self.head_list_field_values(field_name));
        field_values.into_iter().collect()
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        let mut hits_by_trace = FxHashMap::default();
        for trace_shard in &self.trace_shards {
            let persisted_hits = {
                let shard = trace_shard.read();
                shard.search_persisted_segments(request)
            };
            if let Some(persisted_hits) = persisted_hits {
                for hit in persisted_hits {
                    merge_trace_search_hit(&mut hits_by_trace, hit);
                }
            } else {
                for hit in trace_shard.read().search_traces(request) {
                    merge_trace_search_hit(&mut hits_by_trace, hit);
                }
            }
        }
        for hit in self.head_search_traces(request) {
            merge_trace_search_hit(&mut hits_by_trace, hit);
        }
        let mut hits: Vec<_> = hits_by_trace.into_values().collect();
        hits.sort_by(|left, right| {
            right
                .end_unix_nano
                .cmp(&left.end_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        hits.truncate(request.limit);
        hits
    }

    fn search_logs(&self, request: &LogSearchRequest) -> Vec<LogRow> {
        self.log_state.read().search_logs(request)
    }

    fn rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        self.row_queries.fetch_add(1, Ordering::Relaxed);

        let shard_index = self.trace_shard_index(trace_id);
        let row_refs = self.trace_shards[shard_index].read().row_refs_for_trace(
            trace_id,
            start_unix_nano,
            end_unix_nano,
        );
        let segment_paths = self.segment_paths.read().clone();

        let mut rows = Vec::with_capacity(row_refs.len());
        let mut refs_by_segment: HashMap<u64, Vec<SegmentRowLocation>> = HashMap::new();
        for row_ref in row_refs {
            refs_by_segment
                .entry(u64::from(row_ref.segment_id))
                .or_default()
                .push(row_ref);
        }

        self.segment_read_batches
            .fetch_add(refs_by_segment.len() as u64, Ordering::Relaxed);

        for (segment_id, mut segment_refs) in refs_by_segment {
            let Some(path) = segment_paths.get(&segment_id) else {
                continue;
            };
            segment_refs.sort_by_key(|row| row.row_index);
            if path.extension().and_then(|value| value.to_str()) == Some("part") {
                self.part_selective_decodes.fetch_add(1, Ordering::Relaxed);
            }
            if let Ok(mut segment_rows) = read_rows_for_segment(path, &segment_refs) {
                rows.append(&mut segment_rows);
            }
        }
        let head_rows =
            self.head_rows_for_trace(shard_index, trace_id, start_unix_nano, end_unix_nano);
        if !head_rows.is_empty() {
            self.segment_read_batches.fetch_add(1, Ordering::Relaxed);
        }
        rows.extend(head_rows);
        rows.sort_by_key(|row| row.end_unix_nano);
        rows
    }

    fn stats(&self) -> StorageStatsSnapshot {
        let segment_paths = self.segment_paths.read();
        let mut rows_ingested = 0u64;
        let mut trace_ids = BTreeSet::new();
        for shard in &self.trace_shards {
            let state = shard.read();
            rows_ingested += state.rows_ingested();
            trace_ids.extend(state.list_trace_ids());
        }
        let (trace_head_segments, trace_head_rows, head_trace_ids) = self.head_stats();
        rows_ingested += trace_head_rows;
        trace_ids.extend(head_trace_ids);
        StorageStatsSnapshot {
            rows_ingested,
            traces_tracked: trace_ids.len() as u64,
            retained_trace_blocks: 0,
            persisted_bytes: self.persisted_bytes.load(Ordering::Relaxed),
            segment_count: segment_paths.len() as u64 + trace_head_segments,
            trace_head_segments,
            trace_head_rows,
            typed_field_columns: self.field_column_counts.typed.load(Ordering::Relaxed),
            string_field_columns: self.field_column_counts.string.load(Ordering::Relaxed),
            trace_window_lookups: self.trace_window_lookups.load(Ordering::Relaxed),
            row_queries: self.row_queries.load(Ordering::Relaxed),
            segment_read_batches: self.segment_read_batches.load(Ordering::Relaxed),
            part_selective_decodes: self.part_selective_decodes.load(Ordering::Relaxed),
            fsync_operations: self.fsync_operations.load(Ordering::Relaxed),
            trace_group_commit_flushes: self
                .head_metrics
                .group_commit_flushes
                .load(Ordering::Relaxed),
            trace_group_commit_rows: self.head_metrics.group_commit_rows.load(Ordering::Relaxed),
            trace_group_commit_bytes: self.head_metrics.group_commit_bytes.load(Ordering::Relaxed),
            trace_batch_queue_depth: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.queue_depth.load(Ordering::Relaxed))
                .sum(),
            trace_batch_max_queue_depth: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.max_queue_depth.load(Ordering::Relaxed))
                .max()
                .unwrap_or(0),
            trace_batch_flushes: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.flushes.load(Ordering::Relaxed))
                .sum(),
            trace_batch_rows: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.rows.load(Ordering::Relaxed))
                .sum(),
            trace_batch_input_blocks: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.input_blocks.load(Ordering::Relaxed))
                .sum(),
            trace_batch_output_blocks: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.output_blocks.load(Ordering::Relaxed))
                .sum(),
            trace_batch_wait_micros_total: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.wait_micros_total.load(Ordering::Relaxed))
                .sum(),
            trace_batch_wait_micros_max: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.wait_micros_max.load(Ordering::Relaxed))
                .max()
                .unwrap_or(0),
            trace_live_update_queue_depth: 0,
            trace_seal_queue_depth: self.head_metrics.seal_queue_depth.load(Ordering::Relaxed),
            trace_seal_completed: self.head_metrics.seal_completed.load(Ordering::Relaxed),
            trace_seal_rows: self.head_metrics.seal_rows.load(Ordering::Relaxed),
            trace_seal_bytes: self.head_metrics.seal_bytes.load(Ordering::Relaxed),
            trace_batch_flush_due_to_wait: self
                .append_combiner_stats
                .iter()
                .map(|stats| stats.flush_due_to_wait.load(Ordering::Relaxed))
                .sum(),
            ..StorageStatsSnapshot::default()
        }
    }

    fn preferred_trace_ingest_shards(&self) -> usize {
        self.trace_shards.len()
    }

    fn trace_batch_payload_mode(&self) -> TraceBatchPayloadMode {
        TraceBatchPayloadMode::Passthrough
    }
}

impl DiskTraceShardState {
    fn observe_persisted_segment(&mut self, meta: SegmentMeta) {
        let segment_id = meta.segment_id;
        let mut summary = PersistedSegmentSummary {
            window: TraceWindowBounds::new(meta.min_time_unix_nano, meta.max_time_unix_nano),
            services: Vec::new(),
            operations: Vec::new(),
            field_values_by_name: FxHashMap::default(),
            traces: Vec::new(),
        };

        for service_name in meta.services {
            let service_ref = self.strings.intern(&service_name);
            if !summary.services.contains(&service_ref) {
                summary.services.push(service_ref);
            }
        }

        for trace in meta.traces {
            let mut trace_services = Vec::new();
            for operation_name in &trace.operations {
                let operation_ref = self.strings.intern(operation_name);
                if !summary.operations.contains(&operation_ref) {
                    summary.operations.push(operation_ref);
                }
            }
            for service_name in &trace.services {
                let service_ref = self.strings.intern(service_name);
                if !trace_services.contains(&service_ref) {
                    trace_services.push(service_ref);
                }
            }
            let mut trace_operations = Vec::new();
            for operation_name in &trace.operations {
                let operation_ref = self.strings.intern(operation_name);
                if !trace_operations.contains(&operation_ref) {
                    trace_operations.push(operation_ref);
                }
            }
            let mut trace_field_values_by_name: FxHashMap<StringRef, Vec<StringRef>> =
                FxHashMap::default();
            for (field_name, values) in &trace.fields {
                let field_name_ref = self.strings.intern(field_name);
                let summary_values = summary.field_values_by_name.entry(field_name_ref).or_default();
                let trace_values = trace_field_values_by_name.entry(field_name_ref).or_default();
                for value in values {
                    let field_value_ref = self.strings.intern(value);
                    if !summary_values.contains(&field_value_ref) {
                        summary_values.push(field_value_ref);
                    }
                    if !trace_values.contains(&field_value_ref) {
                        trace_values.push(field_value_ref);
                    }
                }
            }
            trace_services.sort_unstable();
            trace_services.dedup();
            trace_operations.sort_unstable();
            trace_operations.dedup();
            for values in trace_field_values_by_name.values_mut() {
                values.sort_unstable();
                values.dedup();
            }
            let trace_ref = self.observe_persisted_trace_runtime(trace);
            summary.traces.push(PersistedTraceSummary {
                trace_ref,
                window: self
                    .windows_by_trace
                    .get(&trace_ref)
                    .copied()
                    .expect("persisted trace window tracked"),
                services: trace_services,
                operations: trace_operations,
                field_values_by_name: trace_field_values_by_name,
            });
        }

        summary.services.sort_unstable();
        summary.operations.sort_unstable();
        for values in summary.field_values_by_name.values_mut() {
            values.sort_unstable();
            values.dedup();
        }
        summary
            .traces
            .sort_by(|left, right| left.trace_ref.cmp(&right.trace_ref));
        self.persisted_segments.insert(segment_id, summary);
    }

    fn observe_persisted_trace_runtime(&mut self, trace: SegmentTraceMeta) -> TraceRef {
        let trace_ref = self.trace_ids.intern(&trace.trace_id);
        self.observe_trace_metadata(trace_ref, trace.start_unix_nano, trace.end_unix_nano);

        let row_refs = self.row_refs_by_trace.entry(trace_ref).or_default();
        let row_count = trace.rows.len() as u64;
        merge_sorted_row_locations(row_refs, trace.rows);
        self.rows_ingested += row_count;
        trace_ref
    }

    fn observe_trace_metadata(
        &mut self,
        trace_ref: TraceRef,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) {
        self.all_trace_refs.insert(trace_ref);
        self.windows_by_trace
            .entry(trace_ref)
            .and_modify(|window| window.observe(start_unix_nano, end_unix_nano))
            .or_insert_with(|| TraceWindowBounds::new(start_unix_nano, end_unix_nano));
    }

    fn trace_window_bounds(&self, trace_id: &str) -> Option<TraceWindowBounds> {
        let trace_ref = self.trace_ids.lookup(trace_id)?;
        self.windows_by_trace.get(&trace_ref).copied()
    }

    fn list_services(&self) -> Vec<String> {
        if !self.persisted_segments.is_empty() {
            let mut services = BTreeSet::new();
            for summary in self.persisted_segments.values() {
                for service_ref in &summary.services {
                    if let Some(service_name) = self.strings.resolve(*service_ref) {
                        services.insert(service_name.to_string());
                    }
                }
            }
            return services.into_iter().collect();
        }
        let mut services: Vec<String> = self
            .trace_refs_by_service
            .keys()
            .filter_map(|value| self.strings.resolve(*value).map(ToString::to_string))
            .collect();
        services.sort();
        services
    }

    fn list_field_names(&self) -> Vec<String> {
        if !self.persisted_segments.is_empty() {
            let mut field_names = BTreeSet::new();
            for summary in self.persisted_segments.values() {
                for field_name_ref in summary.field_values_by_name.keys() {
                    if let Some(field_name) = self.strings.resolve(*field_name_ref) {
                        field_names.insert(field_name.to_string());
                    }
                }
            }
            return field_names.into_iter().collect();
        }
        let mut field_names: Vec<String> = self
            .field_values_by_name
            .keys()
            .filter_map(|value| self.strings.resolve(*value).map(ToString::to_string))
            .collect();
        field_names.sort();
        field_names
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        if !self.persisted_segments.is_empty() {
            let Some(field_name_ref) = self.strings.lookup(field_name) else {
                return Vec::new();
            };
            let mut values = BTreeSet::new();
            for summary in self.persisted_segments.values() {
                if let Some(summary_values) = summary.field_values_by_name.get(&field_name_ref) {
                    for field_value_ref in summary_values {
                        if let Some(field_value) = self.strings.resolve(*field_value_ref) {
                            values.insert(field_value.to_string());
                        }
                    }
                }
            }
            return values.into_iter().collect();
        }
        let Some(field_name_ref) = self.strings.lookup(field_name) else {
            return Vec::new();
        };
        let mut values: Vec<String> = self
            .field_values_by_name
            .get(&field_name_ref)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value| self.strings.resolve(*value).map(ToString::to_string))
                    .collect()
            })
            .unwrap_or_default();
        values.sort();
        values
    }

    fn list_trace_ids(&self) -> Vec<String> {
        let mut trace_ids: Vec<String> = self
            .windows_by_trace
            .keys()
            .filter_map(|trace_ref| self.trace_ids.resolve(*trace_ref).map(ToString::to_string))
            .collect();
        trace_ids.sort();
        trace_ids
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        let candidate_trace_refs = self.candidate_trace_refs(request);

        let mut hits: Vec<TraceSearchHit> = candidate_trace_refs
            .into_iter()
            .filter_map(|trace_ref| {
                let window = self.windows_by_trace.get(&trace_ref)?;
                let overlaps = window.end_unix_nano >= request.start_unix_nano
                    && window.start_unix_nano <= request.end_unix_nano;
                if !overlaps {
                    return None;
                }

                let services = self
                    .services_by_trace
                    .get(&trace_ref)
                    .map(|values| {
                        let mut values: Vec<String> = values
                            .iter()
                            .filter_map(|value| {
                                self.strings.resolve(*value).map(ToString::to_string)
                            })
                            .collect();
                        values.sort();
                        values
                    })
                    .unwrap_or_default();

                Some(TraceSearchHit {
                    trace_id: self.trace_ids.resolve(trace_ref)?.to_string(),
                    start_unix_nano: window.start_unix_nano,
                    end_unix_nano: window.end_unix_nano,
                    services,
                })
            })
            .collect();

        hits.sort_by(|left, right| {
            right
                .end_unix_nano
                .cmp(&left.end_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        hits.truncate(request.limit);
        hits
    }

    fn row_refs_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<SegmentRowLocation> {
        let Some(trace_ref) = self.trace_ids.lookup(trace_id) else {
            return Vec::new();
        };
        self.row_refs_by_trace
            .get(&trace_ref)
            .map(|rows| {
                rows.iter()
                    .filter(|row| {
                        row.end_unix_nano >= start_unix_nano && row.start_unix_nano <= end_unix_nano
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn rows_ingested(&self) -> u64 {
        self.rows_ingested
    }

    fn candidate_persisted_segment_ids(&self, request: &TraceSearchRequest) -> Vec<u64> {
        let mut segment_ids = self
            .persisted_segments
            .iter()
            .filter(|summary| self.persisted_segment_matches_request(*summary, request))
            .map(|(segment_id, _)| *segment_id)
            .collect::<Vec<_>>();
        segment_ids.sort_unstable();
        segment_ids
    }

    fn search_persisted_segments(&self, request: &TraceSearchRequest) -> Option<Vec<TraceSearchHit>> {
        if self.persisted_segments.is_empty() {
            return Some(Vec::new());
        }

        let candidate_segments = self.candidate_persisted_segment_ids(request);
        let mut hits = Vec::new();
        for segment_id in candidate_segments {
            let summary = self.persisted_segments.get(&segment_id)?;
            if summary.traces.is_empty() {
                return None;
            }
            for trace in &summary.traces {
                if !self.persisted_trace_matches_request(trace, request) {
                    continue;
                }
                let mut services = trace
                    .services
                    .iter()
                    .filter_map(|value| self.strings.resolve(*value).map(ToString::to_string))
                    .collect::<Vec<_>>();
                services.sort();
                services.dedup();
                hits.push(TraceSearchHit {
                    trace_id: self.trace_ids.resolve(trace.trace_ref)?.to_string(),
                    start_unix_nano: trace.window.start_unix_nano,
                    end_unix_nano: trace.window.end_unix_nano,
                    services,
                });
            }
        }

        hits.sort_by(|left, right| {
            right
                .end_unix_nano
                .cmp(&left.end_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        Some(hits)
    }

    fn persisted_segment_matches_request(
        &self,
        (.., summary): (&u64, &PersistedSegmentSummary),
        request: &TraceSearchRequest,
    ) -> bool {
        if summary.window.end_unix_nano < request.start_unix_nano
            || summary.window.start_unix_nano > request.end_unix_nano
        {
            return false;
        }
        if let Some(service_name) = &request.service_name {
            let Some(service_ref) = self.strings.lookup(service_name) else {
                return false;
            };
            if !summary.services.contains(&service_ref) {
                return false;
            }
        }
        if let Some(operation_name) = &request.operation_name {
            let Some(operation_ref) = self.strings.lookup(operation_name) else {
                return false;
            };
            if !summary.operations.contains(&operation_ref) {
                return false;
            }
        }
        for field_filter in &request.field_filters {
            let Some(field_name_ref) = self.strings.lookup(&field_filter.name) else {
                return false;
            };
            let Some(field_value_ref) = self.strings.lookup(&field_filter.value) else {
                return false;
            };
            if !summary
                .field_values_by_name
                .get(&field_name_ref)
                .map(|values| values.contains(&field_value_ref))
                .unwrap_or(false)
            {
                return false;
            }
        }
        true
    }

    fn persisted_trace_matches_request(
        &self,
        trace: &PersistedTraceSummary,
        request: &TraceSearchRequest,
    ) -> bool {
        if trace.window.end_unix_nano < request.start_unix_nano
            || trace.window.start_unix_nano > request.end_unix_nano
        {
            return false;
        }
        if let Some(service_name) = &request.service_name {
            let Some(service_ref) = self.strings.lookup(service_name) else {
                return false;
            };
            if !trace.services.contains(&service_ref) {
                return false;
            }
        }
        if let Some(operation_name) = &request.operation_name {
            let Some(operation_ref) = self.strings.lookup(operation_name) else {
                return false;
            };
            if !trace.operations.contains(&operation_ref) {
                return false;
            }
        }
        for field_filter in &request.field_filters {
            let Some(field_name_ref) = self.strings.lookup(&field_filter.name) else {
                return false;
            };
            let Some(field_value_ref) = self.strings.lookup(&field_filter.value) else {
                return false;
            };
            if !trace
                .field_values_by_name
                .get(&field_name_ref)
                .map(|values| values.contains(&field_value_ref))
                .unwrap_or(false)
            {
                return false;
            }
        }
        true
    }

    fn candidate_trace_refs(&self, request: &TraceSearchRequest) -> RoaringBitmap {
        let mut candidate_refs: Option<RoaringBitmap> = request
            .service_name
            .as_ref()
            .and_then(|service_name| self.strings.lookup(service_name))
            .map(|service_ref| {
                self.trace_refs_by_service
                    .get(&service_ref)
                    .cloned()
                    .unwrap_or_default()
            });

        if request.service_name.is_some() && candidate_refs.is_none() {
            return RoaringBitmap::new();
        }

        if let Some(operation_name) = &request.operation_name {
            let Some(operation_ref) = self.strings.lookup(operation_name) else {
                return RoaringBitmap::new();
            };
            let matching = self
                .trace_refs_by_operation
                .get(&operation_ref)
                .cloned()
                .unwrap_or_default();
            candidate_refs = Some(match candidate_refs {
                Some(current) => current & matching,
                None => matching,
            });
        }

        for field_filter in &request.field_filters {
            let Some(field_name_ref) = self.strings.lookup(&field_filter.name) else {
                return RoaringBitmap::new();
            };
            let Some(field_value_ref) = self.strings.lookup(&field_filter.value) else {
                return RoaringBitmap::new();
            };
            let matching = self
                .trace_refs_by_field_name_value
                .get(&field_name_ref)
                .and_then(|values| values.get(&field_value_ref))
                .cloned()
                .unwrap_or_default();
            candidate_refs = Some(match candidate_refs {
                Some(current) => current & matching,
                None => matching,
            });
        }

        candidate_refs.unwrap_or_else(|| self.all_trace_refs.clone())
    }
}

fn merge_trace_search_hit(
    hits_by_trace: &mut FxHashMap<String, TraceSearchHit>,
    mut incoming: TraceSearchHit,
) {
    incoming.services.sort();
    incoming.services.dedup();
    match hits_by_trace.get_mut(&incoming.trace_id) {
        Some(existing) => {
            existing.start_unix_nano = existing.start_unix_nano.min(incoming.start_unix_nano);
            existing.end_unix_nano = existing.end_unix_nano.max(incoming.end_unix_nano);
            existing.services.extend(incoming.services);
            existing.services.sort();
            existing.services.dedup();
        }
        None => {
            hits_by_trace.insert(incoming.trace_id.clone(), incoming);
        }
    }
}

#[cfg(test)]
fn segment_trace_matches_request(trace: &SegmentTraceMeta, request: &TraceSearchRequest) -> bool {
    if trace.end_unix_nano < request.start_unix_nano
        || trace.start_unix_nano > request.end_unix_nano
    {
        return false;
    }
    if let Some(service_name) = &request.service_name {
        if !trace.services.iter().any(|service| service == service_name) {
            return false;
        }
    }
    if let Some(operation_name) = &request.operation_name {
        if !trace
            .operations
            .iter()
            .any(|operation| operation == operation_name)
        {
            return false;
        }
    }
    for field_filter in &request.field_filters {
        if !trace
            .fields
            .get(&field_filter.name)
            .map(|values| values.iter().any(|value| value == &field_filter.value))
            .unwrap_or(false)
        {
            return false;
        }
    }
    true
}

#[cfg(test)]
fn search_segment_meta_for_request(
    meta: &SegmentMeta,
    request: &TraceSearchRequest,
) -> Vec<TraceSearchHit> {
    let mut hits = meta
        .traces
        .iter()
        .filter(|trace| segment_trace_matches_request(trace, request))
        .map(|trace| {
            let mut services = trace.services.clone();
            services.sort();
            services.dedup();
            TraceSearchHit {
                trace_id: trace.trace_id.clone(),
                start_unix_nano: trace.start_unix_nano,
                end_unix_nano: trace.end_unix_nano,
                services,
            }
        })
        .collect::<Vec<_>>();
    hits.sort_by(|left, right| {
        right
            .end_unix_nano
            .cmp(&left.end_unix_nano)
            .then_with(|| left.trace_id.cmp(&right.trace_id))
    });
    hits
}

impl ActiveSegment {
    fn open(
        segments_path: &Path,
        segment_id: u64,
        wal_enabled: bool,
        wal_writer_capacity_bytes: usize,
        deferred_wal_writes: bool,
    ) -> Result<Self, StorageError> {
        let wal_path = segment_wal_path(segments_path, segment_id);
        let wal_sink = if !wal_enabled {
            TraceWalSink::Disabled
        } else if deferred_wal_writes {
            TraceWalSink::Deferred(Vec::with_capacity(wal_writer_capacity_bytes))
        } else {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(&wal_path)?;
            TraceWalSink::Immediate(BufWriter::with_capacity(wal_writer_capacity_bytes, file))
        };

        Ok(Self {
            segment_id,
            wal_path,
            wal_sink,
            accumulator: SegmentAccumulator::default(),
            cached_row_length_prefixes: Vec::new(),
            cached_payload_ranges: Vec::new(),
            cached_payload_bytes: Vec::new(),
        })
    }

    fn append_block_rows(
        &mut self,
        prepared_rows: &[PreparedBlockRowMetadata],
        prepared_shared_groups: &[PreparedSharedGroupMetadata],
        encoded_row_bytes: &[u8],
        encoded_row_ranges: &[std::ops::Range<usize>],
        start_index: usize,
        end_index: usize,
    ) -> Result<TraceAppendRowsResult, StorageError> {
        if start_index >= end_index {
            return Ok(TraceAppendRowsResult::default());
        }

        let total_payload_bytes: usize = encoded_row_ranges[start_index..end_index]
            .iter()
            .map(|range| range.len())
            .sum();
        let wal_enabled = !matches!(self.wal_sink, TraceWalSink::Disabled);
        let first_write = self.accumulator.row_count == 0 && self.accumulator.persisted_bytes == 0;
        let row_count = end_index - start_index;
        let bytes_written = if wal_enabled {
            wal_batch_storage_bytes(total_payload_bytes, row_count)
                + if first_write { WAL_HEADER_BYTES } else { 0 }
        } else {
            total_payload_bytes as u64
        };
        if wal_enabled && first_write {
            self.write_wal_all(WAL_MAGIC)?;
            self.write_wal_all(&[WAL_VERSION_BATCHED_BINARY])?;
        }

        let batch_body_len: u32 = wal_batch_body_bytes(total_payload_bytes, row_count)
            .try_into()
            .map_err(|_| {
                StorageError::Message("trace wal batch body exceeds u32 length".to_string())
            })?;
        let row_count_bytes = (row_count as u32).to_le_bytes();
        let batch_payload_start = encoded_row_ranges[start_index].start;
        let batch_payload_end = encoded_row_ranges[end_index - 1].end;
        if wal_enabled {
            self.cached_row_length_prefixes.clear();
            self.cached_row_length_prefixes
                .reserve(row_count * LENGTH_PREFIX_BYTES as usize);
            for row_index in start_index..end_index {
                let payload = &encoded_row_bytes[encoded_row_ranges[row_index].clone()];
                let payload_len_bytes = (payload.len() as u32).to_le_bytes();
                self.cached_row_length_prefixes
                    .extend_from_slice(&payload_len_bytes);
            }
            let mut checksum_hasher = crc32fast::Hasher::new();
            checksum_hasher.update(&row_count_bytes);
            checksum_hasher.update(&self.cached_row_length_prefixes);
            checksum_hasher.update(&encoded_row_bytes[batch_payload_start..batch_payload_end]);
            self.write_wal_all(&batch_body_len.to_le_bytes())?;
            self.write_wal_all(&checksum_hasher.finalize().to_le_bytes())?;
            self.write_wal_all(&row_count_bytes)?;
            let row_length_prefixes = std::mem::take(&mut self.cached_row_length_prefixes);
            self.write_wal_all(&row_length_prefixes)?;
            self.cached_row_length_prefixes = row_length_prefixes;
        }

        let payload_storage_base = match &self.wal_sink {
            TraceWalSink::Immediate(_) => self.cached_payload_bytes.len(),
            TraceWalSink::Deferred(staged_wal_bytes) => staged_wal_bytes.len(),
            TraceWalSink::Disabled => self.cached_payload_bytes.len(),
        };
        if matches!(
            self.wal_sink,
            TraceWalSink::Immediate(_) | TraceWalSink::Disabled
        ) {
            self.cached_payload_bytes
                .extend_from_slice(&encoded_row_bytes[batch_payload_start..batch_payload_end]);
        }
        if wal_enabled {
            self.write_wal_all(&encoded_row_bytes[batch_payload_start..batch_payload_end])?;
        }
        self.cached_payload_ranges.reserve(row_count);
        let mut intern_cache = BatchInternCache::default();
        let base_row_index = self.accumulator.row_count;

        let mut row_locations = Vec::with_capacity(row_count);
        for (batch_offset, row_index) in (start_index..end_index).enumerate() {
            let payload = &encoded_row_bytes[encoded_row_ranges[row_index].clone()];
            let payload_start =
                payload_storage_base + (encoded_row_ranges[row_index].start - batch_payload_start);
            self.cached_payload_ranges
                .push(payload_start..payload_start + payload.len());
            let row_location = SegmentRowLocation {
                segment_id: self.segment_id.try_into().map_err(|_| {
                    StorageError::Message("segment id exceeds u32 row-ref limit".to_string())
                })?,
                row_index: (base_row_index + batch_offset as u64)
                    .try_into()
                    .map_err(|_| {
                        StorageError::Message("segment row index exceeds u32 limit".to_string())
                    })?,
                start_unix_nano: prepared_rows[row_index].start_unix_nano,
                end_unix_nano: prepared_rows[row_index].end_unix_nano,
            };
            row_locations.push(row_location);
        }
        self.accumulator.ingest_prepared_block_rows(
            &prepared_rows[start_index..end_index],
            prepared_shared_groups,
            &row_locations,
            &mut intern_cache,
        );
        self.accumulator.persisted_bytes += bytes_written;
        Ok(TraceAppendRowsResult { bytes_written })
    }

    fn read_rows(
        &self,
        row_refs: &[SegmentRowLocation],
    ) -> Result<Vec<TraceSpanRow>, StorageError> {
        let payload_bytes = match &self.wal_sink {
            TraceWalSink::Immediate(_) => self.cached_payload_bytes.as_slice(),
            TraceWalSink::Deferred(staged_wal_bytes) => staged_wal_bytes.as_slice(),
            TraceWalSink::Disabled => self.cached_payload_bytes.as_slice(),
        };
        let mut rows = Vec::with_capacity(row_refs.len());
        for row_ref in row_refs {
            let Some(range) = self.cached_payload_ranges.get(row_ref.row_index as usize) else {
                return Err(StorageError::Message(format!(
                    "active segment row {} missing from cache",
                    row_ref.row_index
                )));
            };
            rows.push(
                decode_trace_row(&payload_bytes[range.clone()])
                    .map_err(|err| StorageError::Message(err.to_string()))?,
            );
        }
        Ok(rows)
    }

    fn persisted_bytes(&self) -> u64 {
        self.accumulator.persisted_bytes
    }

    fn row_count(&self) -> u64 {
        self.accumulator.row_count
    }

    fn is_empty(&self) -> bool {
        self.accumulator.row_count == 0
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        if let TraceWalSink::Immediate(writer) = &mut self.wal_sink {
            writer.flush()?;
        }
        Ok(())
    }

    fn sync_data(&mut self) -> Result<(), StorageError> {
        if let TraceWalSink::Immediate(writer) = &mut self.wal_sink {
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    fn trace_window_bounds(&self, trace_id: &str) -> Option<TraceWindowBounds> {
        self.accumulator.trace_window_bounds(trace_id)
    }

    fn read_rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Result<Vec<TraceSpanRow>, StorageError> {
        let row_refs =
            self.accumulator
                .row_refs_for_trace(trace_id, start_unix_nano, end_unix_nano);
        self.read_rows(&row_refs)
    }

    fn list_trace_ids(&self) -> Vec<String> {
        self.accumulator.list_trace_ids()
    }

    fn list_services(&self) -> Vec<String> {
        self.accumulator.list_services()
    }

    fn list_field_names(&self) -> Vec<String> {
        self.accumulator.list_field_names()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.accumulator.list_field_values(field_name)
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.accumulator.search_traces(request)
    }

    fn into_snapshot(self, shard_index: usize) -> HeadSegmentSnapshot {
        let wal_enabled = !matches!(self.wal_sink, TraceWalSink::Disabled);
        let staged_wal_bytes = match self.wal_sink {
            TraceWalSink::Immediate(_) => Vec::new(),
            TraceWalSink::Deferred(staged_wal_bytes) => staged_wal_bytes,
            TraceWalSink::Disabled => Vec::new(),
        };
        HeadSegmentSnapshot {
            shard_index,
            segment_id: self.segment_id,
            wal_path: self.wal_path,
            wal_enabled,
            staged_wal_bytes,
            accumulator: self.accumulator,
            cached_payload_ranges: self.cached_payload_ranges,
            cached_payload_bytes: self.cached_payload_bytes,
        }
    }

    fn write_wal_all(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        match &mut self.wal_sink {
            TraceWalSink::Immediate(writer) => writer.write_all(bytes)?,
            TraceWalSink::Deferred(staged_wal_bytes) => staged_wal_bytes.extend_from_slice(bytes),
            TraceWalSink::Disabled => {}
        }
        Ok(())
    }

}

impl HeadSegmentSnapshot {
    fn trace_window_bounds(&self, trace_id: &str) -> Option<TraceWindowBounds> {
        self.accumulator.trace_window_bounds(trace_id)
    }

    fn row_count(&self) -> u64 {
        self.accumulator.row_count
    }

    fn list_trace_ids(&self) -> Vec<String> {
        self.accumulator.list_trace_ids()
    }

    fn list_services(&self) -> Vec<String> {
        self.accumulator.list_services()
    }

    fn list_field_names(&self) -> Vec<String> {
        self.accumulator.list_field_names()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.accumulator.list_field_values(field_name)
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.accumulator.search_traces(request)
    }

    fn read_rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Result<Vec<TraceSpanRow>, StorageError> {
        let row_refs =
            self.accumulator
                .row_refs_for_trace(trace_id, start_unix_nano, end_unix_nano);
        self.read_rows(&row_refs)
    }

    fn read_rows(
        &self,
        row_refs: &[SegmentRowLocation],
    ) -> Result<Vec<TraceSpanRow>, StorageError> {
        let payload_bytes = if self.staged_wal_bytes.is_empty() {
            self.cached_payload_bytes.as_slice()
        } else {
            self.staged_wal_bytes.as_slice()
        };
        let mut rows = Vec::with_capacity(row_refs.len());
        for row_ref in row_refs {
            let Some(range) = self.cached_payload_ranges.get(row_ref.row_index as usize) else {
                return Err(StorageError::Message(format!(
                    "head snapshot row {} missing from cache",
                    row_ref.row_index
                )));
            };
            rows.push(
                decode_trace_row(&payload_bytes[range.clone()])
                    .map_err(|err| StorageError::Message(err.to_string()))?,
            );
        }
        Ok(rows)
    }

    fn build_meta(&self, segment_path: &Path) -> Result<SegmentMeta, StorageError> {
        let (min_time_unix_nano, max_time_unix_nano) = self
            .accumulator
            .time_range()
            .ok_or_else(|| invalid_data("cannot build meta for empty head snapshot"))?;

        Ok(self.accumulator.build_meta(
            self.segment_id,
            segment_path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
                .ok_or_else(|| invalid_data("segment path has no file name"))?,
            min_time_unix_nano,
            max_time_unix_nano,
        ))
    }
}

impl SegmentAccumulator {
    fn trace_window_bounds(&self, trace_id: &str) -> Option<TraceWindowBounds> {
        let trace_ref = self.trace_ids.lookup(trace_id)?;
        self.traces.get(&trace_ref).map(|trace| trace.window)
    }

    fn row_refs_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<SegmentRowLocation> {
        let Some(trace_ref) = self.trace_ids.lookup(trace_id) else {
            return Vec::new();
        };
        self.traces
            .get(&trace_ref)
            .map(|trace| {
                trace
                    .rows
                    .iter()
                    .filter(|row| {
                        row.end_unix_nano >= start_unix_nano && row.start_unix_nano <= end_unix_nano
                    })
                    .copied()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn list_trace_ids(&self) -> Vec<String> {
        let mut trace_ids: Vec<String> = self
            .traces
            .keys()
            .filter_map(|trace_ref| self.trace_ids.resolve(*trace_ref).map(ToString::to_string))
            .collect();
        trace_ids.sort();
        trace_ids
    }

    fn list_services(&self) -> Vec<String> {
        let mut services = BTreeSet::new();
        for trace in self.traces.values() {
            for service_ref in &trace.services {
                if let Some(service_name) = self.strings.resolve(*service_ref) {
                    services.insert(service_name.to_string());
                }
            }
        }
        let mut services: Vec<String> = services.into_iter().collect();
        services.sort();
        services
    }

    fn list_field_names(&self) -> Vec<String> {
        let mut field_names = BTreeSet::new();
        for trace in self.traces.values() {
            for (field_name_ref, _) in &trace.fields {
                if let Some(field_name) = self.strings.resolve(*field_name_ref) {
                    field_names.insert(field_name.to_string());
                }
            }
        }
        let mut field_names: Vec<String> = field_names.into_iter().collect();
        field_names.sort();
        field_names
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        let mut values = BTreeSet::new();
        for trace in self.traces.values() {
            for (field_name_ref, field_value_ref) in &trace.fields {
                let Some(existing_name) = self.strings.resolve(*field_name_ref) else {
                    continue;
                };
                if existing_name != field_name {
                    continue;
                }
                if let Some(field_value) = self.strings.resolve(*field_value_ref) {
                    values.insert(field_value.to_string());
                }
            }
        }
        let mut values: Vec<String> = values.into_iter().collect();
        values.sort();
        values
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        let mut hits: Vec<TraceSearchHit> = self
            .traces
            .iter()
            .filter_map(|(trace_ref, trace)| {
                let overlaps = trace.window.end_unix_nano >= request.start_unix_nano
                    && trace.window.start_unix_nano <= request.end_unix_nano;
                if !overlaps {
                    return None;
                }
                if let Some(service_name) = &request.service_name {
                    let matches = trace.services.iter().any(|service_ref| {
                        self.strings.resolve(*service_ref) == Some(service_name.as_str())
                    });
                    if !matches {
                        return None;
                    }
                }
                if let Some(operation_name) = &request.operation_name {
                    let matches = trace.operations.iter().any(|operation_ref| {
                        self.strings.resolve(*operation_ref) == Some(operation_name.as_str())
                    });
                    if !matches {
                        return None;
                    }
                }
                for field_filter in &request.field_filters {
                    let matches = trace
                        .fields
                        .iter()
                        .any(|(field_name_ref, field_value_ref)| {
                            self.strings.resolve(*field_name_ref)
                                == Some(field_filter.name.as_str())
                                && self.strings.resolve(*field_value_ref)
                                    == Some(field_filter.value.as_str())
                        });
                    if !matches {
                        return None;
                    }
                }

                let mut services: Vec<String> = trace
                    .services
                    .iter()
                    .filter_map(|value| self.strings.resolve(*value).map(ToString::to_string))
                    .collect();
                services.sort();

                Some(TraceSearchHit {
                    trace_id: self.trace_ids.resolve(*trace_ref)?.to_string(),
                    start_unix_nano: trace.window.start_unix_nano,
                    end_unix_nano: trace.window.end_unix_nano,
                    services,
                })
            })
            .collect();

        hits.sort_by(|left, right| {
            right
                .end_unix_nano
                .cmp(&left.end_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        hits.truncate(request.limit);
        hits
    }

    fn ingest_prepared_block_rows<'a>(
        &mut self,
        prepared_rows: &'a [PreparedBlockRowMetadata],
        prepared_shared_groups: &'a [PreparedSharedGroupMetadata],
        row_locations: &[SegmentRowLocation],
        intern_cache: &mut BatchInternCache<'a>,
    ) {
        if prepared_rows.is_empty() {
            return;
        }
        debug_assert_eq!(prepared_rows.len(), row_locations.len());

        let mut run_start = 0usize;
        while run_start < prepared_rows.len() {
            let trace_id = prepared_rows[run_start].trace_id.as_ref();
            let mut run_end = run_start + 1;
            while run_end < prepared_rows.len()
                && prepared_rows[run_end].trace_id.as_ref() == trace_id
            {
                run_end += 1;
            }
            self.ingest_prepared_block_row_run(
                &prepared_rows[run_start..run_end],
                prepared_shared_groups,
                &row_locations[run_start..run_end],
                intern_cache,
            );
            run_start = run_end;
        }
    }

    fn ingest_prepared_block_row_run<'a>(
        &mut self,
        prepared_rows: &'a [PreparedBlockRowMetadata],
        prepared_shared_groups: &'a [PreparedSharedGroupMetadata],
        row_locations: &[SegmentRowLocation],
        intern_cache: &mut BatchInternCache<'a>,
    ) {
        let Some(first_row) = prepared_rows.first() else {
            return;
        };
        let Some(first_location) = row_locations.first() else {
            return;
        };
        debug_assert_eq!(prepared_rows.len(), row_locations.len());

        let mut min_start_unix_nano = first_location.start_unix_nano;
        let mut max_end_unix_nano = first_location.end_unix_nano;
        for row_location in row_locations.iter().skip(1) {
            min_start_unix_nano = min_start_unix_nano.min(row_location.start_unix_nano);
            max_end_unix_nano = max_end_unix_nano.max(row_location.end_unix_nano);
        }

        self.row_count += row_locations.len() as u64;
        self.observe_time_range(min_start_unix_nano, max_end_unix_nano);

        let trace_ref = intern_cache.trace_ref(&mut self.trace_ids, first_row.trace_id.as_ref());

        let mut batch_services = SmallVec::<[StringRef; 4]>::new();
        let mut batch_operations = SmallVec::<[StringRef; 4]>::new();
        let mut batch_fields = SmallVec::<[(StringRef, StringRef); 8]>::new();
        for prepared_row in prepared_rows {
            if let Some(service_name) = prepared_row.service.as_ref() {
                let service_ref = intern_cache.string_ref(&mut self.strings, service_name.as_ref());
                if !batch_services.contains(&service_ref) {
                    batch_services.push(service_ref);
                }
            }
            if let Some(operation_name) = prepared_row.operation.as_ref() {
                let operation_ref =
                    intern_cache.string_ref(&mut self.strings, operation_name.as_ref());
                if !batch_operations.contains(&operation_ref) {
                    batch_operations.push(operation_ref);
                }
            }
            observe_prepared_row_indexed_fields(
                prepared_row,
                prepared_shared_groups,
                |field_name, field_value| {
                    let field_name_ref =
                        intern_cache.string_ref(&mut self.strings, field_name.as_ref());
                    let field_value_ref =
                        intern_cache.string_ref(&mut self.strings, field_value.as_ref());
                    if !batch_fields.iter().any(|(existing_name, existing_value)| {
                        *existing_name == field_name_ref && *existing_value == field_value_ref
                    }) {
                        batch_fields.push((field_name_ref, field_value_ref));
                    }
                },
            );
        }

        let trace = self.traces.entry(trace_ref).or_insert_with(|| {
            SegmentTraceAccumulator::new(min_start_unix_nano, max_end_unix_nano)
        });
        trace.window.observe(min_start_unix_nano, max_end_unix_nano);
        for service_ref in batch_services {
            push_unique_string_ref(&mut trace.services, service_ref);
        }
        for operation_ref in batch_operations {
            push_unique_string_ref(&mut trace.operations, operation_ref);
        }
        for (field_name_ref, field_value_ref) in batch_fields {
            let _ = push_unique_string_ref_pair(&mut trace.fields, field_name_ref, field_value_ref);
        }
        trace.rows.extend_from_slice(row_locations);
    }

    fn observe_time_range(&mut self, start_unix_nano: i64, end_unix_nano: i64) {
        self.min_time_unix_nano = Some(
            self.min_time_unix_nano
                .map(|value| value.min(start_unix_nano))
                .unwrap_or(start_unix_nano),
        );
        self.max_time_unix_nano = Some(
            self.max_time_unix_nano
                .map(|value| value.max(end_unix_nano))
                .unwrap_or(end_unix_nano),
        );
    }

    fn observe_row(&mut self, row: &TraceSpanRow, row_location: SegmentRowLocation) {
        self.row_count += 1;
        self.observe_time_range(row.start_unix_nano, row.end_unix_nano);

        let trace_ref = self.trace_ids.intern(&row.trace_id);
        let service_ref = row
            .service_name()
            .filter(|service_name| !service_name.is_empty() && *service_name != "-")
            .map(|service_name| self.strings.intern(service_name));
        let operation_ref = (!row.name.is_empty()).then(|| self.strings.intern(row.name.as_str()));
        let mut indexed_fields = Vec::new();
        for field in &row.fields {
            if should_index_field(&field.name) {
                observe_segment_trace_field_refs(
                    &mut self.strings,
                    &mut indexed_fields,
                    &field.name,
                    &field.value,
                );
            }
        }

        let trace = self.traces.entry(trace_ref).or_insert_with(|| {
            SegmentTraceAccumulator::new(row.start_unix_nano, row.end_unix_nano)
        });
        trace.window.observe(row.start_unix_nano, row.end_unix_nano);
        if let Some(service_ref) = service_ref {
            let _ = push_unique_string_ref(&mut trace.services, service_ref);
        }
        if let Some(operation_ref) = operation_ref {
            let _ = push_unique_string_ref(&mut trace.operations, operation_ref);
        }
        for (field_name_ref, field_value_ref) in indexed_fields {
            let _ = push_unique_string_ref_pair(&mut trace.fields, field_name_ref, field_value_ref);
        }
        trace.rows.push(row_location);
    }

    fn time_range(&self) -> Option<(i64, i64)> {
        Some((self.min_time_unix_nano?, self.max_time_unix_nano?))
    }

    fn build_meta(
        &self,
        segment_id: u64,
        segment_file: String,
        min_time_unix_nano: i64,
        max_time_unix_nano: i64,
    ) -> SegmentMeta {
        let mut services = BTreeSet::new();
        let mut field_values_by_name: HashMap<String, BTreeSet<String>> = HashMap::new();
        let traces = self
            .traces
            .iter()
            .map(|(trace_ref, trace)| {
                let trace_id = self
                    .trace_ids
                    .resolve(*trace_ref)
                    .expect("trace ref should resolve for segment meta")
                    .to_string();
                let mut trace_services: Vec<String> = trace
                    .services
                    .iter()
                    .map(|service_ref| {
                        let service_name = self
                            .strings
                            .resolve(*service_ref)
                            .expect("service ref should resolve for segment meta")
                            .to_string();
                        services.insert(service_name.clone());
                        service_name
                    })
                    .collect();
                trace_services.sort();
                let mut trace_operations: Vec<String> = trace
                    .operations
                    .iter()
                    .map(|operation_ref| {
                        self.strings
                            .resolve(*operation_ref)
                            .expect("operation ref should resolve for segment meta")
                            .to_string()
                    })
                    .collect();
                trace_operations.sort();
                let mut trace_fields: HashMap<String, Vec<String>> = HashMap::new();
                for (field_name_ref, field_value_ref) in &trace.fields {
                    let field_name = self
                        .strings
                        .resolve(*field_name_ref)
                        .expect("field-name ref should resolve for segment meta")
                        .to_string();
                    let field_value = self
                        .strings
                        .resolve(*field_value_ref)
                        .expect("field-value ref should resolve for segment meta")
                        .to_string();
                    field_values_by_name
                        .entry(field_name.clone())
                        .or_default()
                        .insert(field_value.clone());
                    trace_fields
                        .entry(field_name)
                        .or_default()
                        .push(field_value);
                }
                for values in trace_fields.values_mut() {
                    values.sort();
                }
                let mut rows = trace.rows.clone();
                rows.sort_by_key(|location| location.end_unix_nano);
                SegmentTraceMeta {
                    trace_id,
                    start_unix_nano: trace.window.start_unix_nano,
                    end_unix_nano: trace.window.end_unix_nano,
                    services: trace_services,
                    operations: trace_operations,
                    fields: trace_fields,
                    rows,
                }
            })
            .collect();
        let (typed_field_columns, string_field_columns) =
            count_field_column_encodings_from_value_sets(field_values_by_name.values());

        SegmentMeta {
            segment_id,
            segment_file,
            row_count: self.row_count,
            persisted_bytes: self.persisted_bytes,
            min_time_unix_nano,
            max_time_unix_nano,
            typed_field_columns,
            string_field_columns,
            services: services.into_iter().collect(),
            traces,
        }
    }
}

impl ColumnarPart {
    fn from_rows(rows: &[TraceSpanRow]) -> Self {
        let row_count = rows.len() as u32;
        let trace_ids = DictionaryStringColumn::from_required(
            rows.iter().map(|row| row.trace_id.clone()).collect(),
        );
        let span_ids = DictionaryStringColumn::from_required(
            rows.iter().map(|row| row.span_id.clone()).collect(),
        );
        let parent_span_ids = DictionaryOptionalStringColumn::from_optional(
            rows.iter().map(|row| row.parent_span_id.clone()).collect(),
        );
        let names = DictionaryStringColumn::from_required(
            rows.iter().map(|row| row.name.clone()).collect(),
        );
        let start_unix_nanos = rows.iter().map(|row| row.start_unix_nano).collect();
        let end_unix_nanos = rows.iter().map(|row| row.end_unix_nano).collect();
        let time_unix_nanos = rows.iter().map(|row| row.time_unix_nano).collect();

        let mut field_values_by_name: HashMap<String, Vec<Option<String>>> = HashMap::new();
        for (row_index, row) in rows.iter().enumerate() {
            for field in row
                .fields
                .iter()
                .filter(|field| !is_materialized_core_field(&field.name))
            {
                let values = field_values_by_name
                    .entry(field.name.to_string())
                    .or_insert_with(|| vec![None; row_index + 1]);
                while values.len() <= row_index {
                    values.push(None);
                }
                values[row_index] = Some(field.value.to_string());
            }
        }

        for values in field_values_by_name.values_mut() {
            while values.len() < rows.len() {
                values.push(None);
            }
        }

        let mut field_columns: Vec<PartFieldColumn> = field_values_by_name
            .into_iter()
            .map(|(name, values)| PartFieldColumn {
                name,
                values: PartFieldColumnValues::from_optional_strings(values),
            })
            .collect();
        field_columns.sort_by(|left, right| left.name.cmp(&right.name));

        Self {
            row_count,
            trace_ids,
            span_ids,
            parent_span_ids,
            names,
            start_unix_nanos,
            end_unix_nanos,
            time_unix_nanos,
            field_columns,
        }
    }

    fn rows(&self) -> Result<Vec<TraceSpanRow>, StorageError> {
        (0..self.row_count as usize)
            .map(|row_index| self.row(row_index))
            .collect()
    }

    fn row(&self, row_index: usize) -> Result<TraceSpanRow, StorageError> {
        let trace_id = self.trace_ids.get(row_index)?.to_string();
        let span_id = self.span_ids.get(row_index)?.to_string();
        let parent_span_id = self
            .parent_span_ids
            .get(row_index)?
            .map(|value| value.to_string());
        let name = self.names.get(row_index)?.to_string();
        let start_unix_nano = *self
            .start_unix_nanos
            .get(row_index)
            .ok_or_else(|| invalid_data("missing start time for row"))?;
        let end_unix_nano = *self
            .end_unix_nanos
            .get(row_index)
            .ok_or_else(|| invalid_data("missing end time for row"))?;

        let mut fields = Vec::new();
        for column in &self.field_columns {
            if let Some(value) = column.values.string_value(row_index)? {
                fields.push(vtcore::Field::new(column.name.clone(), value));
            }
        }

        TraceSpanRow::new(
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_unix_nano,
            end_unix_nano,
            fields,
        )
        .map_err(|error| invalid_data(error.to_string()).into())
    }
}

impl DictionaryStringColumn {
    fn from_required(values: Vec<String>) -> Self {
        let mut dictionary = Vec::new();
        let mut indexes = Vec::with_capacity(values.len());
        let mut dictionary_indexes = HashMap::new();

        for value in values {
            let index = if let Some(index) = dictionary_indexes.get(&value) {
                *index
            } else {
                let index = dictionary.len() as u32;
                dictionary.push(value.clone());
                dictionary_indexes.insert(value, index);
                index
            };
            indexes.push(index);
        }

        Self {
            dictionary,
            indexes,
        }
    }

    fn get(&self, row_index: usize) -> Result<&str, StorageError> {
        let dictionary_index = *self
            .indexes
            .get(row_index)
            .ok_or_else(|| invalid_data("missing dictionary index"))?
            as usize;
        self.dictionary
            .get(dictionary_index)
            .map(|value| value.as_str())
            .ok_or_else(|| invalid_data("dictionary index out of bounds").into())
    }
}

impl DictionaryOptionalStringColumn {
    fn from_optional(values: Vec<Option<String>>) -> Self {
        let mut dictionary = Vec::new();
        let mut indexes = Vec::with_capacity(values.len());
        let mut dictionary_indexes = HashMap::new();

        for value in values {
            let index = if let Some(value) = value {
                if let Some(index) = dictionary_indexes.get(&value) {
                    Some(*index)
                } else {
                    let index = dictionary.len() as u32;
                    dictionary.push(value.clone());
                    dictionary_indexes.insert(value, index);
                    Some(index)
                }
            } else {
                None
            };
            indexes.push(index);
        }

        Self {
            dictionary,
            indexes,
        }
    }

    fn get(&self, row_index: usize) -> Result<Option<&str>, StorageError> {
        let Some(dictionary_index) = self
            .indexes
            .get(row_index)
            .ok_or_else(|| invalid_data("missing optional dictionary index"))?
        else {
            return Ok(None);
        };
        self.dictionary
            .get(*dictionary_index as usize)
            .map(|value| Some(value.as_str()))
            .ok_or_else(|| invalid_data("optional dictionary index out of bounds").into())
    }
}

impl PartFieldColumnValues {
    fn from_optional_strings(values: Vec<Option<String>>) -> Self {
        if let Some(column) = OptionalBoolColumn::from_optional_strings(&values) {
            return Self::Bool(column);
        }
        if let Some(column) = OptionalI64Column::from_optional_strings(&values) {
            return Self::I64(column);
        }
        Self::String(DictionaryOptionalStringColumn::from_optional(values))
    }

    fn string_value(&self, row_index: usize) -> Result<Option<String>, StorageError> {
        match self {
            Self::String(column) => Ok(column.get(row_index)?.map(ToString::to_string)),
            Self::I64(column) => Ok(column.get(row_index)?.map(|value| value.to_string())),
            Self::Bool(column) => Ok(column.get(row_index)?.map(|value| value.to_string())),
        }
    }

    fn type_code(&self) -> u8 {
        match self {
            Self::String(_) => PART_FIELD_TYPE_STRING,
            Self::I64(_) => PART_FIELD_TYPE_I64,
            Self::Bool(_) => PART_FIELD_TYPE_BOOL,
        }
    }

}

impl OptionalI64Column {
    fn from_optional_strings(values: &[Option<String>]) -> Option<Self> {
        let mut typed_values = Vec::with_capacity(values.len());
        for value in values {
            match value {
                Some(value) => typed_values.push(Some(value.parse::<i64>().ok()?)),
                None => typed_values.push(None),
            }
        }
        Some(Self {
            values: typed_values,
        })
    }

    fn get(&self, row_index: usize) -> Result<Option<i64>, StorageError> {
        self.values
            .get(row_index)
            .copied()
            .ok_or_else(|| invalid_data("missing i64 column value").into())
    }
}

impl OptionalBoolColumn {
    fn from_optional_strings(values: &[Option<String>]) -> Option<Self> {
        let mut typed_values = Vec::with_capacity(values.len());
        for value in values {
            match value {
                Some(value) => {
                    let normalized = value.trim().to_ascii_lowercase();
                    match normalized.as_str() {
                        "true" => typed_values.push(Some(true)),
                        "false" => typed_values.push(Some(false)),
                        _ => return None,
                    }
                }
                None => typed_values.push(None),
            }
        }
        Some(Self {
            values: typed_values,
        })
    }

    fn get(&self, row_index: usize) -> Result<Option<bool>, StorageError> {
        self.values
            .get(row_index)
            .copied()
            .ok_or_else(|| invalid_data("missing bool column value").into())
    }
}

fn recover_state(
    root_path: &Path,
    segments_path: &Path,
    trace_shards: usize,
    recovery_seal_worker_count: usize,
) -> Result<RecoveredDiskState, StorageError> {
    let mut segment_files: HashMap<u64, SegmentFileSet> = HashMap::new();

    for entry in fs::read_dir(segments_path)? {
        let entry = entry?;
        let path = entry.path();
        let Some(file_name) = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
        else {
            continue;
        };
        let Some((segment_id, kind)) = parse_segment_file_name(&file_name) else {
            continue;
        };
        let files = segment_files.entry(segment_id).or_default();
        match kind {
            SegmentFileKind::Wal => files.wal_path = Some(path),
            SegmentFileKind::Part => files.part_path = Some(path),
            SegmentFileKind::LegacyRows => files.legacy_rows_path = Some(path),
            SegmentFileKind::MetaJson => files.meta_json_path = Some(path),
            SegmentFileKind::MetaBinary => files.meta_bin_path = Some(path),
        }
    }

    let trace_shards = trace_shards.max(1);
    let mut segment_ids: Vec<u64> = segment_files.keys().cloned().collect();
    segment_ids.sort_unstable();
    let next_segment_id = segment_ids
        .iter()
        .copied()
        .max()
        .map(|value| value + 1)
        .unwrap_or(1);
    let snapshot =
        load_valid_recovery_snapshot(root_path, segments_path, &segment_files, trace_shards)?;
    let covered_segment_ids = snapshot
        .as_ref()
        .map(|snapshot| {
            snapshot
                .covered_segments
                .iter()
                .map(|covered_segment| covered_segment.segment_id)
                .collect::<FxHashSet<_>>()
        })
        .unwrap_or_default();
    let mut recovered = snapshot
        .map(|snapshot| snapshot.into_recovered_state(segments_path))
        .unwrap_or_else(|| RecoveredDiskState {
            trace_shards: (0..trace_shards)
                .map(|_| DiskTraceShardState::default())
                .collect(),
            ..RecoveredDiskState::default()
        });

    let mut recovery_entries = Vec::with_capacity(segment_ids.len());

    for segment_id in segment_ids.iter().copied() {
        let files = segment_files
            .remove(&segment_id)
            .ok_or_else(|| invalid_data("missing segment file set during recovery"))?;
        let source_path = files
            .part_path
            .clone()
            .or_else(|| files.wal_path.clone())
            .or_else(|| files.legacy_rows_path.clone())
            .ok_or_else(|| invalid_data("segment is missing data file"))?;
        if fs::metadata(&source_path)
            .map(|metadata| metadata.len() == 0)
            .unwrap_or(false)
        {
            let _ = fs::remove_file(&source_path);
            continue;
        }
        if covered_segment_ids.contains(&segment_id) {
            if files.meta_json_path.is_none()
                && files.meta_bin_path.is_none()
                && files.part_path.is_some()
            {
                recovered.skip_startup_compaction = true;
            }
            continue;
        }
        recovery_entries.push(RecoveryEntry {
            segment_id,
            data_path: source_path,
            legacy_meta_path: files.meta_json_path,
        });
    }

    let recovered_segments = materialize_recovery_entries(
        recovery_entries,
        recovery_seal_worker_count,
        segments_path,
    )?;

    for recovered_segment in recovered_segments {
        recovered.persisted_bytes += recovered_segment.data_bytes;
        recovered.persisted_bytes += recovered_segment.meta_bytes;
        recovered.typed_field_columns += recovered_segment.meta.typed_field_columns;
        recovered.string_field_columns += recovered_segment.meta.string_field_columns;
        recovered.segment_paths.insert(
            recovered_segment.meta.segment_id,
            recovered_segment.data_path,
        );
        let shard_index = recovered_segment
            .meta
            .traces
            .first()
            .map(|trace| trace_shard_index(&trace.trace_id, trace_shards))
            .unwrap_or(0);
        recovered.trace_shards[shard_index].observe_persisted_segment(recovered_segment.meta);
    }

    recovered.next_segment_id = recovered.next_segment_id.max(next_segment_id);
    Ok(recovered)
}

#[derive(Debug)]
struct RecoveryEntry {
    segment_id: u64,
    data_path: PathBuf,
    legacy_meta_path: Option<PathBuf>,
}

#[derive(Debug)]
struct RecoveredSegmentEntry {
    data_path: PathBuf,
    meta: SegmentMeta,
    data_bytes: u64,
    meta_bytes: u64,
}

fn materialize_recovery_entries(
    entries: Vec<RecoveryEntry>,
    recovery_worker_count: usize,
    segments_path: &Path,
) -> Result<Vec<RecoveredSegmentEntry>, StorageError> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }

    let worker_count = recovery_worker_count.clamp(1, entries.len());
    let mut entry_buckets: Vec<Vec<RecoveryEntry>> =
        (0..worker_count).map(|_| Vec::new()).collect();
    for (entry_index, entry) in entries.into_iter().enumerate() {
        entry_buckets[entry_index % worker_count].push(entry);
    }

    let mut recovered_segments = thread::scope(
        |scope| -> Result<Vec<RecoveredSegmentEntry>, StorageError> {
            let mut handles = Vec::new();
            for entry_bucket in entry_buckets
                .into_iter()
                .filter(|bucket| !bucket.is_empty())
            {
                handles.push(scope.spawn(
                    move || -> Result<Vec<RecoveredSegmentEntry>, StorageError> {
                        let mut prepared = Vec::with_capacity(entry_bucket.len());
                        for entry in entry_bucket {
                            prepared.push(materialize_recovery_entry(entry, segments_path)?);
                        }
                        Ok(prepared)
                    },
                ));
            }

            let mut recovered = Vec::new();
            for handle in handles {
                recovered.extend(handle.join().map_err(|_| {
                    StorageError::Message("disk recovery materializer panicked".to_string())
                })??);
            }
            Ok(recovered)
        },
    )?;

    recovered_segments.sort_by_key(|entry| entry.meta.segment_id);
    Ok(recovered_segments)
}

fn materialize_recovery_entry(
    entry: RecoveryEntry,
    segments_path: &Path,
) -> Result<RecoveredSegmentEntry, StorageError> {
    let legacy_meta_path = entry
        .legacy_meta_path
        .unwrap_or_else(|| segment_meta_path(segments_path, entry.segment_id));
    let mut meta =
        load_or_rebuild_segment_meta(entry.segment_id, &entry.data_path, &legacy_meta_path)?;
    if meta.typed_field_columns == 0 && meta.string_field_columns == 0 {
        let rebuilt = rebuild_segment_meta(entry.segment_id, &entry.data_path)?;
        meta.typed_field_columns = rebuilt.typed_field_columns;
        meta.string_field_columns = rebuilt.string_field_columns;
        let _ = write_segment_meta_binary(
            &segment_binary_meta_path(segments_path, entry.segment_id),
            &meta,
            DiskSyncPolicy::None,
        )?;
    }

    Ok(RecoveredSegmentEntry {
        data_bytes: fs::metadata(&entry.data_path)?.len(),
        meta_bytes: segment_metadata_bytes(segments_path, entry.segment_id),
        data_path: entry.data_path,
        meta,
    })
}

fn load_or_rebuild_segment_meta(
    segment_id: u64,
    part_path: &Path,
    meta_path: &Path,
) -> Result<SegmentMeta, StorageError> {
    let binary_meta_path = part_path
        .parent()
        .map(|segments_path| segment_binary_meta_path(segments_path, segment_id))
        .unwrap_or_else(|| {
            PathBuf::from(format!(
                "{SEGMENT_PREFIX}{segment_id:020}{META_BINARY_SUFFIX}"
            ))
        });
    if binary_meta_path.exists() {
        match read_segment_meta_binary(&binary_meta_path) {
            Ok(meta) => Ok(meta),
            Err(_) => {
                let _ = fs::remove_file(&binary_meta_path);
                if meta_path.exists() {
                    let file = File::open(meta_path)?;
                    let meta: SegmentMeta = serde_json::from_reader(file)?;
                    let _ =
                        write_segment_meta_binary(&binary_meta_path, &meta, DiskSyncPolicy::None)?;
                    Ok(meta)
                } else {
                    let meta = rebuild_segment_meta(segment_id, part_path)?;
                    let _ =
                        write_segment_meta_binary(&binary_meta_path, &meta, DiskSyncPolicy::None)?;
                    Ok(meta)
                }
            }
        }
    } else if meta_path.exists() {
        let file = File::open(meta_path)?;
        let meta: SegmentMeta = serde_json::from_reader(file)?;
        let _ = write_segment_meta_binary(&binary_meta_path, &meta, DiskSyncPolicy::None)?;
        Ok(meta)
    } else {
        let meta = rebuild_segment_meta(segment_id, part_path)?;
        let _ = write_segment_meta_binary(&binary_meta_path, &meta, DiskSyncPolicy::None)?;
        Ok(meta)
    }
}

fn recover_log_state(log_path: &Path) -> Result<(LogIndexedState, u64), StorageError> {
    if !log_path.exists() {
        return Ok((LogIndexedState::default(), 0));
    }

    let mut file = OpenOptions::new().read(true).write(true).open(log_path)?;
    let persisted_bytes = file.metadata()?.len();
    if persisted_bytes == 0 {
        return Ok((LogIndexedState::default(), 0));
    }

    let mut header = vec![0u8; LOG_WAL_MAGIC.len()];
    file.read_exact(&mut header)?;
    if header != LOG_WAL_MAGIC {
        return Err(invalid_data("invalid log wal header").into());
    }
    let mut version = [0u8; 1];
    file.read_exact(&mut version)?;
    if version[0] != LOG_WAL_VERSION_BINARY {
        return Err(invalid_data(format!("unsupported log wal version {}", version[0])).into());
    }

    let mut rows = Vec::new();
    let mut offset = LOG_WAL_HEADER_BYTES;
    let mut last_good_end = offset;
    loop {
        let mut len_buf = [0u8; LENGTH_PREFIX_BYTES as usize];
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
            Err(error) => return Err(error.into()),
        }

        let len = u32::from_le_bytes(len_buf);
        let mut checksum_buf = [0u8; WAL_CHECKSUM_BYTES as usize];
        match file.read_exact(&mut checksum_buf) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
            Err(error) => return Err(error.into()),
        }
        let expected_checksum = u32::from_le_bytes(checksum_buf);

        let mut payload = vec![0u8; len as usize];
        match file.read_exact(&mut payload) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
            Err(error) => return Err(error.into()),
        }

        if crc32fast::hash(&payload) != expected_checksum {
            truncate_invalid_row_tail(&mut file, last_good_end)?;
            break;
        }

        match decode_log_row(&payload) {
            Ok(row) => rows.push(row),
            Err(_) => {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
        }
        offset += wal_record_storage_bytes(len);
        last_good_end = offset;
    }

    let mut state = LogIndexedState::default();
    state.ingest_rows(rows);
    let recovered_bytes = file.metadata()?.len();
    Ok((state, recovered_bytes))
}

fn append_logs_to_file(writer: &mut BufWriter<File>, rows: &[LogRow]) -> Result<u64, StorageError> {
    let file_len = writer.get_ref().metadata()?.len();
    let mut buffered_bytes = Vec::new();
    let mut bytes_written = 0u64;
    if file_len == 0 {
        buffered_bytes.extend_from_slice(LOG_WAL_MAGIC);
        buffered_bytes.push(LOG_WAL_VERSION_BINARY);
        bytes_written += LOG_WAL_HEADER_BYTES;
    }

    for row in rows {
        let payload = encode_log_row(row);
        let checksum = crc32fast::hash(&payload);
        buffered_bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buffered_bytes.extend_from_slice(&checksum.to_le_bytes());
        buffered_bytes.extend_from_slice(&payload);
        bytes_written += wal_record_storage_bytes(payload.len() as u32);
    }

    writer.write_all(&buffered_bytes)?;
    Ok(bytes_written)
}

fn is_materialized_core_field(field_name: &str) -> bool {
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
    )
}

fn should_index_field(field_name: &str) -> bool {
    !matches!(
        field_name,
        "_time"
            | "trace_id"
            | "span_id"
            | "parent_span_id"
            | "start_time_unix_nano"
            | "end_time_unix_nano"
    )
}

fn default_trace_shards() -> usize {
    std::thread::available_parallelism()
        .map(|value| value.get().clamp(1, 16))
        .unwrap_or(4)
}

fn trace_shard_index(trace_id: &str, trace_shards: usize) -> usize {
    if trace_shards <= 1 {
        return 0;
    }
    let mut hasher = FxHasher::default();
    trace_id.hash(&mut hasher);
    (hasher.finish() as usize) % trace_shards
}

fn trace_block_shard_index(block: &TraceBlock, trace_shards: usize) -> Option<usize> {
    if block.is_empty() {
        return None;
    }
    let shard_index = trace_shard_index(block.trace_id_at(0), trace_shards);
    for row_index in 1..block.row_count() {
        if trace_shard_index(block.trace_id_at(row_index), trace_shards) != shard_index {
            return None;
        }
    }
    Some(shard_index)
}

fn partition_trace_block_by_shard(block: TraceBlock, trace_shards: usize) -> Vec<TraceBlock> {
    if trace_shards <= 1 || block.is_empty() {
        return vec![block];
    }

    let mut shard_builders: Vec<TraceBlockBuilder> = (0..trace_shards)
        .map(|_| TraceBlockBuilder::default())
        .collect();
    for row_index in 0..block.row_count() {
        let shard_index = trace_shard_index(block.trace_id_at(row_index), trace_shards);
        shard_builders[shard_index].push_prevalidated_split_fields(
            block.trace_ids[row_index].clone(),
            block.span_ids[row_index].clone(),
            block.parent_span_ids[row_index].clone(),
            block.names[row_index].clone(),
            block.start_unix_nano_at(row_index),
            block.end_unix_nano_at(row_index),
            block.time_unix_nano_at(row_index),
            block.shared_fields_for_row(row_index),
            block.row_fields_at(row_index).iter().cloned(),
        );
    }

    shard_builders
        .into_iter()
        .map(TraceBlockBuilder::finish)
        .filter(|block| !block.is_empty())
        .collect()
}

fn route_trace_blocks_for_append(
    blocks: Vec<TraceBlock>,
    trace_shards: usize,
) -> Vec<(usize, Vec<TraceBlock>)> {
    let mut routed = Vec::new();
    for block in blocks {
        if block.is_empty() {
            continue;
        }
        if let Some(shard_index) = trace_block_shard_index(&block, trace_shards) {
            push_trace_block_for_shard(&mut routed, shard_index, block);
            continue;
        }
        for block in partition_trace_block_by_shard(block, trace_shards) {
            let shard_index = trace_block_shard_index(&block, trace_shards).unwrap_or(0);
            push_trace_block_for_shard(&mut routed, shard_index, block);
        }
    }
    routed
}

fn push_trace_block_for_shard(
    routed: &mut Vec<(usize, Vec<TraceBlock>)>,
    shard_index: usize,
    block: TraceBlock,
) {
    if let Some((_, shard_blocks)) = routed
        .iter_mut()
        .find(|(existing_shard_index, _)| *existing_shard_index == shard_index)
    {
        shard_blocks.push(block);
    } else {
        routed.push((shard_index, vec![block]));
    }
}

fn observe_live_indexed_field(
    indexed_fields: &mut SmallArcPairList,
    field_name: Arc<str>,
    field_value: Arc<str>,
) {
    if field_name.is_empty() || field_value.is_empty() {
        return;
    }
    push_unique_arc_pair(indexed_fields, field_name, field_value);
}

fn push_unique_arc_pair(values: &mut SmallArcPairList, name: Arc<str>, value: Arc<str>) {
    if values.iter().any(|(existing_name, existing_value)| {
        existing_name.as_ref() == name.as_ref() && existing_value.as_ref() == value.as_ref()
    }) {
        return;
    }
    values.push((name, value));
}

fn prepare_block_row_metadata(
    block: &TraceBlock,
    prepared_shared_groups: &[PreparedSharedGroupMetadata],
    trace_shards: usize,
) -> Vec<PreparedBlockRowMetadata> {
    let mut prepared_rows = Vec::with_capacity(block.row_count());
    for row_index in 0..block.row_count() {
        let trace_id = block.trace_ids[row_index].clone();
        let row_fields = block.row_fields_at(row_index);
        let shared_group_index = block
            .shared_field_group_id_at(row_index)
            .checked_sub(1)
            .map(|group_index| group_index as usize);
        let shared_group =
            shared_group_index.and_then(|group_index| prepared_shared_groups.get(group_index));
        let mut service = shared_group.and_then(|group| group.service.clone());
        let mut indexed_fields = SmallArcPairList::with_capacity(row_fields.len());
        for field in row_fields {
            if service.is_none()
                && field.name.as_ref() == "resource_attr:service.name"
                && !field.value.is_empty()
                && field.value.as_ref() != "-"
            {
                service = Some(field.value.clone());
            }
            if should_index_field(&field.name) {
                observe_live_indexed_field(
                    &mut indexed_fields,
                    field.name.clone(),
                    field.value.clone(),
                );
            }
        }
        prepared_rows.push(PreparedBlockRowMetadata {
            shard_index: trace_shard_index(trace_id.as_ref(), trace_shards),
            start_unix_nano: block.start_unix_nano_at(row_index),
            end_unix_nano: block.end_unix_nano_at(row_index),
            shared_group_index,
            trace_id,
            service,
            operation: (!block.names[row_index].is_empty()).then(|| block.names[row_index].clone()),
            indexed_fields,
        });
    }
    prepared_rows
}

fn prepare_shared_group_metadata(block: &TraceBlock) -> Vec<PreparedSharedGroupMetadata> {
    let mut prepared_groups = Vec::with_capacity(block.shared_field_group_count());
    for group_id in 1..=block.shared_field_group_count() as u32 {
        let mut service = None;
        let mut indexed_fields = SmallArcPairList::new();
        for field in block.shared_fields_for_group_id(group_id) {
            if service.is_none()
                && field.name.as_ref() == "resource_attr:service.name"
                && !field.value.is_empty()
                && field.value.as_ref() != "-"
            {
                service = Some(field.value.clone());
            }
            if should_index_field(&field.name) {
                observe_live_indexed_field(
                    &mut indexed_fields,
                    field.name.clone(),
                    field.value.clone(),
                );
            }
        }
        prepared_groups.push(PreparedSharedGroupMetadata {
            service,
            indexed_fields,
        });
    }
    prepared_groups
}

fn push_unique_string_ref(values: &mut Vec<StringRef>, value: StringRef) -> bool {
    if values.contains(&value) {
        return false;
    }
    values.push(value);
    true
}

fn push_unique_string_ref_pair(
    values: &mut Vec<(StringRef, StringRef)>,
    name: StringRef,
    value: StringRef,
) -> bool {
    if values
        .iter()
        .any(|(existing_name, existing_value)| *existing_name == name && *existing_value == value)
    {
        return false;
    }
    values.push((name, value));
    true
}

fn observe_prepared_row_indexed_fields<'a>(
    prepared_row: &'a PreparedBlockRowMetadata,
    prepared_shared_groups: &'a [PreparedSharedGroupMetadata],
    mut observe: impl FnMut(&'a Arc<str>, &'a Arc<str>),
) {
    if let Some(shared_group_index) = prepared_row.shared_group_index {
        if let Some(shared_group) = prepared_shared_groups.get(shared_group_index) {
            for (field_name, field_value) in &shared_group.indexed_fields {
                observe(field_name, field_value);
            }
        }
    }
    for (field_name, field_value) in &prepared_row.indexed_fields {
        observe(field_name, field_value);
    }
}


fn merge_sorted_row_locations(
    existing: &mut Vec<SegmentRowLocation>,
    mut incoming: Vec<SegmentRowLocation>,
) {
    if incoming.is_empty() {
        return;
    }

    sort_row_locations_by_end(&mut incoming);
    if existing.is_empty() {
        *existing = incoming;
        return;
    }

    let append_only = existing
        .last()
        .zip(incoming.first())
        .map(|(current_last, incoming_first)| {
            current_last.end_unix_nano <= incoming_first.end_unix_nano
        })
        .unwrap_or(false);
    if append_only {
        existing.reserve(incoming.len());
        existing.extend(incoming);
        return;
    }

    let prepend_only = existing
        .first()
        .zip(incoming.last())
        .map(|(current_first, incoming_last)| {
            incoming_last.end_unix_nano <= current_first.end_unix_nano
        })
        .unwrap_or(false);
    if prepend_only {
        incoming.reserve(existing.len());
        incoming.extend(std::mem::take(existing));
        *existing = incoming;
        return;
    }

    existing.reserve(incoming.len());
    existing.extend(incoming);
    existing.sort_by_key(|row| row.end_unix_nano);
}

fn sort_row_locations_by_end(rows: &mut Vec<SegmentRowLocation>) {
    if rows
        .windows(2)
        .any(|window| window[0].end_unix_nano > window[1].end_unix_nano)
    {
        rows.sort_by_key(|row| row.end_unix_nano);
    }
}

fn observe_segment_trace_field_refs(
    strings: &mut StringTable,
    fields: &mut Vec<(StringRef, StringRef)>,
    field_name: &str,
    field_value: &str,
) {
    if field_name.is_empty() || field_value.is_empty() {
        return;
    }
    let field_name_ref = strings.intern(field_name);
    let field_value_ref = strings.intern(field_value);
    let _ = push_unique_string_ref_pair(fields, field_name_ref, field_value_ref);
}

fn rebuild_segment_meta(segment_id: u64, segment_path: &Path) -> Result<SegmentMeta, StorageError> {
    let rows = if segment_path.extension().and_then(|value| value.to_str()) == Some("part") {
        read_all_rows_from_part(segment_path)?
    } else {
        read_all_rows_from_row_file(segment_path, segment_id)?
    };

    let mut accumulator = SegmentAccumulator::default();
    for (row_index, (row, _, _)) in rows.into_iter().enumerate() {
        accumulator.observe_row(
            &row,
            SegmentRowLocation {
                segment_id: segment_id
                    .try_into()
                    .map_err(|_| invalid_data("segment id exceeds u32 row-ref limit"))?,
                row_index: row_index as u32,
                start_unix_nano: row.start_unix_nano,
                end_unix_nano: row.end_unix_nano,
            },
        );
    }

    let (min_time_unix_nano, max_time_unix_nano) = accumulator
        .time_range()
        .ok_or_else(|| invalid_data("cannot recover empty segment"))?;

    let segment_file = segment_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or_else(|| invalid_data("segment path has no file name"))?;

    Ok(accumulator.build_meta(
        segment_id,
        segment_file,
        min_time_unix_nano,
        max_time_unix_nano,
    ))
}

fn write_segment_meta_binary(
    meta_path: &Path,
    meta: &SegmentMeta,
    sync_policy: DiskSyncPolicy,
) -> Result<(u64, bool), StorageError> {
    let mut file = File::create(meta_path)?;
    file.write_all(SEGMENT_META_BINARY_MAGIC)?;
    file.write_all(&[SEGMENT_META_BINARY_VERSION])?;
    write_u8(&mut file, META_COMPRESSION_ZSTD)?;
    let payload = serialize_segment_meta_binary_payload(meta)?;
    let compressed = compress_bytes_zstd(&payload, META_ZSTD_LEVEL)?;
    write_u64(&mut file, payload.len() as u64)?;
    write_u32(&mut file, crc32fast::hash(&compressed))?;
    file.write_all(&compressed)?;
    file.flush()?;
    let synced = if sync_policy.requires_sync() {
        file.sync_data()?;
        true
    } else {
        false
    };
    Ok((file.metadata()?.len(), synced))
}

fn read_segment_meta_binary(meta_path: &Path) -> Result<SegmentMeta, StorageError> {
    let mut file = File::open(meta_path)?;
    let mut magic = [0u8; 7];
    file.read_exact(&mut magic)?;
    if magic != SEGMENT_META_BINARY_MAGIC {
        return Err(invalid_data("invalid binary segment meta header").into());
    }
    let version = read_u8(&mut file)?;
    if version != SEGMENT_META_BINARY_VERSION {
        return Err(
            invalid_data(format!("unsupported binary segment meta version {version}")).into(),
        );
    }
    let compression = read_u8(&mut file)?;
    if compression != META_COMPRESSION_ZSTD {
        return Err(invalid_data("unsupported binary segment meta compression").into());
    }
    let uncompressed_len = read_u64(&mut file)?;
    let expected_checksum = read_u32(&mut file)?;
    let mut compressed = Vec::new();
    file.read_to_end(&mut compressed)?;
    if crc32fast::hash(&compressed) != expected_checksum {
        return Err(invalid_data("binary segment meta checksum mismatch").into());
    }
    let payload =
        decompress_bytes_zstd(&compressed, uncompressed_len, "binary segment meta payload")?;
    deserialize_segment_meta_binary_payload(&payload)
}

fn segment_metadata_bytes(segments_path: &Path, segment_id: u64) -> u64 {
    let legacy_meta_path = segment_meta_path(segments_path, segment_id);
    let binary_meta_path = segment_binary_meta_path(segments_path, segment_id);
    fs::metadata(&legacy_meta_path)
        .map(|meta| meta.len())
        .unwrap_or(0)
        .saturating_add(
            fs::metadata(&binary_meta_path)
                .map(|meta| meta.len())
                .unwrap_or(0),
        )
}

fn read_all_rows_from_row_file(
    path: &Path,
    _segment_id: u64,
) -> Result<Vec<(TraceSpanRow, u64, u32)>, StorageError> {
    let mut file = OpenOptions::new().read(true).write(true).open(path)?;
    let (format, mut offset) = detect_row_file_format(&mut file)?;
    let mut rows = Vec::new();
    let mut last_good_end = offset;

    if matches!(format, RowFileFormat::BatchedBinary) {
        loop {
            let batch_offset = offset;
            let mut batch_len_buf = [0u8; LENGTH_PREFIX_BYTES as usize];
            match file.read_exact(&mut batch_len_buf) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                    truncate_invalid_row_tail(&mut file, last_good_end)?;
                    break;
                }
                Err(error) => return Err(error.into()),
            }

            let batch_len = u32::from_le_bytes(batch_len_buf) as usize;
            let mut checksum_bytes = [0u8; WAL_CHECKSUM_BYTES as usize];
            match file.read_exact(&mut checksum_bytes) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                    truncate_invalid_row_tail(&mut file, last_good_end)?;
                    break;
                }
                Err(error) => return Err(error.into()),
            }
            let expected_checksum = u32::from_le_bytes(checksum_bytes);

            let mut batch_body = vec![0u8; batch_len];
            match file.read_exact(&mut batch_body) {
                Ok(()) => {}
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                    truncate_invalid_row_tail(&mut file, last_good_end)?;
                    break;
                }
                Err(error) => return Err(error.into()),
            }

            if crc32fast::hash(&batch_body) != expected_checksum {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }

            if batch_body.len() < WAL_BATCH_ROW_COUNT_BYTES as usize {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }

            let row_count = u32::from_le_bytes(
                batch_body[..WAL_BATCH_ROW_COUNT_BYTES as usize]
                    .try_into()
                    .map_err(|_| invalid_data("wal batch row count truncated"))?,
            ) as usize;
            let length_block_start = WAL_BATCH_ROW_COUNT_BYTES as usize;
            let length_block_len = row_count * LENGTH_PREFIX_BYTES as usize;
            let payload_block_start = length_block_start + length_block_len;
            if payload_block_start > batch_body.len() {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                return Ok(rows);
            }

            let mut payload_offset = payload_block_start;
            for row_index in 0..row_count {
                let len_offset = length_block_start + row_index * LENGTH_PREFIX_BYTES as usize;
                let len_end = len_offset + LENGTH_PREFIX_BYTES as usize;
                let len = match batch_body.get(len_offset..len_end) {
                    Some(value) => u32::from_le_bytes(
                        value
                            .try_into()
                            .map_err(|_| invalid_data("wal row length prefix truncated"))?,
                    ) as usize,
                    None => {
                        truncate_invalid_row_tail(&mut file, last_good_end)?;
                        return Ok(rows);
                    }
                };
                let payload_end = payload_offset + len;
                let payload = match batch_body.get(payload_offset..payload_end) {
                    Some(payload) => payload,
                    None => {
                        truncate_invalid_row_tail(&mut file, last_good_end)?;
                        return Ok(rows);
                    }
                };

                match decode_row_payload(format, payload) {
                    Ok(row) => rows.push((
                        row,
                        batch_offset + wal_batch_record_header_bytes() + payload_offset as u64,
                        len as u32,
                    )),
                    Err(_) => {
                        truncate_invalid_row_tail(&mut file, last_good_end)?;
                        return Ok(rows);
                    }
                }
                payload_offset = payload_end;
            }

            if payload_offset != batch_body.len() {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }

            offset = batch_offset + LENGTH_PREFIX_BYTES + WAL_CHECKSUM_BYTES + batch_len as u64;
            last_good_end = offset;
        }

        return Ok(rows);
    }

    loop {
        let record_offset = offset;
        let mut len_buf = [0u8; LENGTH_PREFIX_BYTES as usize];
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
            Err(error) => return Err(error.into()),
        }

        let len = u32::from_le_bytes(len_buf);
        let expected_checksum = if format.uses_checksum() {
            let mut checksum_bytes = [0u8; WAL_CHECKSUM_BYTES as usize];
            match file.read_exact(&mut checksum_bytes) {
                Ok(()) => Some(u32::from_le_bytes(checksum_bytes)),
                Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                    truncate_invalid_row_tail(&mut file, last_good_end)?;
                    break;
                }
                Err(error) => return Err(error.into()),
            }
        } else {
            None
        };

        let mut payload = vec![0u8; len as usize];
        match file.read_exact(&mut payload) {
            Ok(()) => {}
            Err(error) if error.kind() == ErrorKind::UnexpectedEof => {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
            Err(error) => return Err(error.into()),
        }

        if let Some(expected_checksum) = expected_checksum {
            if crc32fast::hash(&payload) != expected_checksum {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
        }

        match decode_row_payload(format, &payload) {
            Ok(row) => rows.push((row, record_offset, len)),
            Err(_) => {
                truncate_invalid_row_tail(&mut file, last_good_end)?;
                break;
            }
        }
        offset = record_offset + wal_record_storage_bytes_for_format(format, len);
        last_good_end = offset;
    }

    Ok(rows)
}

fn write_batched_binary_row_file(
    path: &Path,
    payload_bytes: &[u8],
    payload_ranges: &[std::ops::Range<usize>],
    sync_policy: DiskSyncPolicy,
) -> Result<(u64, bool), StorageError> {
    let mut file = File::create(path)?;
    file.write_all(WAL_MAGIC)?;
    file.write_all(&[WAL_VERSION_BATCHED_BINARY])?;

    let mut row_index = 0usize;
    while row_index < payload_ranges.len() {
        let chunk_start = row_index;
        let mut chunk_payload_bytes = 0usize;
        let mut chunk_rows = 0usize;
        while row_index < payload_ranges.len() {
            let row_len = payload_ranges[row_index].len();
            let next_payload_bytes = chunk_payload_bytes.saturating_add(row_len);
            let next_rows = chunk_rows + 1;
            let next_body_bytes = wal_batch_body_bytes(next_payload_bytes, next_rows);
            if next_body_bytes > u32::MAX as u64 && chunk_rows > 0 {
                break;
            }
            if next_body_bytes > u32::MAX as u64 {
                return Err(invalid_data("single row exceeds batched row-file size limit").into());
            }
            chunk_payload_bytes = next_payload_bytes;
            chunk_rows = next_rows;
            row_index += 1;
        }

        let batch_body_len = u32::try_from(wal_batch_body_bytes(chunk_payload_bytes, chunk_rows))
            .map_err(|_| invalid_data("batched row-file payload exceeds u32 length"))?;
        let row_count_bytes = (chunk_rows as u32).to_le_bytes();
        let mut row_length_prefixes = Vec::with_capacity(chunk_rows * LENGTH_PREFIX_BYTES as usize);
        let mut checksum = crc32fast::Hasher::new();
        checksum.update(&row_count_bytes);
        for range in &payload_ranges[chunk_start..row_index] {
            let payload_len_bytes = (range.len() as u32).to_le_bytes();
            row_length_prefixes.extend_from_slice(&payload_len_bytes);
        }
        checksum.update(&row_length_prefixes);
        for range in &payload_ranges[chunk_start..row_index] {
            checksum.update(&payload_bytes[range.clone()]);
        }

        file.write_all(&batch_body_len.to_le_bytes())?;
        file.write_all(&checksum.finalize().to_le_bytes())?;
        file.write_all(&row_count_bytes)?;
        file.write_all(&row_length_prefixes)?;
        for range in &payload_ranges[chunk_start..row_index] {
            file.write_all(&payload_bytes[range.clone()])?;
        }
    }

    file.flush()?;
    let synced = if sync_policy.requires_sync() {
        file.sync_data()?;
        true
    } else {
        false
    };
    Ok((file.metadata()?.len(), synced))
}

fn read_all_rows_from_part(path: &Path) -> Result<Vec<(TraceSpanRow, u64, u32)>, StorageError> {
    let part = read_part(path)?;
    Ok(part
        .rows()?
        .into_iter()
        .enumerate()
        .map(|(row_index, row)| (row, row_index as u64, 0))
        .collect())
}

fn read_all_rows_from_persisted_segment(path: &Path) -> Result<Vec<TraceSpanRow>, StorageError> {
    if path.extension().and_then(|value| value.to_str()) == Some("part") {
        return Ok(read_all_rows_from_part(path)?
            .into_iter()
            .map(|(row, _, _)| row)
            .collect());
    }
    Ok(read_all_rows_from_row_file(path, 0)?
        .into_iter()
        .map(|(row, _, _)| row)
        .collect())
}

fn read_rows_for_segment(
    path: &Path,
    row_refs: &[SegmentRowLocation],
) -> Result<Vec<TraceSpanRow>, StorageError> {
    if path.extension().and_then(|value| value.to_str()) == Some("part") {
        let row_indexes: Vec<u32> = row_refs.iter().map(|row_ref| row_ref.row_index).collect();
        return read_rows_by_indexes_from_part(path, &row_indexes);
    }

    let wanted: FxHashSet<u32> = row_refs.iter().map(|row_ref| row_ref.row_index).collect();
    Ok(read_all_rows_from_row_file(path, 0)?
        .into_iter()
        .enumerate()
        .filter_map(|(row_index, (row, _, _))| wanted.contains(&(row_index as u32)).then_some(row))
        .collect())
}

fn decode_row_payload(format: RowFileFormat, payload: &[u8]) -> Result<TraceSpanRow, StorageError> {
    match format {
        RowFileFormat::Legacy | RowFileFormat::ChecksummedJson => {
            Ok(serde_json::from_slice(payload)?)
        }
        RowFileFormat::ChecksummedBinary | RowFileFormat::BatchedBinary => {
            decode_trace_row(payload)
                .map_err(|error| StorageError::Io(invalid_data(error.to_string())))
        }
    }
}

fn read_rows_by_indexes_from_part(
    path: &Path,
    row_indexes: &[u32],
) -> Result<Vec<TraceSpanRow>, StorageError> {
    if row_indexes.is_empty() {
        return Ok(Vec::new());
    }

    let mut file = File::open(path)?;
    let part_version = validate_and_skip_part_header(&mut file)?;

    let row_count = read_u32(&mut file)? as usize;
    let normalized_indexes = normalize_selected_indexes(row_indexes, row_count)?;
    let trace_ids = read_required_string_values_for_rows(&mut file, &normalized_indexes)?;
    let span_ids = read_required_string_values_for_rows(&mut file, &normalized_indexes)?;
    let parent_span_ids = read_optional_string_values_for_rows(&mut file, &normalized_indexes)?;
    let names = read_required_string_values_for_rows(&mut file, &normalized_indexes)?;
    let start_unix_nanos = read_i64_values_for_rows(&mut file, &normalized_indexes)?;
    let end_unix_nanos = read_i64_values_for_rows(&mut file, &normalized_indexes)?;
    let _time_unix_nanos = read_i64_values_for_rows(&mut file, &normalized_indexes)?;
    let field_count = read_u32(&mut file)? as usize;
    let mut field_values_by_row = vec![Vec::new(); normalized_indexes.len()];
    for _ in 0..field_count {
        let field_name = read_string(&mut file)?;
        let values = read_part_field_values_for_rows(&mut file, part_version, &normalized_indexes)?;
        for (row_position, value) in values.into_iter().enumerate() {
            if let Some(value) = value {
                field_values_by_row[row_position]
                    .push(vtcore::Field::new(field_name.clone(), value));
            }
        }
    }

    let mut rows = Vec::with_capacity(normalized_indexes.len());
    for row_position in 0..normalized_indexes.len() {
        rows.push(
            TraceSpanRow::new(
                trace_ids[row_position].clone(),
                span_ids[row_position].clone(),
                parent_span_ids[row_position].clone(),
                names[row_position].clone(),
                start_unix_nanos[row_position],
                end_unix_nanos[row_position],
                std::mem::take(&mut field_values_by_row[row_position]),
            )
            .map_err(|error| invalid_data(error.to_string()))?,
        );
    }
    Ok(rows)
}

fn build_segment_meta_from_rows(
    segment_id: u64,
    segment_path: &Path,
    rows: &[TraceSpanRow],
) -> Result<SegmentMeta, StorageError> {
    let mut accumulator = SegmentAccumulator::default();
    for (row_index, row) in rows.iter().enumerate() {
        accumulator.observe_row(
            row,
            SegmentRowLocation {
                segment_id: segment_id
                    .try_into()
                    .map_err(|_| invalid_data("segment id exceeds u32 row-ref limit"))?,
                row_index: row_index as u32,
                start_unix_nano: row.start_unix_nano,
                end_unix_nano: row.end_unix_nano,
            },
        );
    }

    let (min_time_unix_nano, max_time_unix_nano) = accumulator
        .time_range()
        .ok_or_else(|| invalid_data("cannot build meta for empty compacted segment"))?;
    let segment_file = segment_path
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .ok_or_else(|| invalid_data("segment path has no file name"))?;

    Ok(accumulator.build_meta(
        segment_id,
        segment_file,
        min_time_unix_nano,
        max_time_unix_nano,
    ))
}

fn compact_persisted_segments(
    segments_path: &Path,
    next_segment_id: &mut u64,
    target_size_bytes: u64,
) -> Result<bool, StorageError> {
    let mut segment_files: HashMap<u64, SegmentFileSet> = HashMap::new();
    for entry in fs::read_dir(segments_path)? {
        let entry = entry?;
        let path = entry.path();
        let Some(file_name) = path
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
        else {
            continue;
        };
        let Some((segment_id, kind)) = parse_segment_file_name(&file_name) else {
            continue;
        };
        let files = segment_files.entry(segment_id).or_default();
        match kind {
            SegmentFileKind::Wal => files.wal_path = Some(path),
            SegmentFileKind::Part => files.part_path = Some(path),
            SegmentFileKind::LegacyRows => files.legacy_rows_path = Some(path),
            SegmentFileKind::MetaJson => files.meta_json_path = Some(path),
            SegmentFileKind::MetaBinary => files.meta_bin_path = Some(path),
        }
    }

    let mut candidates = Vec::new();
    for (segment_id, files) in segment_files {
        let Some(data_path) = files
            .part_path
            .or(files.wal_path)
            .or(files.legacy_rows_path)
        else {
            continue;
        };
        let mut metadata_paths = Vec::new();
        if let Some(meta_json_path) = files.meta_json_path {
            metadata_paths.push(meta_json_path);
        }
        if let Some(meta_bin_path) = files.meta_bin_path {
            metadata_paths.push(meta_bin_path);
        }
        let total_bytes = fs::metadata(&data_path)?.len()
            + metadata_paths
                .iter()
                .map(|path| fs::metadata(path).map(|meta| meta.len()).unwrap_or(0))
                .sum::<u64>();
        candidates.push(CompactableSegment {
            segment_id,
            data_path,
            metadata_paths,
            total_bytes,
        });
    }

    candidates.sort_by_key(|candidate| candidate.segment_id);

    let mut changed = false;
    let mut group = Vec::new();
    let mut group_bytes = 0u64;

    for candidate in candidates {
        let would_exceed_target = !group.is_empty()
            && group_bytes.saturating_add(candidate.total_bytes) > target_size_bytes;
        if would_exceed_target && group.len() >= 2 {
            compact_segment_group(segments_path, &group, next_segment_id)?;
            changed = true;
            group.clear();
            group_bytes = 0;
        }

        group_bytes = group_bytes.saturating_add(candidate.total_bytes);
        group.push(candidate);
    }

    if group.len() >= 2 {
        compact_segment_group(segments_path, &group, next_segment_id)?;
        changed = true;
    }

    Ok(changed)
}

fn compact_segment_group(
    segments_path: &Path,
    group: &[CompactableSegment],
    next_segment_id: &mut u64,
) -> Result<(), StorageError> {
    let mut rows = Vec::new();
    for segment in group {
        rows.extend(read_all_rows_from_persisted_segment(&segment.data_path)?);
    }

    if rows.is_empty() {
        return Ok(());
    }

    let compacted_segment_id = *next_segment_id;
    *next_segment_id += 1;
    let compacted_part_path = segment_part_path(segments_path, compacted_segment_id);
    let compacted_meta_path = segment_binary_meta_path(segments_path, compacted_segment_id);
    let compacted_part = ColumnarPart::from_rows(&rows);
    let _ = write_part(&compacted_part_path, &compacted_part, DiskSyncPolicy::None)?;
    let compacted_meta =
        build_segment_meta_from_rows(compacted_segment_id, &compacted_part_path, &rows)?;
    let _ = write_segment_meta_binary(&compacted_meta_path, &compacted_meta, DiskSyncPolicy::None)?;

    for segment in group {
        fs::remove_file(&segment.data_path)?;
        for metadata_path in &segment.metadata_paths {
            if metadata_path.exists() {
                fs::remove_file(metadata_path)?;
            }
        }
    }

    Ok(())
}

fn read_part(path: &Path) -> Result<ColumnarPart, StorageError> {
    let mut file = File::open(path)?;
    let part_version = validate_and_skip_part_header(&mut file)?;
    read_columnar_part(&mut file, part_version)
}

fn write_part(
    path: &Path,
    part: &ColumnarPart,
    sync_policy: DiskSyncPolicy,
) -> Result<(u64, bool), StorageError> {
    let mut file = File::create(path)?;
    file.write_all(PART_MAGIC)?;
    file.write_all(&[PART_VERSION])?;
    write_columnar_part(&mut file, part)?;
    file.flush()?;
    let synced = if sync_policy.requires_sync() {
        file.sync_data()?;
        true
    } else {
        false
    };
    Ok((fs::metadata(path)?.len(), synced))
}

fn write_columnar_part(writer: &mut File, part: &ColumnarPart) -> Result<(), StorageError> {
    write_u32(writer, part.row_count)?;
    write_required_string_column(writer, &part.trace_ids)?;
    write_required_string_column(writer, &part.span_ids)?;
    write_optional_string_column(writer, &part.parent_span_ids)?;
    write_required_string_column(writer, &part.names)?;
    write_i64_vec(writer, &part.start_unix_nanos)?;
    write_i64_vec(writer, &part.end_unix_nanos)?;
    write_i64_vec(writer, &part.time_unix_nanos)?;
    write_u32(writer, part.field_columns.len() as u32)?;
    for field_column in &part.field_columns {
        write_string(writer, &field_column.name)?;
        write_u8(writer, field_column.values.type_code())?;
        write_part_field_column_values(writer, &field_column.values)?;
    }
    Ok(())
}

fn read_columnar_part(reader: &mut File, part_version: u8) -> Result<ColumnarPart, StorageError> {
    let row_count = read_u32(reader)?;
    let trace_ids = read_required_string_column(reader)?;
    let span_ids = read_required_string_column(reader)?;
    let parent_span_ids = read_optional_string_column(reader)?;
    let names = read_required_string_column(reader)?;
    let start_unix_nanos = read_i64_vec(reader)?;
    let end_unix_nanos = read_i64_vec(reader)?;
    let time_unix_nanos = read_i64_vec(reader)?;
    let field_count = read_u32(reader)? as usize;
    let mut field_columns = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        field_columns.push(PartFieldColumn {
            name: read_string(reader)?,
            values: read_part_field_column_values(reader, part_version)?,
        });
    }

    Ok(ColumnarPart {
        row_count,
        trace_ids,
        span_ids,
        parent_span_ids,
        names,
        start_unix_nanos,
        end_unix_nanos,
        time_unix_nanos,
        field_columns,
    })
}

fn write_required_string_column(
    writer: &mut File,
    column: &DictionaryStringColumn,
) -> Result<(), StorageError> {
    write_u32(writer, column.dictionary.len() as u32)?;
    for value in &column.dictionary {
        write_string(writer, value)?;
    }
    write_u32(writer, column.indexes.len() as u32)?;
    for index in &column.indexes {
        write_u32(writer, *index)?;
    }
    Ok(())
}

fn normalize_selected_indexes(
    row_indexes: &[u32],
    row_count: usize,
) -> Result<Vec<usize>, StorageError> {
    let mut normalized = Vec::with_capacity(row_indexes.len());
    for row_index in row_indexes {
        let row_index = *row_index as usize;
        if row_index >= row_count {
            return Err(invalid_data("row index out of bounds").into());
        }
        normalized.push(row_index);
    }
    Ok(normalized)
}

fn read_required_string_values_for_rows(
    reader: &mut File,
    row_indexes: &[usize],
) -> Result<Vec<String>, StorageError> {
    let dictionary_len = read_u32(reader)? as usize;
    let mut dictionary = Vec::with_capacity(dictionary_len);
    for _ in 0..dictionary_len {
        dictionary.push(read_string(reader)?);
    }

    let index_len = read_u32(reader)? as usize;
    let index_block_start = reader.stream_position()?;
    let selected = read_u32_values_at_indexes(reader, index_block_start, index_len, row_indexes)?
        .into_iter()
        .map(|dictionary_index| {
            dictionary
                .get(dictionary_index as usize)
                .cloned()
                .ok_or_else(|| invalid_data("dictionary index out of bounds").into())
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    reader.seek(SeekFrom::Start(
        index_block_start + (index_len as u64 * std::mem::size_of::<u32>() as u64),
    ))?;
    Ok(selected)
}

fn read_optional_string_values_for_rows(
    reader: &mut File,
    row_indexes: &[usize],
) -> Result<Vec<Option<String>>, StorageError> {
    let dictionary_len = read_u32(reader)? as usize;
    let mut dictionary = Vec::with_capacity(dictionary_len);
    for _ in 0..dictionary_len {
        dictionary.push(read_string(reader)?);
    }

    let index_len = read_u32(reader)? as usize;
    let index_block_start = reader.stream_position()?;
    let selected = read_i32_values_at_indexes(reader, index_block_start, index_len, row_indexes)?
        .into_iter()
        .map(|dictionary_index| {
            if dictionary_index < 0 {
                Ok(None)
            } else {
                dictionary
                    .get(dictionary_index as usize)
                    .cloned()
                    .map(Some)
                    .ok_or_else(|| invalid_data("optional dictionary index out of bounds").into())
            }
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    reader.seek(SeekFrom::Start(
        index_block_start + (index_len as u64 * std::mem::size_of::<i32>() as u64),
    ))?;
    Ok(selected)
}

fn read_part_field_values_for_rows(
    reader: &mut File,
    part_version: u8,
    row_indexes: &[usize],
) -> Result<Vec<Option<String>>, StorageError> {
    if part_version < 2 {
        return read_optional_string_values_for_rows(reader, row_indexes);
    }

    match read_u8(reader)? {
        PART_FIELD_TYPE_STRING => read_optional_string_values_for_rows(reader, row_indexes),
        PART_FIELD_TYPE_I64 => Ok(read_optional_i64_values_for_rows(reader, row_indexes)?
            .into_iter()
            .map(|value| value.map(|value| value.to_string()))
            .collect()),
        PART_FIELD_TYPE_BOOL => Ok(read_optional_bool_values_for_rows(reader, row_indexes)?
            .into_iter()
            .map(|value| value.map(|value| value.to_string()))
            .collect()),
        _ => Err(invalid_data("unsupported part field column type").into()),
    }
}

fn read_i64_values_for_rows(
    reader: &mut File,
    row_indexes: &[usize],
) -> Result<Vec<i64>, StorageError> {
    let len = read_u32(reader)? as usize;
    let value_block_start = reader.stream_position()?;
    let selected = read_i64_values_at_indexes(reader, value_block_start, len, row_indexes)?;
    reader.seek(SeekFrom::Start(
        value_block_start + (len as u64 * std::mem::size_of::<i64>() as u64),
    ))?;
    Ok(selected)
}

fn read_optional_i64_values_for_rows(
    reader: &mut File,
    row_indexes: &[usize],
) -> Result<Vec<Option<i64>>, StorageError> {
    let len = read_u32(reader)? as usize;
    let presence_block_start = reader.stream_position()?;
    let presence = read_u8_values_at_indexes(reader, presence_block_start, len, row_indexes)?;
    let value_block_start = presence_block_start + len as u64;
    let values = read_i64_values_at_indexes(reader, value_block_start, len, row_indexes)?;
    reader.seek(SeekFrom::Start(
        value_block_start + (len as u64 * std::mem::size_of::<i64>() as u64),
    ))?;
    Ok(presence
        .into_iter()
        .zip(values)
        .map(|(presence, value)| if presence == 0 { None } else { Some(value) })
        .collect())
}

fn read_optional_bool_values_for_rows(
    reader: &mut File,
    row_indexes: &[usize],
) -> Result<Vec<Option<bool>>, StorageError> {
    let len = read_u32(reader)? as usize;
    let block_start = reader.stream_position()?;
    let values = read_i8_values_at_indexes(reader, block_start, len, row_indexes)?;
    reader.seek(SeekFrom::Start(block_start + len as u64))?;
    values
        .into_iter()
        .map(|value| match value {
            -1 => Ok(None),
            0 => Ok(Some(false)),
            1 => Ok(Some(true)),
            _ => Err(invalid_data("invalid bool column marker").into()),
        })
        .collect()
}

fn read_u32_values_at_indexes(
    reader: &mut File,
    block_start: u64,
    len: usize,
    row_indexes: &[usize],
) -> Result<Vec<u32>, StorageError> {
    let mut values = Vec::with_capacity(row_indexes.len());
    for row_index in row_indexes {
        if *row_index >= len {
            return Err(invalid_data("u32 column index out of bounds").into());
        }
        reader.seek(SeekFrom::Start(
            block_start + (*row_index as u64 * std::mem::size_of::<u32>() as u64),
        ))?;
        values.push(read_u32(reader)?);
    }
    Ok(values)
}

fn read_u8_values_at_indexes(
    reader: &mut File,
    block_start: u64,
    len: usize,
    row_indexes: &[usize],
) -> Result<Vec<u8>, StorageError> {
    let mut values = Vec::with_capacity(row_indexes.len());
    for row_index in row_indexes {
        if *row_index >= len {
            return Err(invalid_data("u8 column index out of bounds").into());
        }
        reader.seek(SeekFrom::Start(block_start + *row_index as u64))?;
        let mut value = [0u8; 1];
        reader.read_exact(&mut value)?;
        values.push(value[0]);
    }
    Ok(values)
}

fn read_i8_values_at_indexes(
    reader: &mut File,
    block_start: u64,
    len: usize,
    row_indexes: &[usize],
) -> Result<Vec<i8>, StorageError> {
    let mut values = Vec::with_capacity(row_indexes.len());
    for row_index in row_indexes {
        if *row_index >= len {
            return Err(invalid_data("i8 column index out of bounds").into());
        }
        reader.seek(SeekFrom::Start(block_start + *row_index as u64))?;
        let mut value = [0u8; 1];
        reader.read_exact(&mut value)?;
        values.push(value[0] as i8);
    }
    Ok(values)
}

fn read_i32_values_at_indexes(
    reader: &mut File,
    block_start: u64,
    len: usize,
    row_indexes: &[usize],
) -> Result<Vec<i32>, StorageError> {
    let mut values = Vec::with_capacity(row_indexes.len());
    for row_index in row_indexes {
        if *row_index >= len {
            return Err(invalid_data("i32 column index out of bounds").into());
        }
        reader.seek(SeekFrom::Start(
            block_start + (*row_index as u64 * std::mem::size_of::<i32>() as u64),
        ))?;
        values.push(read_i32(reader)?);
    }
    Ok(values)
}

fn read_i64_values_at_indexes(
    reader: &mut File,
    block_start: u64,
    len: usize,
    row_indexes: &[usize],
) -> Result<Vec<i64>, StorageError> {
    let mut values = Vec::with_capacity(row_indexes.len());
    for row_index in row_indexes {
        if *row_index >= len {
            return Err(invalid_data("i64 column index out of bounds").into());
        }
        reader.seek(SeekFrom::Start(
            block_start + (*row_index as u64 * std::mem::size_of::<i64>() as u64),
        ))?;
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        values.push(i64::from_le_bytes(bytes));
    }
    Ok(values)
}

fn read_required_string_column(reader: &mut File) -> Result<DictionaryStringColumn, StorageError> {
    let dictionary_len = read_u32(reader)? as usize;
    let mut dictionary = Vec::with_capacity(dictionary_len);
    for _ in 0..dictionary_len {
        dictionary.push(read_string(reader)?);
    }

    let index_len = read_u32(reader)? as usize;
    let mut indexes = Vec::with_capacity(index_len);
    for _ in 0..index_len {
        indexes.push(read_u32(reader)?);
    }

    Ok(DictionaryStringColumn {
        dictionary,
        indexes,
    })
}

fn write_optional_string_column(
    writer: &mut File,
    column: &DictionaryOptionalStringColumn,
) -> Result<(), StorageError> {
    write_u32(writer, column.dictionary.len() as u32)?;
    for value in &column.dictionary {
        write_string(writer, value)?;
    }
    write_u32(writer, column.indexes.len() as u32)?;
    for index in &column.indexes {
        match index {
            Some(value) => write_i32(writer, *value as i32)?,
            None => write_i32(writer, -1)?,
        }
    }
    Ok(())
}

fn read_optional_string_column(
    reader: &mut File,
) -> Result<DictionaryOptionalStringColumn, StorageError> {
    let dictionary_len = read_u32(reader)? as usize;
    let mut dictionary = Vec::with_capacity(dictionary_len);
    for _ in 0..dictionary_len {
        dictionary.push(read_string(reader)?);
    }

    let index_len = read_u32(reader)? as usize;
    let mut indexes = Vec::with_capacity(index_len);
    for _ in 0..index_len {
        let value = read_i32(reader)?;
        indexes.push(if value < 0 { None } else { Some(value as u32) });
    }

    Ok(DictionaryOptionalStringColumn {
        dictionary,
        indexes,
    })
}

fn write_part_field_column_values(
    writer: &mut File,
    values: &PartFieldColumnValues,
) -> Result<(), StorageError> {
    match values {
        PartFieldColumnValues::String(column) => write_optional_string_column(writer, column),
        PartFieldColumnValues::I64(column) => write_optional_i64_column(writer, column),
        PartFieldColumnValues::Bool(column) => write_optional_bool_column(writer, column),
    }
}

fn read_part_field_column_values(
    reader: &mut File,
    part_version: u8,
) -> Result<PartFieldColumnValues, StorageError> {
    if part_version < 2 {
        return Ok(PartFieldColumnValues::String(read_optional_string_column(
            reader,
        )?));
    }

    match read_u8(reader)? {
        PART_FIELD_TYPE_STRING => Ok(PartFieldColumnValues::String(read_optional_string_column(
            reader,
        )?)),
        PART_FIELD_TYPE_I64 => Ok(PartFieldColumnValues::I64(read_optional_i64_column(
            reader,
        )?)),
        PART_FIELD_TYPE_BOOL => Ok(PartFieldColumnValues::Bool(read_optional_bool_column(
            reader,
        )?)),
        _ => Err(invalid_data("unsupported part field column type").into()),
    }
}

fn write_optional_i64_column(
    writer: &mut File,
    column: &OptionalI64Column,
) -> Result<(), StorageError> {
    write_u32(writer, column.values.len() as u32)?;
    for value in &column.values {
        write_u8(writer, u8::from(value.is_some()))?;
    }
    for value in &column.values {
        writer.write_all(&value.unwrap_or_default().to_le_bytes())?;
    }
    Ok(())
}

fn read_optional_i64_column(reader: &mut File) -> Result<OptionalI64Column, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut presence = Vec::with_capacity(len);
    for _ in 0..len {
        presence.push(read_u8(reader)?);
    }
    let mut values = Vec::with_capacity(len);
    for marker in presence {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        let value = i64::from_le_bytes(bytes);
        values.push(if marker == 0 { None } else { Some(value) });
    }
    Ok(OptionalI64Column { values })
}

fn write_optional_bool_column(
    writer: &mut File,
    column: &OptionalBoolColumn,
) -> Result<(), StorageError> {
    write_u32(writer, column.values.len() as u32)?;
    for value in &column.values {
        let marker = match value {
            Some(true) => 1i8,
            Some(false) => 0i8,
            None => -1i8,
        };
        write_i8(writer, marker)?;
    }
    Ok(())
}

fn read_optional_bool_column(reader: &mut File) -> Result<OptionalBoolColumn, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(match read_i8(reader)? {
            -1 => None,
            0 => Some(false),
            1 => Some(true),
            _ => return Err(invalid_data("invalid bool column marker").into()),
        });
    }
    Ok(OptionalBoolColumn { values })
}

fn write_i64_vec<W: Write>(writer: &mut W, values: &[i64]) -> Result<(), StorageError> {
    write_u32(writer, values.len() as u32)?;
    for value in values {
        writer.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}

fn read_i64_vec<R: Read>(reader: &mut R) -> Result<Vec<i64>, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        values.push(i64::from_le_bytes(bytes));
    }
    Ok(values)
}

fn write_string<W: Write>(writer: &mut W, value: &str) -> Result<(), StorageError> {
    write_u32(writer, value.len() as u32)?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

fn read_string<R: Read>(reader: &mut R) -> Result<String, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|error| invalid_data(error.to_string()).into())
}

fn write_u32<W: Write>(writer: &mut W, value: u32) -> Result<(), StorageError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u8<W: Write>(writer: &mut W, value: u8) -> Result<(), StorageError> {
    writer.write_all(&[value])?;
    Ok(())
}

fn read_u32<R: Read>(reader: &mut R) -> Result<u32, StorageError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u8<R: Read>(reader: &mut R) -> Result<u8, StorageError> {
    let mut bytes = [0u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn write_i32<W: Write>(writer: &mut W, value: i32) -> Result<(), StorageError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_i8<W: Write>(writer: &mut W, value: i8) -> Result<(), StorageError> {
    writer.write_all(&[value as u8])?;
    Ok(())
}

fn read_i8<R: Read>(reader: &mut R) -> Result<i8, StorageError> {
    let mut bytes = [0u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0] as i8)
}

fn read_i32<R: Read>(reader: &mut R) -> Result<i32, StorageError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(i32::from_le_bytes(bytes))
}

fn write_u64<W: Write>(writer: &mut W, value: u64) -> Result<(), StorageError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn read_u64<R: Read>(reader: &mut R) -> Result<u64, StorageError> {
    let mut bytes = [0u8; 8];
    reader.read_exact(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes))
}

fn write_len<W: Write>(writer: &mut W, len: usize, what: &str) -> Result<(), StorageError> {
    let len = u32::try_from(len).map_err(|_| invalid_data(format!("{what} length exceeds u32")))?;
    write_u32(writer, len)
}

fn read_len<R: Read>(reader: &mut R) -> Result<usize, StorageError> {
    Ok(read_u32(reader)? as usize)
}

fn write_u32_vec<W: Write>(writer: &mut W, values: &[u32]) -> Result<(), StorageError> {
    write_len(writer, values.len(), "u32 vec")?;
    for value in values {
        write_u32(writer, *value)?;
    }
    Ok(())
}

fn read_u32_vec<R: Read>(reader: &mut R) -> Result<Vec<u32>, StorageError> {
    let len = read_len(reader)?;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_u32(reader)?);
    }
    Ok(values)
}

fn read_string_vec<R: Read>(reader: &mut R) -> Result<Vec<String>, StorageError> {
    let len = read_len(reader)?;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_string(reader)?);
    }
    Ok(values)
}

fn write_string_table_binary<W: Write>(
    writer: &mut W,
    table: &StringTable,
) -> Result<(), StorageError> {
    write_len(writer, table.values_by_id.len(), "string table")?;
    for value in &table.values_by_id {
        write_string(writer, value.as_ref())?;
    }
    Ok(())
}

fn read_string_table_binary<R: Read>(reader: &mut R) -> Result<StringTable, StorageError> {
    let values = read_string_vec(reader)?;
    Ok(StringTable::from_values(values))
}

fn write_roaring_bitmap_binary<W: Write>(
    writer: &mut W,
    bitmap: &RoaringBitmap,
) -> Result<(), StorageError> {
    bitmap.serialize_into(writer).map_err(StorageError::from)?;
    Ok(())
}

fn read_roaring_bitmap_binary<R: Read>(reader: &mut R) -> Result<RoaringBitmap, StorageError> {
    RoaringBitmap::deserialize_from(reader).map_err(StorageError::from)
}

fn write_trace_window_bounds_binary<W: Write>(
    writer: &mut W,
    bounds: TraceWindowBounds,
) -> Result<(), StorageError> {
    writer.write_all(&bounds.start_unix_nano.to_le_bytes())?;
    writer.write_all(&bounds.end_unix_nano.to_le_bytes())?;
    Ok(())
}

fn read_trace_window_bounds_binary<R: Read>(
    reader: &mut R,
) -> Result<TraceWindowBounds, StorageError> {
    let mut start = [0u8; 8];
    let mut end = [0u8; 8];
    reader.read_exact(&mut start)?;
    reader.read_exact(&mut end)?;
    Ok(TraceWindowBounds::new(
        i64::from_le_bytes(start),
        i64::from_le_bytes(end),
    ))
}

fn write_segment_row_location_binary<W: Write>(
    writer: &mut W,
    row: SegmentRowLocation,
) -> Result<(), StorageError> {
    write_u32(writer, row.segment_id)?;
    write_u32(writer, row.row_index)?;
    writer.write_all(&row.start_unix_nano.to_le_bytes())?;
    writer.write_all(&row.end_unix_nano.to_le_bytes())?;
    Ok(())
}

fn read_segment_row_location_binary<R: Read>(
    reader: &mut R,
) -> Result<SegmentRowLocation, StorageError> {
    let segment_id = read_u32(reader)?;
    let row_index = read_u32(reader)?;
    let mut start = [0u8; 8];
    let mut end = [0u8; 8];
    reader.read_exact(&mut start)?;
    reader.read_exact(&mut end)?;
    Ok(SegmentRowLocation {
        segment_id,
        row_index,
        start_unix_nano: i64::from_le_bytes(start),
        end_unix_nano: i64::from_le_bytes(end),
    })
}

fn write_segment_row_locations_binary<W: Write>(
    writer: &mut W,
    rows: &[SegmentRowLocation],
) -> Result<(), StorageError> {
    write_len(writer, rows.len(), "segment row locations")?;
    for row in rows {
        write_segment_row_location_binary(writer, *row)?;
    }
    Ok(())
}

fn read_segment_row_locations_binary<R: Read>(
    reader: &mut R,
) -> Result<Vec<SegmentRowLocation>, StorageError> {
    let len = read_len(reader)?;
    let mut rows = Vec::with_capacity(len);
    for _ in 0..len {
        rows.push(read_segment_row_location_binary(reader)?);
    }
    Ok(rows)
}

fn write_persisted_segment_summaries_binary<W: Write>(
    writer: &mut W,
    summaries: &FxHashMap<u64, PersistedSegmentSummary>,
) -> Result<(), StorageError> {
    let mut entries = summaries.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(segment_id, _)| **segment_id);
    write_len(writer, entries.len(), "persisted segment summaries")?;
    for (segment_id, summary) in entries {
        write_u64(writer, *segment_id)?;
        write_trace_window_bounds_binary(writer, summary.window)?;
        write_u32_vec(writer, &summary.services)?;
        write_u32_vec(writer, &summary.operations)?;
        let mut field_entries = summary.field_values_by_name.iter().collect::<Vec<_>>();
        field_entries.sort_by_key(|(field_name_ref, _)| **field_name_ref);
        write_len(writer, field_entries.len(), "persisted segment summary fields")?;
        for (field_name_ref, field_value_refs) in field_entries {
            write_u32(writer, *field_name_ref)?;
            write_u32_vec(writer, field_value_refs)?;
        }
        write_len(writer, summary.traces.len(), "persisted segment trace summaries")?;
        for trace in &summary.traces {
            write_u32(writer, trace.trace_ref)?;
            write_trace_window_bounds_binary(writer, trace.window)?;
            write_u32_vec(writer, &trace.services)?;
            write_u32_vec(writer, &trace.operations)?;
            let mut trace_field_entries = trace.field_values_by_name.iter().collect::<Vec<_>>();
            trace_field_entries.sort_by_key(|(field_name_ref, _)| **field_name_ref);
            write_len(
                writer,
                trace_field_entries.len(),
                "persisted trace summary fields",
            )?;
            for (field_name_ref, field_value_refs) in trace_field_entries {
                write_u32(writer, *field_name_ref)?;
                write_u32_vec(writer, field_value_refs)?;
            }
        }
    }
    Ok(())
}

fn read_persisted_segment_summaries_binary<R: Read>(
    reader: &mut R,
    version: u8,
) -> Result<FxHashMap<u64, PersistedSegmentSummary>, StorageError> {
    let mut summaries = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        let segment_id = read_u64(reader)?;
        let window = read_trace_window_bounds_binary(reader)?;
        let services = read_u32_vec(reader)?;
        let operations = read_u32_vec(reader)?;
        let mut field_values_by_name = FxHashMap::default();
        for _ in 0..read_len(reader)? {
            field_values_by_name.insert(read_u32(reader)?, read_u32_vec(reader)?);
        }
        let mut traces = Vec::new();
        if version >= 3 {
            let trace_count = read_len(reader)?;
            traces = Vec::with_capacity(trace_count);
            for _ in 0..trace_count {
                let trace_ref = read_u32(reader)?;
                let trace_window = read_trace_window_bounds_binary(reader)?;
                let services = read_u32_vec(reader)?;
                let operations = read_u32_vec(reader)?;
                let mut trace_field_values_by_name = FxHashMap::default();
                for _ in 0..read_len(reader)? {
                    trace_field_values_by_name.insert(read_u32(reader)?, read_u32_vec(reader)?);
                }
                traces.push(PersistedTraceSummary {
                    trace_ref,
                    window: trace_window,
                    services,
                    operations,
                    field_values_by_name: trace_field_values_by_name,
                });
            }
        }
        summaries.insert(
            segment_id,
            PersistedSegmentSummary {
                window,
                services,
                operations,
                field_values_by_name,
                traces,
            },
        );
    }
    Ok(summaries)
}

fn write_disk_trace_shard_state_binary<W: Write>(
    writer: &mut W,
    shard: &DiskTraceShardState,
) -> Result<(), StorageError> {
    write_string_table_binary(writer, &shard.trace_ids)?;
    write_string_table_binary(writer, &shard.strings)?;
    write_roaring_bitmap_binary(writer, &shard.all_trace_refs)?;

    let mut windows = shard.windows_by_trace.iter().collect::<Vec<_>>();
    windows.sort_by_key(|(trace_ref, _)| **trace_ref);
    write_len(writer, windows.len(), "trace windows")?;
    for (trace_ref, bounds) in windows {
        write_u32(writer, *trace_ref)?;
        write_trace_window_bounds_binary(writer, *bounds)?;
    }

    let mut rows_by_trace = shard.row_refs_by_trace.iter().collect::<Vec<_>>();
    rows_by_trace.sort_by_key(|(trace_ref, _)| **trace_ref);
    write_len(writer, rows_by_trace.len(), "row refs by trace")?;
    for (trace_ref, rows) in rows_by_trace {
        write_u32(writer, *trace_ref)?;
        write_segment_row_locations_binary(writer, rows)?;
    }

    write_persisted_segment_summaries_binary(writer, &shard.persisted_segments)?;
    write_u64(writer, shard.rows_ingested)?;
    Ok(())
}

fn read_disk_trace_shard_state_binary<R: Read>(
    reader: &mut R,
    version: u8,
) -> Result<DiskTraceShardState, StorageError> {
    if version >= 3 {
        return read_disk_trace_shard_state_binary_v3(reader, version);
    }
    read_disk_trace_shard_state_binary_legacy(reader, version)
}

fn read_disk_trace_shard_state_binary_v3<R: Read>(
    reader: &mut R,
    version: u8,
) -> Result<DiskTraceShardState, StorageError> {
    let trace_ids = read_string_table_binary(reader)?;
    let strings = read_string_table_binary(reader)?;
    let all_trace_refs = read_roaring_bitmap_binary(reader)?;

    let mut windows_by_trace = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        windows_by_trace.insert(read_u32(reader)?, read_trace_window_bounds_binary(reader)?);
    }

    let mut row_refs_by_trace = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        row_refs_by_trace.insert(
            read_u32(reader)?,
            read_segment_row_locations_binary(reader)?,
        );
    }

    let persisted_segments = if version >= 2 {
        read_persisted_segment_summaries_binary(reader, version)?
    } else {
        FxHashMap::default()
    };

    Ok(DiskTraceShardState {
        trace_ids,
        strings,
        all_trace_refs,
        windows_by_trace,
        persisted_segments,
        services_by_trace: FxHashMap::default(),
        trace_refs_by_service: FxHashMap::default(),
        trace_refs_by_operation: FxHashMap::default(),
        trace_refs_by_field_name_value: FxHashMap::default(),
        field_values_by_name: FxHashMap::default(),
        row_refs_by_trace,
        rows_ingested: read_u64(reader)?,
    })
}

fn read_disk_trace_shard_state_binary_legacy<R: Read>(
    reader: &mut R,
    version: u8,
) -> Result<DiskTraceShardState, StorageError> {
    let trace_ids = read_string_table_binary(reader)?;
    let strings = read_string_table_binary(reader)?;
    let all_trace_refs = read_roaring_bitmap_binary(reader)?;

    let mut windows_by_trace = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        windows_by_trace.insert(read_u32(reader)?, read_trace_window_bounds_binary(reader)?);
    }

    let mut services_by_trace = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        services_by_trace.insert(read_u32(reader)?, read_u32_vec(reader)?);
    }

    let trace_refs_by_service = read_bitmap_map_binary(reader)?;
    let trace_refs_by_operation = read_bitmap_map_binary(reader)?;

    let mut trace_refs_by_field_name_value = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        let field_name_ref = read_u32(reader)?;
        let mut value_map = FxHashMap::default();
        for _ in 0..read_len(reader)? {
            value_map.insert(read_u32(reader)?, read_roaring_bitmap_binary(reader)?);
        }
        trace_refs_by_field_name_value.insert(field_name_ref, value_map);
    }

    let mut field_values_by_name = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        field_values_by_name.insert(read_u32(reader)?, read_u32_vec(reader)?);
    }

    let mut row_refs_by_trace = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        row_refs_by_trace.insert(
            read_u32(reader)?,
            read_segment_row_locations_binary(reader)?,
        );
    }

    let persisted_segments = if version >= 2 {
        read_persisted_segment_summaries_binary(reader, version)?
    } else {
        FxHashMap::default()
    };

    Ok(DiskTraceShardState {
        trace_ids,
        strings,
        all_trace_refs,
        windows_by_trace,
        persisted_segments,
        services_by_trace,
        trace_refs_by_service,
        trace_refs_by_operation,
        trace_refs_by_field_name_value,
        field_values_by_name,
        row_refs_by_trace,
        rows_ingested: read_u64(reader)?,
    })
}

fn read_bitmap_map_binary<R: Read>(
    reader: &mut R,
) -> Result<FxHashMap<u32, RoaringBitmap>, StorageError> {
    let mut map = FxHashMap::default();
    for _ in 0..read_len(reader)? {
        map.insert(read_u32(reader)?, read_roaring_bitmap_binary(reader)?);
    }
    Ok(map)
}

fn serialize_segment_meta_binary_payload(meta: &SegmentMeta) -> Result<Vec<u8>, StorageError> {
    let mut payload = Vec::new();
    write_segment_meta_binary_payload(&mut payload, meta)?;
    Ok(payload)
}

fn deserialize_segment_meta_binary_payload(bytes: &[u8]) -> Result<SegmentMeta, StorageError> {
    let mut reader = Cursor::new(bytes);
    let meta = read_segment_meta_binary_payload(&mut reader)?;
    if reader.position() != bytes.len() as u64 {
        return Err(invalid_data("binary segment meta payload has trailing bytes").into());
    }
    Ok(meta)
}

fn compress_bytes_zstd(payload: &[u8], level: i32) -> Result<Vec<u8>, StorageError> {
    zstd::stream::encode_all(Cursor::new(payload), level).map_err(StorageError::from)
}

fn decompress_bytes_zstd(
    payload: &[u8],
    expected_uncompressed_len: u64,
    what: &str,
) -> Result<Vec<u8>, StorageError> {
    let decoded = zstd::stream::decode_all(Cursor::new(payload)).map_err(StorageError::from)?;
    if decoded.len() as u64 != expected_uncompressed_len {
        return Err(invalid_data(format!(
            "{what} length mismatch: expected {expected_uncompressed_len}, got {}",
            decoded.len()
        ))
        .into());
    }
    Ok(decoded)
}

fn build_segment_meta_string_dictionary(meta: &SegmentMeta) -> StringTable {
    let mut strings = StringTable::default();
    for trace in &meta.traces {
        strings.intern(&trace.trace_id);
        for service_name in &trace.services {
            strings.intern(service_name);
        }
        for operation_name in &trace.operations {
            strings.intern(operation_name);
        }
        let mut fields = trace.fields.iter().collect::<Vec<_>>();
        fields.sort_by(|(left, _), (right, _)| left.cmp(right));
        for (field_name, values) in fields {
            strings.intern(field_name);
            for value in values {
                strings.intern(value);
            }
        }
    }
    strings
}

fn write_dictionary_string_ref<W: Write>(
    writer: &mut W,
    dictionary: &StringTable,
    value: &str,
    what: &str,
) -> Result<(), StorageError> {
    let string_ref = dictionary
        .lookup(value)
        .ok_or_else(|| invalid_data(format!("missing {what} dictionary entry: {value}")))?;
    write_u32(writer, string_ref)
}

fn read_dictionary_string_ref<R: Read>(
    reader: &mut R,
    dictionary: &StringTable,
    what: &str,
) -> Result<String, StorageError> {
    let string_ref = read_u32(reader)?;
    dictionary
        .resolve(string_ref)
        .map(ToString::to_string)
        .ok_or_else(|| invalid_data(format!("{what} dictionary ref out of bounds")).into())
}

fn write_dictionary_string_vec<W: Write>(
    writer: &mut W,
    dictionary: &StringTable,
    values: &[String],
    what: &str,
) -> Result<(), StorageError> {
    write_len(writer, values.len(), what)?;
    for value in values {
        write_dictionary_string_ref(writer, dictionary, value, what)?;
    }
    Ok(())
}

fn read_dictionary_string_vec<R: Read>(
    reader: &mut R,
    dictionary: &StringTable,
    what: &str,
) -> Result<Vec<String>, StorageError> {
    let len = read_len(reader)?;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        values.push(read_dictionary_string_ref(reader, dictionary, what)?);
    }
    Ok(values)
}

fn write_compact_segment_row_locations<W: Write>(
    writer: &mut W,
    rows: &[SegmentRowLocation],
    segment_id: u32,
) -> Result<(), StorageError> {
    write_len(writer, rows.len(), "segment trace rows")?;
    for row in rows {
        if row.segment_id != segment_id {
            return Err(invalid_data("segment trace row belongs to a different segment").into());
        }
        write_u32(writer, row.row_index)?;
        writer.write_all(&row.start_unix_nano.to_le_bytes())?;
        writer.write_all(&row.end_unix_nano.to_le_bytes())?;
    }
    Ok(())
}

fn read_compact_segment_row_locations<R: Read>(
    reader: &mut R,
    segment_id: u32,
) -> Result<Vec<SegmentRowLocation>, StorageError> {
    let len = read_len(reader)?;
    let mut rows = Vec::with_capacity(len);
    for _ in 0..len {
        let row_index = read_u32(reader)?;
        let mut start = [0u8; 8];
        let mut end = [0u8; 8];
        reader.read_exact(&mut start)?;
        reader.read_exact(&mut end)?;
        rows.push(SegmentRowLocation {
            segment_id,
            row_index,
            start_unix_nano: i64::from_le_bytes(start),
            end_unix_nano: i64::from_le_bytes(end),
        });
    }
    Ok(rows)
}

fn derive_segment_services(traces: &[SegmentTraceMeta]) -> Vec<String> {
    let mut services = BTreeSet::new();
    for trace in traces {
        for service_name in &trace.services {
            services.insert(service_name.clone());
        }
    }
    services.into_iter().collect()
}

fn derive_segment_row_count(traces: &[SegmentTraceMeta]) -> u64 {
    traces.iter().map(|trace| trace.rows.len() as u64).sum()
}

fn write_segment_meta_binary_payload<W: Write>(
    writer: &mut W,
    meta: &SegmentMeta,
) -> Result<(), StorageError> {
    let dictionary = build_segment_meta_string_dictionary(meta);
    let segment_id_u32 = meta
        .segment_id
        .try_into()
        .map_err(|_| invalid_data("segment id exceeds u32 row-ref limit"))?;

    write_u64(writer, meta.segment_id)?;
    write_u64(writer, meta.persisted_bytes)?;
    writer.write_all(&meta.min_time_unix_nano.to_le_bytes())?;
    writer.write_all(&meta.max_time_unix_nano.to_le_bytes())?;
    write_u64(writer, meta.typed_field_columns)?;
    write_u64(writer, meta.string_field_columns)?;
    write_string_table_binary(writer, &dictionary)?;
    write_len(writer, meta.traces.len(), "segment traces")?;
    for trace in &meta.traces {
        write_dictionary_string_ref(writer, &dictionary, &trace.trace_id, "segment trace id")?;
        writer.write_all(&trace.start_unix_nano.to_le_bytes())?;
        writer.write_all(&trace.end_unix_nano.to_le_bytes())?;
        write_dictionary_string_vec(
            writer,
            &dictionary,
            &trace.services,
            "segment trace services",
        )?;
        write_dictionary_string_vec(
            writer,
            &dictionary,
            &trace.operations,
            "segment trace operations",
        )?;
        let mut fields = trace.fields.iter().collect::<Vec<_>>();
        fields.sort_by(|(left, _), (right, _)| left.cmp(right));
        write_len(writer, fields.len(), "segment trace fields")?;
        for (field_name, values) in fields {
            write_dictionary_string_ref(
                writer,
                &dictionary,
                field_name,
                "segment trace field name",
            )?;
            write_dictionary_string_vec(writer, &dictionary, values, "segment trace field values")?;
        }
        write_compact_segment_row_locations(writer, &trace.rows, segment_id_u32)?;
    }
    Ok(())
}

fn read_segment_meta_binary_payload<R: Read>(reader: &mut R) -> Result<SegmentMeta, StorageError> {
    let segment_id = read_u64(reader)?;
    let persisted_bytes = read_u64(reader)?;
    let mut min_time = [0u8; 8];
    let mut max_time = [0u8; 8];
    reader.read_exact(&mut min_time)?;
    reader.read_exact(&mut max_time)?;
    let typed_field_columns = read_u64(reader)?;
    let string_field_columns = read_u64(reader)?;
    let dictionary = read_string_table_binary(reader)?;
    let trace_count = read_len(reader)?;
    let segment_id_u32 = segment_id
        .try_into()
        .map_err(|_| invalid_data("segment id exceeds u32 row-ref limit"))?;
    let mut traces = Vec::with_capacity(trace_count);
    for _ in 0..trace_count {
        let trace_id = read_dictionary_string_ref(reader, &dictionary, "segment trace id")?;
        let mut start = [0u8; 8];
        let mut end = [0u8; 8];
        reader.read_exact(&mut start)?;
        reader.read_exact(&mut end)?;
        let services = read_dictionary_string_vec(reader, &dictionary, "segment trace services")?;
        let operations =
            read_dictionary_string_vec(reader, &dictionary, "segment trace operations")?;
        let mut fields = HashMap::new();
        for _ in 0..read_len(reader)? {
            let field_name =
                read_dictionary_string_ref(reader, &dictionary, "segment trace field name")?;
            let values =
                read_dictionary_string_vec(reader, &dictionary, "segment trace field values")?;
            fields.insert(field_name, values);
        }
        traces.push(SegmentTraceMeta {
            trace_id,
            start_unix_nano: i64::from_le_bytes(start),
            end_unix_nano: i64::from_le_bytes(end),
            services,
            operations,
            fields,
            rows: read_compact_segment_row_locations(reader, segment_id_u32)?,
        });
    }
    Ok(SegmentMeta {
        segment_id,
        segment_file: format!("{SEGMENT_PREFIX}{segment_id:020}{PART_SUFFIX}"),
        row_count: derive_segment_row_count(&traces),
        persisted_bytes,
        min_time_unix_nano: i64::from_le_bytes(min_time),
        max_time_unix_nano: i64::from_le_bytes(max_time),
        typed_field_columns,
        string_field_columns,
        services: derive_segment_services(&traces),
        traces,
    })
}

fn serialize_disk_trace_shard_state(shard: &DiskTraceShardState) -> Result<Vec<u8>, StorageError> {
    let mut payload = Vec::new();
    write_disk_trace_shard_state_binary(&mut payload, shard)?;
    Ok(payload)
}

fn deserialize_disk_trace_shard_state_with_version(
    bytes: &[u8],
    version: u8,
) -> Result<DiskTraceShardState, StorageError> {
    let mut reader = Cursor::new(bytes);
    let shard = read_disk_trace_shard_state_binary(&mut reader, version)?;
    if reader.position() != bytes.len() as u64 {
        return Err(invalid_data("recovery shard payload has trailing bytes").into());
    }
    Ok(shard)
}

#[cfg(test)]
fn deserialize_disk_trace_shard_state(bytes: &[u8]) -> Result<DiskTraceShardState, StorageError> {
    deserialize_disk_trace_shard_state_with_version(bytes, RECOVERY_SHARD_VERSION)
}

fn write_atomic_file(path: &Path, bytes: &[u8]) -> Result<(), StorageError> {
    let file_name = path
        .file_name()
        .ok_or_else(|| invalid_data("atomic write target is missing file name"))?
        .to_string_lossy()
        .to_string();
    let tmp_path = path.with_file_name(format!("{file_name}.tmp"));
    {
        let mut file = File::create(&tmp_path)?;
        file.write_all(bytes)?;
        file.flush()?;
    }
    fs::rename(&tmp_path, path)?;
    Ok(())
}

fn recovery_covered_segment_kind_for_path(
    path: &Path,
) -> Result<RecoveryCoveredSegmentKind, StorageError> {
    match path.extension().and_then(|value| value.to_str()) {
        Some("part") => Ok(RecoveryCoveredSegmentKind::Part),
        Some("wal") => Ok(RecoveryCoveredSegmentKind::Wal),
        Some("rows") => Ok(RecoveryCoveredSegmentKind::LegacyRows),
        _ => Err(invalid_data(format!(
            "unsupported recovery segment path {}",
            path.display()
        ))
        .into()),
    }
}

fn persist_recovery_manifest_file(
    root_path: &Path,
    recovered: &RecoveredDiskState,
) -> Result<(), StorageError> {
    let mut covered_segments = recovered
        .segment_paths
        .iter()
        .map(|(&segment_id, path)| {
            Ok(RecoveryCoveredSegment {
                segment_id,
                kind: recovery_covered_segment_kind_for_path(path)?,
            })
        })
        .collect::<Result<Vec<_>, StorageError>>()?;
    covered_segments.sort_unstable_by_key(|covered_segment| covered_segment.segment_id);

    let mut manifest = RecoveryManifest {
        covered_segments,
        shards: Vec::with_capacity(recovered.trace_shards.len()),
        persisted_bytes: recovered.persisted_bytes,
        typed_field_columns: recovered.typed_field_columns,
        string_field_columns: recovered.string_field_columns,
        next_segment_id: recovered.next_segment_id,
    };

    for (shard_index, shard) in recovered.trace_shards.iter().enumerate() {
        let payload = serialize_disk_trace_shard_state(shard)?;
        let compressed = compress_bytes_zstd(&payload, SNAPSHOT_ZSTD_LEVEL)?;
        let checksum = crc32fast::hash(&compressed);
        write_recovery_shard_file(
            &recovery_shard_snapshot_path(root_path, shard_index),
            shard_index,
            payload.len() as u64,
            checksum,
            &compressed,
        )?;
        manifest.shards.push(RecoveryShardManifestEntry {
            shard_index,
            checksum,
            compressed_bytes: compressed.len() as u64,
            uncompressed_bytes: payload.len() as u64,
        });
    }

    let mut bytes = Vec::new();
    bytes.write_all(RECOVERY_MANIFEST_MAGIC)?;
    write_u8(&mut bytes, RECOVERY_MANIFEST_VERSION)?;
    write_u64(&mut bytes, manifest.next_segment_id)?;
    write_u64(&mut bytes, manifest.persisted_bytes)?;
    write_u64(&mut bytes, manifest.typed_field_columns)?;
    write_u64(&mut bytes, manifest.string_field_columns)?;
    write_len(
        &mut bytes,
        manifest.covered_segments.len(),
        "snapshot covered segments",
    )?;
    for covered_segment in &manifest.covered_segments {
        write_u64(&mut bytes, covered_segment.segment_id)?;
        write_u8(&mut bytes, covered_segment.kind.to_u8())?;
    }
    write_len(&mut bytes, manifest.shards.len(), "snapshot shard entries")?;
    for shard in &manifest.shards {
        write_u32(
            &mut bytes,
            u32::try_from(shard.shard_index)
                .map_err(|_| invalid_data("recovery shard index exceeds u32"))?,
        )?;
        write_u32(&mut bytes, shard.checksum)?;
        write_u64(&mut bytes, shard.compressed_bytes)?;
        write_u64(&mut bytes, shard.uncompressed_bytes)?;
    }
    write_atomic_file(&recovery_manifest_path(root_path), &bytes)?;

    let keep = manifest
        .shards
        .iter()
        .map(|shard| shard.shard_index)
        .collect::<FxHashSet<_>>();
    cleanup_stale_recovery_shard_files(root_path, &keep)?;
    let legacy_snapshot_path = legacy_recovery_snapshot_path(root_path);
    if legacy_snapshot_path.exists() {
        let _ = fs::remove_file(legacy_snapshot_path);
    }
    Ok(())
}

fn write_recovery_shard_file(
    path: &Path,
    shard_index: usize,
    uncompressed_bytes: u64,
    checksum: u32,
    compressed_payload: &[u8],
) -> Result<(), StorageError> {
    let shard_index =
        u32::try_from(shard_index).map_err(|_| invalid_data("recovery shard index exceeds u32"))?;
    let mut bytes = Vec::with_capacity(compressed_payload.len() + 32);
    bytes.write_all(RECOVERY_SHARD_MAGIC)?;
    write_u8(&mut bytes, RECOVERY_SHARD_VERSION)?;
    write_u8(&mut bytes, SNAPSHOT_COMPRESSION_ZSTD)?;
    write_u32(&mut bytes, shard_index)?;
    write_u64(&mut bytes, uncompressed_bytes)?;
    write_u32(&mut bytes, checksum)?;
    bytes.extend_from_slice(compressed_payload);
    write_atomic_file(path, &bytes)
}

fn load_recovery_manifest_file(root_path: &Path) -> Result<RecoveryManifest, StorageError> {
    let manifest_path = recovery_manifest_path(root_path);
    let mut file = File::open(&manifest_path)?;
    let mut magic = [0u8; 7];
    file.read_exact(&mut magic)?;
    if magic != RECOVERY_MANIFEST_MAGIC {
        return Err(invalid_data("invalid recovery manifest header").into());
    }
    let version = read_u8(&mut file)?;
    if !matches!(version, 1..=RECOVERY_MANIFEST_VERSION) {
        return Err(invalid_data(format!(
            "unsupported recovery manifest version {version}"
        ))
        .into());
    }

    let next_segment_id = read_u64(&mut file)?;
    let persisted_bytes = read_u64(&mut file)?;
    let typed_field_columns = read_u64(&mut file)?;
    let string_field_columns = read_u64(&mut file)?;
    let segment_count = read_len(&mut file)?;
    let mut covered_segments = Vec::with_capacity(segment_count);
    for _ in 0..segment_count {
        let segment_id = read_u64(&mut file)?;
        let kind = if version == 1 {
            RecoveryCoveredSegmentKind::Part
        } else {
            RecoveryCoveredSegmentKind::from_u8(read_u8(&mut file)?)?
        };
        covered_segments.push(RecoveryCoveredSegment { segment_id, kind });
    }
    let shard_count = read_len(&mut file)?;
    let mut shards = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let shard_index = read_u32(&mut file)? as usize;
        shards.push(RecoveryShardManifestEntry {
            shard_index,
            checksum: read_u32(&mut file)?,
            compressed_bytes: read_u64(&mut file)?,
            uncompressed_bytes: read_u64(&mut file)?,
        });
    }

    Ok(RecoveryManifest {
        covered_segments,
        shards,
        persisted_bytes,
        typed_field_columns,
        string_field_columns,
        next_segment_id,
    })
}

fn load_recovery_shard_file(
    root_path: &Path,
    manifest_entry: &RecoveryShardManifestEntry,
) -> Result<DiskTraceShardState, StorageError> {
    let shard_path = recovery_shard_snapshot_path(root_path, manifest_entry.shard_index);
    let mut file = File::open(&shard_path)?;
    let mut magic = [0u8; 7];
    file.read_exact(&mut magic)?;
    if magic != RECOVERY_SHARD_MAGIC {
        return Err(invalid_data("invalid recovery shard header").into());
    }
    let version = read_u8(&mut file)?;
    if !matches!(version, 1..=RECOVERY_SHARD_VERSION) {
        return Err(invalid_data(format!("unsupported recovery shard version {version}")).into());
    }
    let compression = read_u8(&mut file)?;
    if compression != SNAPSHOT_COMPRESSION_ZSTD {
        return Err(invalid_data("unsupported recovery shard compression").into());
    }
    let shard_index = read_u32(&mut file)? as usize;
    if shard_index != manifest_entry.shard_index {
        return Err(invalid_data("recovery shard index mismatch").into());
    }
    let uncompressed_bytes = read_u64(&mut file)?;
    if uncompressed_bytes != manifest_entry.uncompressed_bytes {
        return Err(invalid_data("recovery shard uncompressed size mismatch").into());
    }
    let checksum = read_u32(&mut file)?;
    if checksum != manifest_entry.checksum {
        return Err(invalid_data("recovery shard checksum mismatch").into());
    }
    let mut compressed = Vec::new();
    file.read_to_end(&mut compressed)?;
    if compressed.len() as u64 != manifest_entry.compressed_bytes {
        return Err(invalid_data("recovery shard compressed size mismatch").into());
    }
    if crc32fast::hash(&compressed) != manifest_entry.checksum {
        return Err(invalid_data("recovery shard payload checksum mismatch").into());
    }
    let payload = decompress_bytes_zstd(
        &compressed,
        manifest_entry.uncompressed_bytes,
        "recovery shard payload",
    )?;
    deserialize_disk_trace_shard_state_with_version(&payload, version)
}

fn cleanup_stale_recovery_shard_files(
    root_path: &Path,
    keep: &FxHashSet<usize>,
) -> Result<(), StorageError> {
    for entry in fs::read_dir(root_path)? {
        let entry = entry?;
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
            continue;
        };
        let Some(shard_id) = parse_recovery_shard_file_name(file_name) else {
            continue;
        };
        if !keep.contains(&shard_id) {
            let _ = fs::remove_file(path);
        }
    }
    Ok(())
}

fn load_valid_recovery_snapshot(
    root_path: &Path,
    segments_path: &Path,
    segment_files: &HashMap<u64, SegmentFileSet>,
    trace_shards: usize,
) -> Result<Option<RecoverySnapshot>, StorageError> {
    let manifest = match load_recovery_manifest_file(root_path) {
        Ok(manifest) => manifest,
        Err(error) => {
            let manifest_path = recovery_manifest_path(root_path);
            if manifest_path.exists() {
                eprintln!(
                    "disk recovery manifest ignored at {}: {error}",
                    manifest_path.display()
                );
            }
            return Ok(None);
        }
    };
    if manifest.shards.len() != trace_shards {
        return Ok(None);
    }
    for covered_segment in &manifest.covered_segments {
        let Some(files) = segment_files.get(&covered_segment.segment_id) else {
            return Ok(None);
        };
        let expected_path = covered_segment.expected_path(segments_path);
        let actual_path = match covered_segment.kind {
            RecoveryCoveredSegmentKind::Part => files.part_path.as_ref(),
            RecoveryCoveredSegmentKind::Wal => files.wal_path.as_ref(),
            RecoveryCoveredSegmentKind::LegacyRows => files.legacy_rows_path.as_ref(),
        };
        let Some(actual_path) = actual_path else {
            return Ok(None);
        };
        if actual_path != &expected_path {
            return Ok(None);
        }
    }

    let mut shard_states = vec![None; trace_shards];
    for shard_entry in &manifest.shards {
        if shard_entry.shard_index >= trace_shards
            || shard_states[shard_entry.shard_index].is_some()
        {
            return Ok(None);
        }
        let shard_state = match load_recovery_shard_file(root_path, shard_entry) {
            Ok(shard_state) => shard_state,
            Err(error) => {
                eprintln!(
                    "disk recovery shard ignored at {}: {error}",
                    recovery_shard_snapshot_path(root_path, shard_entry.shard_index).display()
                );
                return Ok(None);
            }
        };
        shard_states[shard_entry.shard_index] = Some(shard_state);
    }

    Ok(Some(RecoverySnapshot {
        covered_segments: manifest.covered_segments,
        trace_shards: shard_states
            .into_iter()
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| invalid_data("recovery manifest missing shard snapshot"))?,
        persisted_bytes: manifest.persisted_bytes,
        typed_field_columns: manifest.typed_field_columns,
        string_field_columns: manifest.string_field_columns,
        next_segment_id: manifest.next_segment_id,
    }))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RowFileFormat {
    Legacy,
    ChecksummedJson,
    ChecksummedBinary,
    BatchedBinary,
}

impl RowFileFormat {
    fn uses_checksum(self) -> bool {
        matches!(self, Self::ChecksummedJson | Self::ChecksummedBinary)
    }
}

fn detect_row_file_format(file: &mut File) -> Result<(RowFileFormat, u64), StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let file_len = file.metadata()?.len();
    if file_len < WAL_HEADER_BYTES {
        file.seek(SeekFrom::Start(0))?;
        return Ok((RowFileFormat::Legacy, 0));
    }

    let mut magic = vec![0u8; WAL_MAGIC.len()];
    file.read_exact(&mut magic)?;
    let mut version = [0u8; 1];
    file.read_exact(&mut version)?;
    if magic == WAL_MAGIC {
        match version[0] {
            WAL_VERSION_JSON => Ok((RowFileFormat::ChecksummedJson, WAL_HEADER_BYTES)),
            WAL_VERSION_BINARY => Ok((RowFileFormat::ChecksummedBinary, WAL_HEADER_BYTES)),
            WAL_VERSION_BATCHED_BINARY => Ok((RowFileFormat::BatchedBinary, WAL_HEADER_BYTES)),
            _ => {
                file.seek(SeekFrom::Start(0))?;
                Ok((RowFileFormat::Legacy, 0))
            }
        }
    } else {
        file.seek(SeekFrom::Start(0))?;
        Ok((RowFileFormat::Legacy, 0))
    }
}

fn wal_record_storage_bytes(payload_len: u32) -> u64 {
    wal_record_storage_bytes_for_format(RowFileFormat::ChecksummedBinary, payload_len)
}

fn wal_record_storage_bytes_for_format(format: RowFileFormat, payload_len: u32) -> u64 {
    match format {
        RowFileFormat::BatchedBinary => LENGTH_PREFIX_BYTES + payload_len as u64,
        _ => {
            let checksum_bytes = if format.uses_checksum() {
                WAL_CHECKSUM_BYTES
            } else {
                0
            };
            LENGTH_PREFIX_BYTES + checksum_bytes + payload_len as u64
        }
    }
}

fn wal_batch_record_header_bytes() -> u64 {
    LENGTH_PREFIX_BYTES + WAL_CHECKSUM_BYTES + WAL_BATCH_ROW_COUNT_BYTES
}

fn wal_batch_body_bytes(payload_bytes: usize, row_count: usize) -> u64 {
    WAL_BATCH_ROW_COUNT_BYTES + (row_count as u64 * LENGTH_PREFIX_BYTES) + payload_bytes as u64
}

fn wal_batch_storage_bytes(payload_bytes: usize, row_count: usize) -> u64 {
    wal_batch_record_header_bytes()
        + (row_count as u64 * LENGTH_PREFIX_BYTES)
        + payload_bytes as u64
}

fn truncate_invalid_row_tail(file: &mut File, valid_len: u64) -> Result<(), StorageError> {
    let current_len = file.metadata()?.len();
    if current_len > valid_len {
        file.set_len(valid_len)?;
    }
    Ok(())
}

fn count_field_column_encodings_from_value_sets<I, J, S>(field_values_by_name: I) -> (u64, u64)
where
    I: IntoIterator<Item = J>,
    J: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut typed = 0u64;
    let mut string = 0u64;
    for values in field_values_by_name {
        let values = values
            .into_iter()
            .map(|value| Some(value.as_ref().to_string()))
            .collect::<Vec<_>>();
        if OptionalBoolColumn::from_optional_strings(&values).is_some()
            || OptionalI64Column::from_optional_strings(&values).is_some()
        {
            typed += 1;
        } else {
            string += 1;
        }
    }
    (typed, string)
}

fn validate_and_skip_part_header(file: &mut File) -> Result<u8, StorageError> {
    file.seek(SeekFrom::Start(0))?;
    let mut magic = vec![0u8; PART_MAGIC.len()];
    file.read_exact(&mut magic)?;
    if magic != PART_MAGIC {
        return Err(invalid_data("invalid part file magic").into());
    }
    let mut version = [0u8; 1];
    file.read_exact(&mut version)?;
    if !matches!(version[0], 1..=PART_VERSION) {
        return Err(invalid_data("unsupported part file version").into());
    }
    Ok(version[0])
}

fn compaction_target_segment_size_bytes(config: &DiskStorageConfig) -> u64 {
    DEFAULT_TARGET_SEGMENT_SIZE_BYTES.max(
        config
            .target_segment_size_bytes
            .saturating_mul(COMPACTION_TARGET_MULTIPLIER),
    )
}

fn segment_wal_path(segments_path: &Path, segment_id: u64) -> PathBuf {
    segments_path.join(format!("{SEGMENT_PREFIX}{segment_id:020}{WAL_SUFFIX}"))
}

fn segment_row_file_path(segments_path: &Path, segment_id: u64) -> PathBuf {
    segments_path.join(format!("{SEGMENT_PREFIX}{segment_id:020}{LEGACY_ROWS_SUFFIX}"))
}

fn segment_part_path(segments_path: &Path, segment_id: u64) -> PathBuf {
    segments_path.join(format!("{SEGMENT_PREFIX}{segment_id:020}{PART_SUFFIX}"))
}

fn segment_meta_path(segments_path: &Path, segment_id: u64) -> PathBuf {
    segments_path.join(format!("{SEGMENT_PREFIX}{segment_id:020}{META_SUFFIX}"))
}

fn segment_binary_meta_path(segments_path: &Path, segment_id: u64) -> PathBuf {
    segments_path.join(format!(
        "{SEGMENT_PREFIX}{segment_id:020}{META_BINARY_SUFFIX}"
    ))
}

fn recovery_manifest_path(root_path: &Path) -> PathBuf {
    root_path.join(RECOVERY_MANIFEST_FILENAME)
}

fn legacy_recovery_snapshot_path(root_path: &Path) -> PathBuf {
    root_path.join("trace-index.snapshot")
}

fn recovery_shard_snapshot_path(root_path: &Path, shard_index: usize) -> PathBuf {
    root_path.join(format!(
        "{RECOVERY_SHARD_FILENAME_PREFIX}{shard_index:04}{RECOVERY_SHARD_FILENAME_SUFFIX}"
    ))
}

fn parse_recovery_shard_file_name(file_name: &str) -> Option<usize> {
    file_name
        .strip_prefix(RECOVERY_SHARD_FILENAME_PREFIX)
        .and_then(|value| value.strip_suffix(RECOVERY_SHARD_FILENAME_SUFFIX))
        .and_then(|value| value.parse().ok())
}

#[derive(Debug, Clone, Copy)]
enum SegmentFileKind {
    Wal,
    Part,
    LegacyRows,
    MetaJson,
    MetaBinary,
}

fn parse_segment_file_name(file_name: &str) -> Option<(u64, SegmentFileKind)> {
    if let Some(value) = file_name
        .strip_prefix(SEGMENT_PREFIX)
        .and_then(|value| value.strip_suffix(WAL_SUFFIX))
    {
        return value.parse().ok().map(|id| (id, SegmentFileKind::Wal));
    }

    if let Some(value) = file_name
        .strip_prefix(SEGMENT_PREFIX)
        .and_then(|value| value.strip_suffix(PART_SUFFIX))
    {
        return value.parse().ok().map(|id| (id, SegmentFileKind::Part));
    }

    if let Some(value) = file_name
        .strip_prefix(SEGMENT_PREFIX)
        .and_then(|value| value.strip_suffix(LEGACY_ROWS_SUFFIX))
    {
        return value
            .parse()
            .ok()
            .map(|id| (id, SegmentFileKind::LegacyRows));
    }

    file_name
        .strip_prefix(SEGMENT_PREFIX)
        .and_then(|value| value.strip_suffix(META_SUFFIX))
        .and_then(|value| value.parse().ok())
        .map(|id| (id, SegmentFileKind::MetaJson))
        .or_else(|| {
            file_name
                .strip_prefix(SEGMENT_PREFIX)
                .and_then(|value| value.strip_suffix(META_BINARY_SUFFIX))
                .and_then(|value| value.parse().ok())
                .map(|id| (id, SegmentFileKind::MetaBinary))
        })
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::{
        coalesce_trace_blocks_for_shard, decode_trace_row, load_or_rebuild_segment_meta,
        load_valid_recovery_snapshot, deserialize_disk_trace_shard_state,
        merge_sorted_row_locations, parse_segment_file_name, persist_recovery_manifest_file,
        prepare_block_row_metadata, prepare_shared_group_metadata, rebuild_segment_meta,
        recover_state, route_trace_blocks_for_append, search_segment_meta_for_request,
        segment_binary_meta_path, segment_meta_path, segment_part_path, segment_row_file_path,
        segment_wal_path, trace_block_shard_index, trace_shard_index, write_segment_meta_binary,
        serialize_disk_trace_shard_state,
        ActiveSegment, BatchInternCache, DiskStorageConfig, DiskStorageEngine, DiskSyncPolicy,
        DiskTraceShardState, PreparedTraceBlockAppend, PreparedTraceShardBatch,
        SegmentAccumulator, SegmentFileSet, SegmentRowLocation, SegmentTraceAccumulator,
        SEGMENTS_DIRNAME,
    };
    use crate::{StorageEngine, StorageError};
    use serde_json::Value;
    use std::{
        collections::HashMap,
        fs,
        path::{Path, PathBuf},
        thread,
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use vtcore::{Field, FieldFilter, TraceBlock, TraceSearchRequest, TraceSpanRow};

    fn row(segment_id: u64, row_index: u32, start: i64, end: i64) -> SegmentRowLocation {
        SegmentRowLocation {
            segment_id: segment_id.try_into().expect("test segment id fits in u32"),
            row_index,
            start_unix_nano: start,
            end_unix_nano: end,
        }
    }

    fn trace_row(
        trace_id: &str,
        span_id: &str,
        name: &str,
        start: i64,
        end: i64,
        fields: &[(&str, &str)],
    ) -> TraceSpanRow {
        TraceSpanRow::new(
            trace_id.to_string(),
            span_id.to_string(),
            None,
            name.to_string(),
            start,
            end,
            fields
                .iter()
                .map(|(field_name, field_value)| Field::new(*field_name, *field_value))
                .collect(),
        )
        .expect("valid trace row")
    }

    fn trace_ids_for_shard(trace_shards: usize, shard_index: usize, count: usize) -> Vec<String> {
        let mut trace_ids = Vec::with_capacity(count);
        let mut candidate = 0usize;
        while trace_ids.len() < count {
            let trace_id = format!("trace-shard-{candidate}");
            if trace_shard_index(&trace_id, trace_shards) == shard_index {
                trace_ids.push(trace_id);
            }
            candidate += 1;
        }
        trace_ids
    }

    fn temp_test_dir(name: &str) -> PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time went backwards")
            .as_nanos();
        let path = std::env::temp_dir().join(format!("rust-vt-disk-unit-{name}-{nanos}"));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    fn wait_until(mut condition: impl FnMut() -> bool) {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::time::Instant::now() < deadline {
            if condition() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(condition(), "condition not satisfied before timeout");
    }

    fn write_test_wal_segment(
        segments_path: &Path,
        segment_id: u64,
        rows: Vec<TraceSpanRow>,
    ) -> Result<(), StorageError> {
        let block = TraceBlock::from_rows(rows);
        let prepared = PreparedTraceBlockAppend::new(block, 1);
        let mut active = ActiveSegment::open(segments_path, segment_id, true, 4096, false)?;
        active.append_block_rows(
            &prepared.prepared_rows,
            &prepared.prepared_shared_groups,
            &prepared.encoded_row_bytes,
            &prepared.encoded_row_ranges,
            0,
            prepared.prepared_rows.len(),
        )?;
        active.flush()?;
        Ok(())
    }

    fn test_segment_data_path(segments_path: &Path, segment_id: u64) -> PathBuf {
        let part_path = segment_part_path(segments_path, segment_id);
        if part_path.exists() {
            return part_path;
        }
        let wal_path = segment_wal_path(segments_path, segment_id);
        if wal_path.exists() {
            return wal_path;
        }
        let rows_path = segment_row_file_path(segments_path, segment_id);
        if rows_path.exists() {
            return rows_path;
        }
        panic!("missing persisted test segment {segment_id}");
    }

    #[test]
    fn load_or_rebuild_segment_meta_reads_legacy_json_and_writes_binary_meta() {
        let path = temp_test_dir("legacy-meta-upgrade");
        let config = DiskStorageConfig::default().with_target_segment_size_bytes(1);

        {
            let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk");
            engine
                .append_rows(vec![trace_row(
                    "trace-legacy-meta",
                    "span-1",
                    "GET /checkout",
                    100,
                    150,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("span_attr:http.method", "GET"),
                    ],
                )])
                .expect("append rows");
        }

        let segments_path = path.join(SEGMENTS_DIRNAME);
        let data_path = test_segment_data_path(&segments_path, 1);
        let meta_path = segment_meta_path(&segments_path, 1);
        let binary_meta_path = segments_path.join("segment-00000000000000000001.meta.bin");

        let _ = fs::remove_file(&meta_path);
        let _ = fs::remove_file(&binary_meta_path);

        let rebuilt = rebuild_segment_meta(1, &data_path).expect("rebuild segment meta");
        fs::write(
            &meta_path,
            serde_json::to_vec_pretty(&rebuilt).expect("serialize legacy json meta"),
        )
        .expect("write legacy json meta");

        let loaded = load_or_rebuild_segment_meta(1, &data_path, &meta_path)
            .expect("load legacy json meta");
        assert_eq!(loaded.row_count, rebuilt.row_count);
        assert_eq!(loaded.services, rebuilt.services);
        assert!(
            binary_meta_path.exists(),
            "loading legacy json metadata should upgrade it to binary metadata",
        );

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn binary_segment_meta_is_materially_smaller_than_legacy_json_for_repeated_strings() {
        let path = temp_test_dir("binary-meta-size");
        let config = DiskStorageConfig::default().with_target_segment_size_bytes(u64::MAX);

        {
            let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk");
            let rows = (0..128)
                .map(|index| {
                    trace_row(
                        &format!("trace-meta-size-{index:04}"),
                        "span-1",
                        "GET /checkout",
                        100 + index as i64,
                        150 + index as i64,
                        &[
                            ("resource_attr:service.name", "checkout"),
                            ("span_attr:http.method", "GET"),
                            ("span_attr:http.route", "/checkout"),
                            ("span_attr:http.status_code", "200"),
                        ],
                    )
                })
                .collect::<Vec<_>>();
            engine.append_rows(rows).expect("append rows");
        }

        let segments_path = path.join(SEGMENTS_DIRNAME);
        let data_path = test_segment_data_path(&segments_path, 1);
        let meta = rebuild_segment_meta(1, &data_path).expect("rebuild segment meta");
        let legacy_json_size = serde_json::to_vec_pretty(&meta)
            .expect("serialize legacy json meta")
            .len() as u64;
        let binary_meta_path = segment_binary_meta_path(&segments_path, 1);
        let _ = fs::remove_file(&binary_meta_path);
        let (binary_size, _) =
            write_segment_meta_binary(&binary_meta_path, &meta, DiskSyncPolicy::None)
                .expect("write binary meta");

        assert!(
            binary_size * 100 < legacy_json_size * 35,
            "binary meta should be materially smaller than legacy json: binary={binary_size} legacy_json={legacy_json_size}",
        );

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn recovery_manifest_round_trips_covered_wal_segments() {
        let path = temp_test_dir("recovery-covered-wal");
        let config = DiskStorageConfig::default()
            .with_target_segment_size_bytes(1)
            .with_trace_shards(1);
        let original_row = trace_row(
            "trace-recovery-wal",
            "span-1",
            "GET /checkout",
            100,
            150,
            &[("resource_attr:service.name", "checkout")],
        );

        {
            let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk");
            engine
                .append_rows(vec![original_row.clone()])
                .expect("append rows");
            wait_until(|| engine.stats().segment_count >= 1);
        }

        let segments_path = path.join(SEGMENTS_DIRNAME);
        let part_path = segment_part_path(&segments_path, 1);
        let wal_path = segment_wal_path(&segments_path, 1);
        let mut recovered = recover_state(&path, &segments_path, 1, 1).expect("recover state");
        if part_path.exists() {
            write_test_wal_segment(&segments_path, 1, vec![original_row])
                .expect("write retained wal segment");
            fs::remove_file(&part_path).expect("remove part to force wal-backed recovery");
        }
        assert!(wal_path.exists(), "fixture should contain retained wal payload");
        assert!(
            !part_path.exists(),
            "fixture should force recovery to rely on wal kind",
        );

        recovered.segment_paths.insert(1, wal_path.clone());
        persist_recovery_manifest_file(&path, &recovered).expect("persist manifest with wal");

        let mut segment_files: HashMap<u64, SegmentFileSet> = HashMap::new();
        for entry in fs::read_dir(&segments_path).expect("read segments dir") {
            let entry = entry.expect("segment dir entry");
            let path = entry.path();
            let Some(file_name) = path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
            else {
                continue;
            };
            let Some((segment_id, kind)) = parse_segment_file_name(&file_name) else {
                continue;
            };
            let files = segment_files.entry(segment_id).or_default();
            match kind {
                super::SegmentFileKind::Wal => files.wal_path = Some(path),
                super::SegmentFileKind::Part => files.part_path = Some(path),
                super::SegmentFileKind::LegacyRows => files.legacy_rows_path = Some(path),
                super::SegmentFileKind::MetaJson => files.meta_json_path = Some(path),
                super::SegmentFileKind::MetaBinary => files.meta_bin_path = Some(path),
            }
        }

        let snapshot = load_valid_recovery_snapshot(&path, &segments_path, &segment_files, 1)
            .expect("load recovery snapshot")
            .expect("snapshot should stay valid");
        let recovered_from_snapshot = snapshot.into_recovered_state(&segments_path);
        assert_eq!(
            recovered_from_snapshot.segment_paths.get(&1),
            Some(&wal_path),
            "recovery manifest should preserve covered wal segments instead of rewriting them as parts",
        );

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn recovery_manifest_round_trips_covered_rows_segments() {
        let path = temp_test_dir("recovery-covered-rows");
        let config = DiskStorageConfig::default()
            .with_target_segment_size_bytes(1)
            .with_trace_shards(1)
            .with_trace_wal_enabled(false);

        {
            let engine = DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk");
            engine
                .append_rows(vec![trace_row(
                    "trace-recovery-rows",
                    "span-1",
                    "GET /checkout",
                    100,
                    150,
                    &[("resource_attr:service.name", "checkout")],
                )])
                .expect("append rows");
            wait_until(|| engine.stats().segment_count >= 1);
        }

        let segments_path = path.join(SEGMENTS_DIRNAME);
        let rows_path = segment_row_file_path(&segments_path, 1);
        assert!(rows_path.exists(), "fixture should contain persisted row-file segment");

        let mut segment_files: HashMap<u64, SegmentFileSet> = HashMap::new();
        for entry in fs::read_dir(&segments_path).expect("read segments dir") {
            let entry = entry.expect("segment dir entry");
            let path = entry.path();
            let Some(file_name) = path
                .file_name()
                .map(|name| name.to_string_lossy().to_string())
            else {
                continue;
            };
            let Some((segment_id, kind)) = parse_segment_file_name(&file_name) else {
                continue;
            };
            let files = segment_files.entry(segment_id).or_default();
            match kind {
                super::SegmentFileKind::Wal => files.wal_path = Some(path),
                super::SegmentFileKind::Part => files.part_path = Some(path),
                super::SegmentFileKind::LegacyRows => files.legacy_rows_path = Some(path),
                super::SegmentFileKind::MetaJson => files.meta_json_path = Some(path),
                super::SegmentFileKind::MetaBinary => files.meta_bin_path = Some(path),
            }
        }

        let snapshot = load_valid_recovery_snapshot(&path, &segments_path, &segment_files, 1)
            .expect("load recovery snapshot")
            .expect("snapshot should stay valid");
        let recovered_from_snapshot = snapshot.into_recovered_state(&segments_path);
        assert_eq!(
            recovered_from_snapshot.segment_paths.get(&1),
            Some(&rows_path),
            "recovery manifest should preserve covered row-file segments",
        );

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn persisted_segment_summaries_prune_segments_before_meta_scan() {
        let path = temp_test_dir("persisted-summary-prune");
        let segments_path = path.join(SEGMENTS_DIRNAME);
        fs::create_dir_all(&segments_path).expect("create segments dir");

        write_test_wal_segment(
            &segments_path,
            1,
            vec![trace_row(
                "trace-checkout",
                "span-1",
                "GET /checkout",
                100,
                150,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("span_attr:http.method", "GET"),
                ],
            )],
        )
        .expect("write checkout segment");
        write_test_wal_segment(
            &segments_path,
            2,
            vec![trace_row(
                "trace-inventory",
                "span-1",
                "POST /inventory",
                200,
                250,
                &[
                    ("resource_attr:service.name", "inventory"),
                    ("span_attr:http.method", "POST"),
                ],
            )],
        )
        .expect("write inventory segment");

        let mut shard = DiskTraceShardState::default();
        shard.observe_persisted_segment(
            rebuild_segment_meta(1, &segment_wal_path(&segments_path, 1))
                .expect("rebuild checkout meta"),
        );
        shard.observe_persisted_segment(
            rebuild_segment_meta(2, &segment_wal_path(&segments_path, 2))
                .expect("rebuild inventory meta"),
        );

        let candidate_segments = shard.candidate_persisted_segment_ids(&TraceSearchRequest {
            start_unix_nano: 0,
            end_unix_nano: 1_000,
            service_name: Some("checkout".to_string()),
            operation_name: Some("GET /checkout".to_string()),
            field_filters: vec![FieldFilter {
                name: "span_attr:http.method".to_string(),
                value: "GET".to_string(),
            }],
            limit: 10,
        });

        assert_eq!(candidate_segments, vec![1]);

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn recovery_shard_round_trips_persisted_segment_summaries() {
        let path = temp_test_dir("recovery-summary-roundtrip");
        let segments_path = path.join(SEGMENTS_DIRNAME);
        fs::create_dir_all(&segments_path).expect("create segments dir");

        write_test_wal_segment(
            &segments_path,
            1,
            vec![trace_row(
                "trace-summary-roundtrip",
                "span-1",
                "GET /checkout",
                100,
                150,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("span_attr:http.method", "GET"),
                ],
            )],
        )
        .expect("write summary segment");

        let mut shard = DiskTraceShardState::default();
        shard.observe_persisted_segment(
            rebuild_segment_meta(1, &segment_wal_path(&segments_path, 1))
                .expect("rebuild summary meta"),
        );

        let encoded = serialize_disk_trace_shard_state(&shard).expect("encode shard state");
        let decoded =
            deserialize_disk_trace_shard_state(&encoded).expect("decode shard state with summaries");
        let request = TraceSearchRequest {
            start_unix_nano: 0,
            end_unix_nano: 1_000,
            service_name: Some("checkout".to_string()),
            operation_name: None,
            field_filters: Vec::new(),
            limit: 10,
        };

        assert_eq!(decoded.candidate_persisted_segment_ids(&request), vec![1]);
        let hits = decoded
            .search_persisted_segments(&request)
            .expect("summary-backed persisted search");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "trace-summary-roundtrip");

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn search_segment_meta_for_request_matches_exact_trace_filters() {
        let path = temp_test_dir("segment-meta-search");
        let segments_path = path.join(SEGMENTS_DIRNAME);
        fs::create_dir_all(&segments_path).expect("create segments dir");

        write_test_wal_segment(
            &segments_path,
            1,
            vec![
                trace_row(
                    "trace-checkout",
                    "span-1",
                    "GET /checkout",
                    100,
                    150,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("span_attr:http.method", "GET"),
                    ],
                ),
                trace_row(
                    "trace-inventory",
                    "span-1",
                    "POST /inventory",
                    200,
                    250,
                    &[
                        ("resource_attr:service.name", "inventory"),
                        ("span_attr:http.method", "POST"),
                    ],
                ),
            ],
        )
        .expect("write test segment");

        let meta =
            rebuild_segment_meta(1, &segment_wal_path(&segments_path, 1)).expect("rebuild meta");
        let hits = search_segment_meta_for_request(
            &meta,
            &TraceSearchRequest {
                start_unix_nano: 0,
                end_unix_nano: 1_000,
                service_name: Some("checkout".to_string()),
                operation_name: Some("GET /checkout".to_string()),
                field_filters: vec![FieldFilter {
                    name: "span_attr:http.method".to_string(),
                    value: "GET".to_string(),
                }],
                limit: 10,
            },
        );

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "trace-checkout");

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn observe_persisted_segment_keeps_only_minimal_trace_runtime_indexes() {
        let path = temp_test_dir("persisted-summary-minimal-indexes");
        let segments_path = path.join(SEGMENTS_DIRNAME);
        fs::create_dir_all(&segments_path).expect("create segments dir");

        write_test_wal_segment(
            &segments_path,
            1,
            vec![trace_row(
                "trace-minimal-indexes",
                "span-1",
                "GET /checkout",
                100,
                150,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("span_attr:http.method", "GET"),
                ],
            )],
        )
        .expect("write test segment");

        let mut shard = DiskTraceShardState::default();
        shard.observe_persisted_segment(
            rebuild_segment_meta(1, &segment_wal_path(&segments_path, 1))
                .expect("rebuild persisted meta"),
        );

        assert!(
            shard.services_by_trace.is_empty()
                && shard.trace_refs_by_service.is_empty()
                && shard.trace_refs_by_operation.is_empty()
                && shard.trace_refs_by_field_name_value.is_empty()
                && shard.field_values_by_name.is_empty(),
            "persisted segment summaries should replace heavy persisted trace indexes",
        );
        assert!(
            shard.trace_window_bounds("trace-minimal-indexes").is_some(),
            "minimal persisted indexes must still preserve trace windows",
        );
        assert_eq!(
            shard.row_refs_for_trace("trace-minimal-indexes", 0, 1_000).len(),
            1,
            "minimal persisted indexes must still preserve row refs",
        );

        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn merge_sorted_row_locations_appends_sorted_tail_without_full_resort() {
        let mut existing = vec![row(1, 0, 100, 150), row(1, 1, 200, 250)];
        let incoming = vec![row(1, 2, 260, 300), row(1, 3, 310, 360)];

        merge_sorted_row_locations(&mut existing, incoming);

        let end_times: Vec<i64> = existing.iter().map(|row| row.end_unix_nano).collect();
        assert_eq!(end_times, vec![150, 250, 300, 360]);
    }

    #[test]
    fn merge_sorted_row_locations_orders_unsorted_incoming_batch() {
        let mut existing = vec![row(2, 0, 10, 20)];
        let incoming = vec![row(2, 2, 40, 60), row(2, 1, 20, 30)];

        merge_sorted_row_locations(&mut existing, incoming);

        let row_indexes: Vec<u32> = existing.iter().map(|row| row.row_index).collect();
        assert_eq!(row_indexes, vec![0, 1, 2]);
    }

    #[test]
    fn segment_row_location_stays_compact_for_live_index() {
        assert!(
            std::mem::size_of::<SegmentRowLocation>() <= 24,
            "segment row refs should stay compact for live postings",
        );
    }

    #[test]
    fn segment_row_location_serialization_omits_wal_offsets_and_lengths() {
        let json = serde_json::to_value(row(7, 3, 100, 150)).expect("serialize row ref");
        let Value::Object(object) = json else {
            panic!("row ref should serialize as object");
        };

        assert!(!object.contains_key("offset"));
        assert!(!object.contains_key("len"));
    }

    #[test]
    fn segment_trace_accumulator_stays_compact_for_active_segment_indexing() {
        assert!(
            std::mem::size_of::<SegmentTraceAccumulator>() <= 128,
            "segment trace accumulator should stay compact enough for lock-hot ingest",
        );
    }

    #[test]
    fn trace_block_shard_index_detects_pre_sharded_multi_trace_block() {
        let trace_shards = 4;
        let trace_ids = trace_ids_for_shard(trace_shards, 2, 2);
        let block = TraceBlock::from_rows(vec![
            trace_row(
                &trace_ids[0],
                "span-1",
                "op-a",
                100,
                150,
                &[("resource_attr:service.name", "checkout")],
            ),
            trace_row(
                &trace_ids[1],
                "span-2",
                "op-b",
                160,
                210,
                &[("resource_attr:service.name", "checkout")],
            ),
        ]);

        assert_eq!(trace_block_shard_index(&block, trace_shards), Some(2));
    }

    #[test]
    fn trace_block_shard_index_rejects_cross_shard_block() {
        let block = TraceBlock::from_rows(vec![
            trace_row(
                "trace-shard-a",
                "span-1",
                "op-a",
                100,
                150,
                &[("resource_attr:service.name", "checkout")],
            ),
            trace_row(
                "trace-shard-b",
                "span-2",
                "op-b",
                160,
                210,
                &[("resource_attr:service.name", "checkout")],
            ),
        ]);

        let shard_index = trace_shard_index(block.trace_id_at(0), 4);
        let other_shard_index = trace_shard_index(block.trace_id_at(1), 4);
        if shard_index != other_shard_index {
            assert_eq!(trace_block_shard_index(&block, 4), None);
        }
    }

    #[test]
    fn route_trace_blocks_for_append_keeps_same_shard_blocks_together() {
        let trace_shards = 4;
        let trace_ids = trace_ids_for_shard(trace_shards, 2, 2);
        let routes = route_trace_blocks_for_append(
            vec![
                TraceBlock::from_rows(vec![trace_row(
                    &trace_ids[0],
                    "span-1",
                    "op-a",
                    100,
                    150,
                    &[("resource_attr:service.name", "checkout")],
                )]),
                TraceBlock::from_rows(vec![trace_row(
                    &trace_ids[1],
                    "span-2",
                    "op-b",
                    160,
                    210,
                    &[("resource_attr:service.name", "checkout")],
                )]),
            ],
            trace_shards,
        );

        assert_eq!(routes.len(), 1);
        assert_eq!(routes[0].0, 2);
        assert_eq!(routes[0].1.len(), 2);
    }

    #[test]
    fn route_trace_blocks_for_append_splits_cross_shard_block_only_once() {
        let trace_shards = 4;
        let block = TraceBlock::from_rows(vec![
            trace_row(
                "trace-route-a",
                "span-1",
                "op-a",
                100,
                150,
                &[("resource_attr:service.name", "checkout")],
            ),
            trace_row(
                "trace-route-b",
                "span-2",
                "op-b",
                160,
                210,
                &[("resource_attr:service.name", "checkout")],
            ),
        ]);

        let routes = route_trace_blocks_for_append(vec![block], trace_shards);

        assert!(routes.len() >= 1);
        assert!(routes.iter().all(|(shard_index, blocks)| {
            !blocks.is_empty()
                && blocks
                    .iter()
                    .all(|block| trace_block_shard_index(block, trace_shards) == Some(*shard_index))
        }));
    }

    #[test]
    fn prepared_trace_shard_batch_concatenates_block_metadata_consistently() {
        let trace_shards = 1;
        let first = PreparedTraceBlockAppend::new(
            TraceBlock::from_rows(vec![
                trace_row(
                    "trace-batch-block-1",
                    "span-1",
                    "op-a",
                    100,
                    150,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.method", "GET"),
                    ],
                ),
                trace_row(
                    "trace-batch-block-1",
                    "span-2",
                    "op-a",
                    160,
                    210,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.status_code", "200"),
                    ],
                ),
            ]),
            trace_shards,
        );
        let second = PreparedTraceBlockAppend::new(
            TraceBlock::from_rows(vec![
                trace_row(
                    "trace-batch-block-2",
                    "span-1",
                    "op-b",
                    300,
                    350,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.method", "POST"),
                    ],
                ),
                trace_row(
                    "trace-batch-block-2",
                    "span-2",
                    "op-b",
                    360,
                    410,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.status_code", "201"),
                    ],
                ),
            ]),
            trace_shards,
        );
        let first_shared_groups = first.prepared_shared_groups.len();
        let first_row_count = first.prepared_rows.len();
        let second_row_count = second.prepared_rows.len();

        let batch = PreparedTraceShardBatch::from_prepared_blocks(vec![first, second]);

        assert_eq!(batch.shard_index, 0);
        assert_eq!(
            batch.prepared_rows.len(),
            first_row_count + second_row_count
        );
        assert!(batch.prepared_shared_groups.len() >= first_shared_groups);
        assert_eq!(
            batch.encoded_row_ranges.len(),
            first_row_count + second_row_count
        );
        assert_eq!(
            batch
                .encoded_row_ranges
                .last()
                .expect("encoded row range")
                .end,
            batch.encoded_row_bytes.len()
        );
        let second_block_first_row = &batch.prepared_rows[first_row_count];
        if let Some(shared_group_index) = second_block_first_row.shared_group_index {
            assert!(
                shared_group_index >= first_shared_groups,
                "second block shared-group indexes should be remapped behind the first block",
            );
        }
    }

    #[test]
    fn disk_storage_config_allows_custom_trace_wal_writer_capacity() {
        let config = DiskStorageConfig::default().with_trace_wal_writer_capacity_bytes(4 << 20);
        assert_eq!(config.trace_wal_writer_capacity_bytes, 4 << 20);

        let clamped = DiskStorageConfig::default().with_trace_wal_writer_capacity_bytes(0);
        assert_eq!(clamped.trace_wal_writer_capacity_bytes, 1);
    }

    #[test]
    fn disk_storage_config_can_defer_trace_wal_writes() {
        let config = DiskStorageConfig::default().with_trace_deferred_wal_writes(true);
        assert!(config.trace_deferred_wal_writes);
    }

    #[test]
    fn disk_storage_config_can_disable_trace_wal() {
        let config = DiskStorageConfig::default().with_trace_wal_enabled(false);
        assert!(!config.trace_wal_enabled);
    }

    #[test]
    fn disk_storage_config_allows_custom_trace_seal_worker_count() {
        let config = DiskStorageConfig::default().with_trace_seal_worker_count(4);
        assert_eq!(config.trace_seal_worker_count, 4);

        let clamped = DiskStorageConfig::default().with_trace_seal_worker_count(0);
        assert_eq!(clamped.trace_seal_worker_count, 1);
    }

    #[test]
    fn deferred_wal_head_trace_reads_without_cached_payload_copy() {
        let path = temp_test_dir("deferred-wal-head-read-no-payload-copy");
        let config = DiskStorageConfig::default()
            .with_trace_shards(1)
            .with_trace_deferred_wal_writes(true);
        let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk engine");
        let expected = trace_row(
            "trace-deferred-read-1",
            "span-1",
            "GET /checkout",
            100,
            150,
            &[
                ("resource_attr:service.name", "checkout"),
                ("http.method", "GET"),
            ],
        );

        engine
            .append_rows(vec![expected.clone()])
            .expect("append trace row");

        {
            let active_segment = engine.active_segments[0].lock();
            let active_segment = active_segment.as_ref().expect("active segment");
            assert!(
                active_segment.cached_payload_bytes.is_empty(),
                "deferred WAL should not duplicate row payloads into cached payload bytes"
            );
            let rows = active_segment
                .read_rows_for_trace("trace-deferred-read-1", 0, i64::MAX)
                .expect("read rows for trace");
            assert_eq!(rows, vec![expected.clone()]);
        }

        drop(engine);
        fs::remove_dir_all(path).expect("cleanup temp dir");
    }

    #[test]
    fn coalesced_trace_block_preparation_preserves_rows_across_multiple_blocks() {
        let trace_shards = 1;
        let blocks = vec![
            TraceBlock::from_rows(vec![
                trace_row(
                    "trace-coalesce-1",
                    "span-1",
                    "op-a",
                    100,
                    150,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.method", "GET"),
                    ],
                ),
                trace_row(
                    "trace-coalesce-1",
                    "span-2",
                    "op-a",
                    160,
                    210,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.status_code", "200"),
                    ],
                ),
            ]),
            TraceBlock::from_rows(vec![
                trace_row(
                    "trace-coalesce-2",
                    "span-1",
                    "op-b",
                    300,
                    350,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.method", "POST"),
                    ],
                ),
                trace_row(
                    "trace-coalesce-2",
                    "span-2",
                    "op-b",
                    360,
                    410,
                    &[
                        ("resource_attr:service.name", "checkout"),
                        ("http.status_code", "201"),
                    ],
                ),
            ]),
        ];

        let individual_batch = PreparedTraceShardBatch::from_prepared_blocks(
            blocks
                .clone()
                .into_iter()
                .map(|block| PreparedTraceBlockAppend::new(block, trace_shards))
                .collect(),
        );
        let coalesced_block = coalesce_trace_blocks_for_shard(blocks.clone());

        assert_eq!(coalesced_block.row_count(), 4);

        let coalesced_batch =
            PreparedTraceShardBatch::from_prepared_blocks(vec![PreparedTraceBlockAppend::new(
                coalesced_block,
                trace_shards,
            )]);

        let decode_rows = |batch: &PreparedTraceShardBatch| {
            batch
                .encoded_row_ranges
                .iter()
                .map(|range| {
                    decode_trace_row(&batch.encoded_row_bytes[range.clone()])
                        .expect("decode coalesced row")
                })
                .collect::<Vec<_>>()
        };

        assert_eq!(
            decode_rows(&coalesced_batch),
            decode_rows(&individual_batch)
        );
        assert_eq!(
            coalesced_batch
                .prepared_rows
                .iter()
                .map(|row| row.trace_id.clone())
                .collect::<Vec<_>>(),
            individual_batch
                .prepared_rows
                .iter()
                .map(|row| row.trace_id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            coalesced_batch
                .prepared_rows
                .iter()
                .map(|row| row.operation.clone())
                .collect::<Vec<_>>(),
            individual_batch
                .prepared_rows
                .iter()
                .map(|row| row.operation.clone())
                .collect::<Vec<_>>()
        );
        assert!(coalesced_batch
            .prepared_rows
            .iter()
            .all(|row| row.shard_index == 0));
        assert_eq!(
            blocks
                .into_iter()
                .flat_map(|block| block.rows())
                .collect::<Vec<_>>(),
            decode_rows(&coalesced_batch)
        );
    }

    #[test]
    fn segment_accumulator_batches_contiguous_prepared_rows_for_same_trace() {
        let block = TraceBlock::from_rows(vec![
            trace_row(
                "trace-batch-1",
                "span-1",
                "op-a",
                100,
                150,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "GET"),
                    ("http.status_code", "200"),
                ],
            ),
            trace_row(
                "trace-batch-1",
                "span-2",
                "op-a",
                90,
                170,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "GET"),
                    ("http.status_code", "200"),
                ],
            ),
            trace_row(
                "trace-batch-1",
                "span-3",
                "op-b",
                180,
                220,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "POST"),
                    ("span.kind", "server"),
                ],
            ),
        ]);
        let prepared_shared_groups = prepare_shared_group_metadata(&block);
        let prepared_rows = prepare_block_row_metadata(&block, &prepared_shared_groups, 2);
        let row_locations = vec![
            row(11, 0, 100, 150),
            row(11, 1, 90, 170),
            row(11, 2, 180, 220),
        ];

        let mut accumulator = SegmentAccumulator::default();
        let mut intern_cache = BatchInternCache::default();
        accumulator.ingest_prepared_block_rows(
            &prepared_rows,
            &prepared_shared_groups,
            &row_locations,
            &mut intern_cache,
        );

        assert_eq!(accumulator.row_count, 3);
        assert_eq!(accumulator.min_time_unix_nano, Some(90));
        assert_eq!(accumulator.max_time_unix_nano, Some(220));

        let trace_ref = accumulator
            .trace_ids
            .lookup("trace-batch-1")
            .expect("trace should be interned");
        let trace = accumulator
            .traces
            .get(&trace_ref)
            .expect("trace accumulator should exist");
        assert_eq!(trace.window.start_unix_nano, 90);
        assert_eq!(trace.window.end_unix_nano, 220);
        assert_eq!(
            trace
                .services
                .iter()
                .filter_map(|value| accumulator.strings.resolve(*value))
                .collect::<Vec<_>>(),
            vec!["checkout"]
        );
        assert_eq!(
            trace
                .operations
                .iter()
                .filter_map(|value| accumulator.strings.resolve(*value))
                .collect::<Vec<_>>(),
            vec!["op-a", "op-b"]
        );
        let mut indexed_fields = trace
            .fields
            .iter()
            .filter_map(|(name, value)| {
                Some((
                    accumulator.strings.resolve(*name)?,
                    accumulator.strings.resolve(*value)?,
                ))
            })
            .collect::<Vec<_>>();
        indexed_fields.sort_unstable();
        assert_eq!(
            indexed_fields,
            vec![
                ("http.method", "GET"),
                ("http.method", "POST"),
                ("http.status_code", "200"),
                ("resource_attr:service.name", "checkout"),
                ("span.kind", "server"),
            ]
        );
        assert_eq!(
            trace
                .rows
                .iter()
                .map(|location| {
                    (
                        location.row_index,
                        location.start_unix_nano,
                        location.end_unix_nano,
                    )
                })
                .collect::<Vec<_>>(),
            row_locations
                .iter()
                .map(|location| {
                    (
                        location.row_index,
                        location.start_unix_nano,
                        location.end_unix_nano,
                    )
                })
                .collect::<Vec<_>>()
        );
    }

}
