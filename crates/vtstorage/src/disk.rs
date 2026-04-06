use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    sync::mpsc,
    sync::Arc,
    time::Instant,
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
const PART_FIELD_TYPE_STRING: u8 = 0;
const PART_FIELD_TYPE_I64: u8 = 1;
const PART_FIELD_TYPE_BOOL: u8 = 2;

type TraceRef = u32;
type StringRef = u32;
type SmallArcList = SmallVec<[Arc<str>; 4]>;
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
    fn requires_sync(self) -> bool {
        matches!(self, Self::Data)
    }
}

#[derive(Debug, Clone)]
pub struct DiskStorageConfig {
    target_segment_size_bytes: u64,
    sync_policy: DiskSyncPolicy,
    trace_shards: usize,
}

impl Default for DiskStorageConfig {
    fn default() -> Self {
        Self {
            target_segment_size_bytes: DEFAULT_TARGET_SEGMENT_SIZE_BYTES,
            sync_policy: DiskSyncPolicy::None,
            trace_shards: default_trace_shards(),
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
}

#[derive(Debug)]
pub struct DiskStorageEngine {
    root_path: PathBuf,
    segments_path: PathBuf,
    segment_paths: RwLock<FxHashMap<u64, PathBuf>>,
    trace_shards: Vec<RwLock<DiskTraceShardState>>,
    pending_trace_updates: Mutex<Vec<(usize, Vec<LiveTraceUpdate>)>>,
    append_combiners: Vec<Mutex<DiskTraceAppendCombinerState>>,
    append_combiner_stats: Vec<DiskTraceAppendCombinerStats>,
    active_segments: Vec<Mutex<Option<ActiveSegment>>>,
    log_state: RwLock<LogIndexedState>,
    log_writer: Mutex<BufWriter<File>>,
    next_segment_id: AtomicU64,
    config: DiskStorageConfig,
    persisted_bytes: AtomicU64,
    trace_window_lookups: AtomicU64,
    row_queries: AtomicU64,
    segment_read_batches: AtomicU64,
    part_selective_decodes: AtomicU64,
    fsync_operations: AtomicU64,
}

#[derive(Debug, Default)]
struct DiskTraceShardState {
    trace_ids: StringTable,
    strings: StringTable,
    all_trace_refs: RoaringBitmap,
    windows_by_trace: FxHashMap<TraceRef, TraceWindowBounds>,
    services_by_trace: FxHashMap<TraceRef, Vec<StringRef>>,
    trace_refs_by_service: FxHashMap<StringRef, RoaringBitmap>,
    trace_refs_by_operation: FxHashMap<StringRef, RoaringBitmap>,
    trace_refs_by_field_name_value: FxHashMap<StringRef, FxHashMap<StringRef, RoaringBitmap>>,
    field_values_by_name: FxHashMap<StringRef, Vec<StringRef>>,
    row_refs_by_trace: FxHashMap<TraceRef, Vec<SegmentRowLocation>>,
    rows_ingested: u64,
}

#[derive(Debug, Default)]
struct StringTable {
    ids_by_value: FxHashMap<Arc<str>, u32>,
    values_by_id: Vec<Arc<str>>,
}

#[derive(Debug, Clone, Copy)]
struct TraceWindowBounds {
    start_unix_nano: i64,
    end_unix_nano: i64,
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
    next_segment_id: u64,
}

#[derive(Debug)]
struct ActiveSegment {
    segment_id: u64,
    wal_path: PathBuf,
    writer: BufWriter<File>,
    accumulator: SegmentAccumulator,
    cached_row_length_prefixes: Vec<u8>,
    cached_payload_ranges: Vec<std::ops::Range<usize>>,
    cached_payload_bytes: Vec<u8>,
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

#[derive(Debug)]
struct LiveTraceUpdate {
    trace_id: Arc<str>,
    start_unix_nano: i64,
    end_unix_nano: i64,
    services: SmallArcList,
    operations: SmallArcList,
    indexed_fields: SmallArcPairList,
    row_locations: Vec<SegmentRowLocation>,
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

impl LiveTraceUpdate {
    fn new(trace_id: Arc<str>, start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            trace_id,
            start_unix_nano,
            end_unix_nano,
            services: SmallArcList::new(),
            operations: SmallArcList::new(),
            indexed_fields: SmallArcPairList::new(),
            row_locations: Vec::new(),
        }
    }

    fn observe_window(&mut self, start_unix_nano: i64, end_unix_nano: i64) {
        if start_unix_nano < self.start_unix_nano {
            self.start_unix_nano = start_unix_nano;
        }
        if end_unix_nano > self.end_unix_nano {
            self.end_unix_nano = end_unix_nano;
        }
    }
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentMeta {
    segment_id: u64,
    segment_file: String,
    row_count: u64,
    persisted_bytes: u64,
    min_time_unix_nano: i64,
    max_time_unix_nano: i64,
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
    meta_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct CompactableSegment {
    segment_id: u64,
    part_path: PathBuf,
    meta_path: PathBuf,
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
        let mut recovered = recover_state(&segments_path, trace_shards)?;
        let log_path = root_path.join("logs.wal");
        let (log_state, log_persisted_bytes) = recover_log_state(&log_path)?;
        recovered.persisted_bytes += log_persisted_bytes;
        let mut next_segment_id = recovered.next_segment_id;
        if compact_persisted_segments(
            &segments_path,
            &mut next_segment_id,
            compaction_target_segment_size_bytes(&config),
        )? {
            let compacted = recover_state(&segments_path, trace_shards)?;
            recovered = compacted;
            recovered.persisted_bytes += log_persisted_bytes;
            next_segment_id = next_segment_id.max(recovered.next_segment_id);
        }
        let active_segments = (0..trace_shards)
            .map(|_| Mutex::new(None))
            .collect::<Vec<_>>();
        let append_combiners = (0..trace_shards)
            .map(|_| Mutex::new(DiskTraceAppendCombinerState::default()))
            .collect::<Vec<_>>();
        let append_combiner_stats = (0..trace_shards)
            .map(|_| DiskTraceAppendCombinerStats::default())
            .collect::<Vec<_>>();
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
            segment_paths: RwLock::new(recovered.segment_paths),
            trace_shards: recovered
                .trace_shards
                .into_iter()
                .map(RwLock::new)
                .collect(),
            pending_trace_updates: Mutex::new(Vec::new()),
            append_combiners,
            append_combiner_stats,
            active_segments,
            log_state: RwLock::new(log_state),
            log_writer: Mutex::new(log_writer),
            next_segment_id: AtomicU64::new(next_segment_id),
            config,
            persisted_bytes: AtomicU64::new(recovered.persisted_bytes),
            trace_window_lookups: AtomicU64::new(0),
            row_queries: AtomicU64::new(0),
            segment_read_batches: AtomicU64::new(0),
            part_selective_decodes: AtomicU64::new(0),
            fsync_operations: AtomicU64::new(0),
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
            *active_segment = Some(ActiveSegment::open(&self.segments_path, next_segment_id)?);
        }
        Ok(active_segment
            .as_mut()
            .expect("active segment should be initialized"))
    }

    fn rotate_active_segment(
        &self,
        active_segment: &mut ActiveSegment,
    ) -> Result<(), StorageError> {
        if active_segment.flush(self.config.sync_policy)? {
            self.fsync_operations.fetch_add(1, Ordering::Relaxed);
        }
        let next_segment_id = self.next_segment_id.fetch_add(1, Ordering::Relaxed);
        *active_segment = ActiveSegment::open(&self.segments_path, next_segment_id)?;
        Ok(())
    }

    fn finalize_active_segment(
        &self,
        active_segment: &mut ActiveSegment,
    ) -> Result<(), StorageError> {
        if active_segment.is_empty() {
            return Ok(());
        }

        if active_segment.flush(self.config.sync_policy)? {
            self.fsync_operations.fetch_add(1, Ordering::Relaxed);
        }
        let part_path = segment_part_path(&self.segments_path, active_segment.segment_id);
        let (old_segment_bytes, new_segment_bytes, part_synced) = seal_rows_file_to_part(
            &active_segment.wal_path,
            &part_path,
            self.config.sync_policy,
        )?;
        if part_synced {
            self.fsync_operations.fetch_add(1, Ordering::Relaxed);
        }
        let meta = active_segment.build_meta(&part_path)?;
        let meta_path = segment_meta_path(&self.segments_path, active_segment.segment_id);
        let (meta_bytes, meta_synced) =
            write_segment_meta(&meta_path, &meta, self.config.sync_policy)?;
        if meta_synced {
            self.fsync_operations.fetch_add(1, Ordering::Relaxed);
        }
        self.replace_persisted_bytes(old_segment_bytes, new_segment_bytes, meta_bytes);
        self.segment_paths
            .write()
            .insert(active_segment.segment_id, part_path);
        Ok(())
    }

    fn replace_persisted_bytes(
        &self,
        old_segment_bytes: u64,
        new_segment_bytes: u64,
        extra_bytes: u64,
    ) {
        let mut current = self.persisted_bytes.load(Ordering::Relaxed);
        loop {
            let updated = current
                .saturating_sub(old_segment_bytes)
                .saturating_add(new_segment_bytes)
                .saturating_add(extra_bytes);
            match self.persisted_bytes.compare_exchange(
                current,
                updated,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }

    fn flush_log_writer(&self) -> Result<(), StorageError> {
        let mut writer = self.log_writer.lock();
        writer.flush()?;
        if self.config.sync_policy.requires_sync() {
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    fn trace_shard_index(&self, trace_id: &str) -> usize {
        trace_shard_index(trace_id, self.trace_shards.len())
    }

    fn publish_live_segment_paths(&self, segment_paths: FxHashMap<u64, PathBuf>) {
        if segment_paths.is_empty() {
            return;
        }
        let mut live_segment_paths = self.segment_paths.write();
        for (segment_id, segment_path) in segment_paths {
            live_segment_paths.entry(segment_id).or_insert(segment_path);
        }
    }

    fn enqueue_live_trace_updates_for_shard(
        &self,
        shard_index: usize,
        updates: Vec<LiveTraceUpdate>,
    ) {
        if updates.is_empty() {
            return;
        }
        let mut pending = self.pending_trace_updates.lock();
        pending.push((shard_index, updates));
    }

    fn drain_pending_trace_updates(&self) {
        let pending = {
            let mut pending = self.pending_trace_updates.lock();
            if pending.is_empty() {
                return;
            }
            std::mem::take(&mut *pending)
        };
        for (shard_index, updates) in pending {
            self.trace_shards[shard_index]
                .write()
                .observe_live_updates(updates);
        }
    }

    fn append_prepared_trace_rows(
        &self,
        prepared_rows: &[PreparedBlockRowMetadata],
        prepared_shared_groups: &[PreparedSharedGroupMetadata],
        encoded_row_bytes: &[u8],
        encoded_row_ranges: &[std::ops::Range<usize>],
        active_segment: &mut ActiveSegment,
        observed_segment_paths: &mut FxHashMap<u64, PathBuf>,
    ) -> Result<Vec<LiveTraceUpdate>, StorageError> {
        let row_count = prepared_rows.len();
        let mut live_updates = Vec::new();

        let mut batch_start = 0usize;
        let mut pending_batch_bytes = 0u64;
        let mut pending_batch_payload_bytes = 0usize;
        let mut pending_batch_row_count = 0usize;
        for index in 0..row_count {
            let mut effective_segment_bytes =
                active_segment.persisted_bytes() + pending_batch_bytes;
            if active_segment.is_empty() && batch_start < index {
                effective_segment_bytes += WAL_HEADER_BYTES;
            }
            if (batch_start < index || !active_segment.is_empty())
                && effective_segment_bytes >= self.config.target_segment_size_bytes
            {
                if batch_start < index {
                    observed_segment_paths
                        .entry(active_segment.segment_id)
                        .or_insert_with(|| active_segment.wal_path.clone());
                    let (batch_updates, bytes_written) = active_segment.append_block_rows(
                        prepared_rows,
                        prepared_shared_groups,
                        &encoded_row_bytes,
                        &encoded_row_ranges,
                        batch_start,
                        index,
                    )?;
                    self.persisted_bytes
                        .fetch_add(bytes_written, Ordering::Relaxed);
                    merge_live_updates(&mut live_updates, batch_updates);
                }
                self.rotate_active_segment(active_segment)?;
                batch_start = index;
                pending_batch_payload_bytes = 0;
                pending_batch_row_count = 0;
            }
            pending_batch_payload_bytes += encoded_row_ranges[index].len();
            pending_batch_row_count += 1;
            pending_batch_bytes =
                wal_batch_storage_bytes(pending_batch_payload_bytes, pending_batch_row_count);
        }
        if batch_start < row_count {
            observed_segment_paths
                .entry(active_segment.segment_id)
                .or_insert_with(|| active_segment.wal_path.clone());
            let (batch_updates, bytes_written) = active_segment.append_block_rows(
                prepared_rows,
                prepared_shared_groups,
                &encoded_row_bytes,
                &encoded_row_ranges,
                batch_start,
                row_count,
            )?;
            self.persisted_bytes
                .fetch_add(bytes_written, Ordering::Relaxed);
            merge_live_updates(&mut live_updates, batch_updates);
        }

        Ok(live_updates)
    }

    fn append_prepared_trace_shard_batch(
        &self,
        prepared_batch: &PreparedTraceShardBatch,
        active_segment: &mut ActiveSegment,
        observed_segment_paths: &mut FxHashMap<u64, PathBuf>,
    ) -> Result<Vec<LiveTraceUpdate>, StorageError> {
        self.append_prepared_trace_rows(
            &prepared_batch.prepared_rows,
            &prepared_batch.prepared_shared_groups,
            &prepared_batch.encoded_row_bytes,
            &prepared_batch.encoded_row_ranges,
            active_segment,
            observed_segment_paths,
        )
    }

    fn append_prepared_trace_shard_batch_once(
        &self,
        prepared_batch: PreparedTraceShardBatch,
    ) -> Result<(), StorageError> {
        let mut observed_segment_paths = FxHashMap::default();
        let mut flush_count = 0u64;
        let mut active_segment_slot = self.active_segments[prepared_batch.shard_index].lock();
        let active_segment = self.ensure_active_segment(&mut active_segment_slot)?;
        let live_updates = self.append_prepared_trace_shard_batch(
            &prepared_batch,
            active_segment,
            &mut observed_segment_paths,
        )?;
        if self.config.sync_policy.requires_sync()
            && active_segment.flush(self.config.sync_policy)?
        {
            flush_count += 1;
        }
        drop(active_segment_slot);

        self.publish_live_segment_paths(observed_segment_paths);
        self.enqueue_live_trace_updates_for_shard(prepared_batch.shard_index, live_updates);
        if flush_count > 0 {
            self.fsync_operations
                .fetch_add(flush_count, Ordering::Relaxed);
        }
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
            self.append_combiner_stats[shard_index].record_flush(
                row_count,
                input_blocks,
                wait_micros,
            );

            let result = self
                .append_prepared_trace_shard_batch_once(
                    PreparedTraceShardBatch::from_prepared_blocks(prepared_blocks),
                )
                .map_err(|error| error.to_string());
            for responder in responders {
                let _ = responder.send(result.clone());
            }
        }
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
}

impl Drop for DiskStorageEngine {
    fn drop(&mut self) {
        for active_segment in &self.active_segments {
            let mut active_segment = active_segment.lock();
            if let Some(active_segment) = active_segment.as_mut() {
                let _ = self.finalize_active_segment(active_segment);
            }
        }
        let _ = self.flush_log_writer();
    }
}

impl StorageEngine for DiskStorageEngine {
    fn append_trace_block(&self, block: TraceBlock) -> Result<(), StorageError> {
        if block.is_empty() {
            return Ok(());
        }

        self.append_trace_blocks(vec![block])
    }

    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        let mut blocks_by_shard = (0..self.trace_shards.len())
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        for block in blocks {
            if block.is_empty() {
                continue;
            }
            if let Some(shard_index) = trace_block_shard_index(&block, self.trace_shards.len()) {
                blocks_by_shard[shard_index].push(block);
                continue;
            }
            for block in partition_trace_block_by_shard(block, self.trace_shards.len()) {
                let shard_index =
                    trace_block_shard_index(&block, self.trace_shards.len()).unwrap_or(0);
                blocks_by_shard[shard_index].push(block);
            }
        }
        for (shard_index, shard_blocks) in blocks_by_shard.into_iter().enumerate() {
            if shard_blocks.is_empty() {
                continue;
            }
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
        self.drain_pending_trace_updates();
        self.trace_window_lookups.fetch_add(1, Ordering::Relaxed);
        self.trace_shards[self.trace_shard_index(trace_id)]
            .read()
            .trace_window(trace_id)
    }

    fn list_trace_ids(&self) -> Vec<String> {
        self.drain_pending_trace_updates();
        let mut trace_ids = BTreeSet::new();
        for shard in &self.trace_shards {
            trace_ids.extend(shard.read().list_trace_ids());
        }
        trace_ids.into_iter().collect()
    }

    fn list_services(&self) -> Vec<String> {
        self.drain_pending_trace_updates();
        let mut services = BTreeSet::new();
        for shard in &self.trace_shards {
            services.extend(shard.read().list_services());
        }
        services.into_iter().collect()
    }

    fn list_field_names(&self) -> Vec<String> {
        self.drain_pending_trace_updates();
        let mut field_names = BTreeSet::new();
        for shard in &self.trace_shards {
            field_names.extend(shard.read().list_field_names());
        }
        field_names.into_iter().collect()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.drain_pending_trace_updates();
        let mut field_values = BTreeSet::new();
        for shard in &self.trace_shards {
            field_values.extend(shard.read().list_field_values(field_name));
        }
        field_values.into_iter().collect()
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.drain_pending_trace_updates();
        let mut hits = Vec::new();
        for shard in &self.trace_shards {
            hits.extend(shard.read().search_traces(request));
        }
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
        self.drain_pending_trace_updates();
        self.row_queries.fetch_add(1, Ordering::Relaxed);

        let row_refs = self.trace_shards[self.trace_shard_index(trace_id)]
            .read()
            .row_refs_for_trace(trace_id, start_unix_nano, end_unix_nano);
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
            {
                let active_segment = self.active_segments[self.trace_shard_index(trace_id)].lock();
                if let Some(active_segment) = active_segment.as_ref() {
                    if active_segment.segment_id == segment_id {
                        if let Ok(mut segment_rows) = active_segment.read_rows(&segment_refs) {
                            rows.append(&mut segment_rows);
                        }
                        continue;
                    }
                }
            }
            if path.extension().and_then(|value| value.to_str()) == Some("part") {
                self.part_selective_decodes.fetch_add(1, Ordering::Relaxed);
            }
            if let Ok(mut segment_rows) = read_rows_for_segment(path, &segment_refs) {
                rows.append(&mut segment_rows);
            }
        }
        rows.sort_by_key(|row| row.end_unix_nano);
        rows
    }

    fn stats(&self) -> StorageStatsSnapshot {
        self.drain_pending_trace_updates();
        let segment_paths = self.segment_paths.read();
        let (rows_ingested, traces_tracked) =
            self.trace_shards
                .iter()
                .fold((0u64, 0u64), |(rows_acc, traces_acc), shard| {
                    let state = shard.read();
                    (
                        rows_acc + state.rows_ingested(),
                        traces_acc + state.traces_tracked(),
                    )
                });
        let (typed_field_columns, string_field_columns) =
            count_field_column_encodings(segment_paths.values());
        StorageStatsSnapshot {
            rows_ingested,
            traces_tracked,
            retained_trace_blocks: 0,
            persisted_bytes: self.persisted_bytes.load(Ordering::Relaxed),
            segment_count: segment_paths.len() as u64,
            typed_field_columns,
            string_field_columns,
            trace_window_lookups: self.trace_window_lookups.load(Ordering::Relaxed),
            row_queries: self.row_queries.load(Ordering::Relaxed),
            segment_read_batches: self.segment_read_batches.load(Ordering::Relaxed),
            part_selective_decodes: self.part_selective_decodes.load(Ordering::Relaxed),
            fsync_operations: self.fsync_operations.load(Ordering::Relaxed),
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
    fn observe_live_updates(&mut self, updates: Vec<LiveTraceUpdate>) {
        for update in updates {
            let mut intern_cache = BatchInternCache::default();
            let trace_ref = intern_cache.trace_ref(&mut self.trace_ids, update.trace_id.as_ref());
            self.observe_trace_metadata(trace_ref, update.start_unix_nano, update.end_unix_nano);
            self.merge_trace_services(trace_ref, &update.services, &mut intern_cache);
            self.merge_trace_operations(trace_ref, &update.operations, &mut intern_cache);
            self.merge_trace_fields(trace_ref, &update.indexed_fields, &mut intern_cache);
            let row_refs = self.row_refs_by_trace.entry(trace_ref).or_default();
            let row_count = update.row_locations.len() as u64;
            merge_sorted_row_locations(row_refs, update.row_locations);
            self.rows_ingested += row_count;
        }
    }

    fn observe_persisted_trace(&mut self, trace: SegmentTraceMeta) {
        let trace_ref = self.trace_ids.intern(&trace.trace_id);
        self.observe_trace_metadata(trace_ref, trace.start_unix_nano, trace.end_unix_nano);

        let trace_services = self.services_by_trace.entry(trace_ref).or_default();
        for service_name in trace.services {
            let service_ref = self.strings.intern(&service_name);
            if push_unique_string_ref(trace_services, service_ref) {
                self.trace_refs_by_service
                    .entry(service_ref)
                    .or_default()
                    .insert(trace_ref);
            }
        }
        for operation_name in trace.operations {
            let operation_ref = self.strings.intern(&operation_name);
            self.trace_refs_by_operation
                .entry(operation_ref)
                .or_default()
                .insert(trace_ref);
        }
        for (field_name, values) in trace.fields {
            let field_name_ref = self.strings.intern(&field_name);
            let trace_refs_by_value = self
                .trace_refs_by_field_name_value
                .entry(field_name_ref)
                .or_default();
            let field_values = self.field_values_by_name.entry(field_name_ref).or_default();
            for value in values {
                let field_value_ref = self.strings.intern(&value);
                trace_refs_by_value
                    .entry(field_value_ref)
                    .or_default()
                    .insert(trace_ref);
                push_unique_string_ref(field_values, field_value_ref);
            }
        }

        let row_refs = self.row_refs_by_trace.entry(trace_ref).or_default();
        let row_count = trace.rows.len() as u64;
        merge_sorted_row_locations(row_refs, trace.rows);
        self.rows_ingested += row_count;
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

    fn merge_trace_services<'a>(
        &mut self,
        trace_ref: TraceRef,
        services: &'a [Arc<str>],
        intern_cache: &mut BatchInternCache<'a>,
    ) {
        if services.is_empty() {
            return;
        }

        let trace_services = self.services_by_trace.entry(trace_ref).or_default();
        for service_name in services {
            let service_ref = intern_cache.string_ref(&mut self.strings, service_name.as_ref());
            if push_unique_string_ref(trace_services, service_ref) {
                self.trace_refs_by_service
                    .entry(service_ref)
                    .or_default()
                    .insert(trace_ref);
            }
        }
    }

    fn merge_trace_operations<'a>(
        &mut self,
        trace_ref: TraceRef,
        operations: &'a [Arc<str>],
        intern_cache: &mut BatchInternCache<'a>,
    ) {
        if operations.is_empty() {
            return;
        }

        for operation_name in operations {
            let operation_ref = intern_cache.string_ref(&mut self.strings, operation_name.as_ref());
            self.trace_refs_by_operation
                .entry(operation_ref)
                .or_default()
                .insert(trace_ref);
        }
    }

    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        let trace_ref = self.trace_ids.lookup(trace_id)?;
        let bounds = self.windows_by_trace.get(&trace_ref)?;
        Some(TraceWindow::new(
            self.trace_ids.resolve(trace_ref)?.to_string(),
            bounds.start_unix_nano,
            bounds.end_unix_nano,
        ))
    }

    fn list_services(&self) -> Vec<String> {
        let mut services: Vec<String> = self
            .trace_refs_by_service
            .keys()
            .filter_map(|value| self.strings.resolve(*value).map(ToString::to_string))
            .collect();
        services.sort();
        services
    }

    fn list_field_names(&self) -> Vec<String> {
        let mut field_names: Vec<String> = self
            .field_values_by_name
            .keys()
            .filter_map(|value| self.strings.resolve(*value).map(ToString::to_string))
            .collect();
        field_names.sort();
        field_names
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
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

    fn traces_tracked(&self) -> u64 {
        self.windows_by_trace.len() as u64
    }

    fn merge_trace_fields<'a>(
        &mut self,
        trace_ref: TraceRef,
        indexed_fields: &'a [(Arc<str>, Arc<str>)],
        intern_cache: &mut BatchInternCache<'a>,
    ) {
        if indexed_fields.is_empty() {
            return;
        }

        for (field_name, field_value) in indexed_fields {
            let field_name_ref = intern_cache.string_ref(&mut self.strings, field_name.as_ref());
            let field_value_ref = intern_cache.string_ref(&mut self.strings, field_value.as_ref());
            self.trace_refs_by_field_name_value
                .entry(field_name_ref)
                .or_default()
                .entry(field_value_ref)
                .or_default()
                .insert(trace_ref);
            push_unique_string_ref(
                self.field_values_by_name.entry(field_name_ref).or_default(),
                field_value_ref,
            );
        }
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

impl ActiveSegment {
    fn open(segments_path: &Path, segment_id: u64) -> Result<Self, StorageError> {
        let wal_path = segment_wal_path(segments_path, segment_id);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;

        Ok(Self {
            segment_id,
            wal_path,
            writer: BufWriter::with_capacity(TRACE_WAL_WRITER_CAPACITY_BYTES, file),
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
    ) -> Result<(Vec<LiveTraceUpdate>, u64), StorageError> {
        if start_index >= end_index {
            return Ok((Vec::new(), 0));
        }

        let total_payload_bytes: usize = encoded_row_ranges[start_index..end_index]
            .iter()
            .map(|range| range.len())
            .sum();
        let first_write = self.accumulator.row_count == 0 && self.accumulator.persisted_bytes == 0;
        let row_count = end_index - start_index;
        let bytes_written = wal_batch_storage_bytes(total_payload_bytes, row_count)
            + if first_write { WAL_HEADER_BYTES } else { 0 };
        if first_write {
            self.writer.write_all(WAL_MAGIC)?;
            self.writer.write_all(&[WAL_VERSION_BATCHED_BINARY])?;
        }

        let batch_body_len: u32 = wal_batch_body_bytes(total_payload_bytes, row_count)
            .try_into()
            .map_err(|_| {
                StorageError::Message("trace wal batch body exceeds u32 length".to_string())
            })?;
        let row_count_bytes = (row_count as u32).to_le_bytes();
        let batch_payload_start = encoded_row_ranges[start_index].start;
        let batch_payload_end = encoded_row_ranges[end_index - 1].end;
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
        self.writer.write_all(&batch_body_len.to_le_bytes())?;
        self.writer
            .write_all(&checksum_hasher.finalize().to_le_bytes())?;
        self.writer.write_all(&row_count_bytes)?;
        self.writer.write_all(&self.cached_row_length_prefixes)?;

        let cached_payload_base = self.cached_payload_bytes.len();
        self.cached_payload_bytes
            .extend_from_slice(&encoded_row_bytes[batch_payload_start..batch_payload_end]);
        self.writer
            .write_all(&encoded_row_bytes[batch_payload_start..batch_payload_end])?;
        self.cached_payload_ranges.reserve(row_count);
        let mut intern_cache = BatchInternCache::default();
        let base_row_index = self.accumulator.row_count;

        let mut row_locations = Vec::with_capacity(row_count);
        for (batch_offset, row_index) in (start_index..end_index).enumerate() {
            let payload = &encoded_row_bytes[encoded_row_ranges[row_index].clone()];
            let payload_start =
                cached_payload_base + (encoded_row_ranges[row_index].start - batch_payload_start);
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
        let live_updates = self.accumulator.observe_prepared_block_rows(
            &prepared_rows[start_index..end_index],
            prepared_shared_groups,
            &row_locations,
            &mut intern_cache,
        );
        self.accumulator.persisted_bytes += bytes_written;
        Ok((live_updates, bytes_written))
    }

    fn read_rows(
        &self,
        row_refs: &[SegmentRowLocation],
    ) -> Result<Vec<TraceSpanRow>, StorageError> {
        let mut rows = Vec::with_capacity(row_refs.len());
        for row_ref in row_refs {
            let Some(range) = self.cached_payload_ranges.get(row_ref.row_index as usize) else {
                return Err(StorageError::Message(format!(
                    "active segment row {} missing from cache",
                    row_ref.row_index
                )));
            };
            rows.push(
                decode_trace_row(&self.cached_payload_bytes[range.clone()])
                    .map_err(|err| StorageError::Message(err.to_string()))?,
            );
        }
        Ok(rows)
    }

    fn persisted_bytes(&self) -> u64 {
        self.accumulator.persisted_bytes
    }

    fn is_empty(&self) -> bool {
        self.accumulator.row_count == 0
    }

    fn flush(&mut self, sync_policy: DiskSyncPolicy) -> Result<bool, StorageError> {
        self.writer.flush()?;
        if sync_policy.requires_sync() {
            self.writer.get_ref().sync_data()?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn build_meta(&self, segment_path: &Path) -> Result<SegmentMeta, StorageError> {
        let (min_time_unix_nano, max_time_unix_nano) = self
            .accumulator
            .time_range()
            .ok_or_else(|| invalid_data("cannot build meta for empty segment"))?;

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
    fn observe_prepared_block_rows<'a>(
        &mut self,
        prepared_rows: &'a [PreparedBlockRowMetadata],
        prepared_shared_groups: &'a [PreparedSharedGroupMetadata],
        row_locations: &[SegmentRowLocation],
        intern_cache: &mut BatchInternCache<'a>,
    ) -> Vec<LiveTraceUpdate> {
        if prepared_rows.is_empty() {
            return Vec::new();
        }
        debug_assert_eq!(prepared_rows.len(), row_locations.len());

        let mut live_updates = Vec::new();
        let mut run_start = 0usize;
        while run_start < prepared_rows.len() {
            let trace_id = prepared_rows[run_start].trace_id.as_ref();
            let mut run_end = run_start + 1;
            while run_end < prepared_rows.len()
                && prepared_rows[run_end].trace_id.as_ref() == trace_id
            {
                run_end += 1;
            }
            if let Some(live_update) = self.observe_prepared_block_row_run(
                &prepared_rows[run_start..run_end],
                prepared_shared_groups,
                &row_locations[run_start..run_end],
                intern_cache,
            ) {
                live_updates.push(live_update);
            }
            run_start = run_end;
        }
        live_updates
    }

    fn observe_prepared_block_row_run<'a>(
        &mut self,
        prepared_rows: &'a [PreparedBlockRowMetadata],
        prepared_shared_groups: &'a [PreparedSharedGroupMetadata],
        row_locations: &[SegmentRowLocation],
        intern_cache: &mut BatchInternCache<'a>,
    ) -> Option<LiveTraceUpdate> {
        let Some(first_row) = prepared_rows.first() else {
            return None;
        };
        let Some(first_location) = row_locations.first() else {
            return None;
        };
        debug_assert_eq!(prepared_rows.len(), row_locations.len());

        if prepared_rows.len() == 1 {
            let start_unix_nano = first_location.start_unix_nano;
            let end_unix_nano = first_location.end_unix_nano;
            self.row_count += 1;
            self.observe_time_range(start_unix_nano, end_unix_nano);

            let trace_ref =
                intern_cache.trace_ref(&mut self.trace_ids, first_row.trace_id.as_ref());
            let service_ref = first_row.service.as_ref().map(|service_name| {
                intern_cache.string_ref(&mut self.strings, service_name.as_ref())
            });
            let operation_ref = first_row.operation.as_ref().map(|operation_name| {
                intern_cache.string_ref(&mut self.strings, operation_name.as_ref())
            });

            let trace = self
                .traces
                .entry(trace_ref)
                .or_insert_with(|| SegmentTraceAccumulator::new(start_unix_nano, end_unix_nano));
            trace.window.observe(start_unix_nano, end_unix_nano);
            let mut live_update =
                LiveTraceUpdate::new(first_row.trace_id.clone(), start_unix_nano, end_unix_nano);
            if let Some(service_ref) = service_ref {
                push_unique_string_ref(&mut trace.services, service_ref);
            }
            if let Some(service_name) = first_row.service.as_ref() {
                push_unique_arc(&mut live_update.services, service_name.clone());
            }
            if let Some(operation_ref) = operation_ref {
                push_unique_string_ref(&mut trace.operations, operation_ref);
            }
            if let Some(operation_name) = first_row.operation.as_ref() {
                push_unique_arc(&mut live_update.operations, operation_name.clone());
            }
            observe_prepared_row_indexed_fields(
                first_row,
                prepared_shared_groups,
                |field_name, field_value| {
                    let field_name_ref =
                        intern_cache.string_ref(&mut self.strings, field_name.as_ref());
                    let field_value_ref =
                        intern_cache.string_ref(&mut self.strings, field_value.as_ref());
                    push_unique_string_ref_pair(&mut trace.fields, field_name_ref, field_value_ref);
                    push_unique_arc_pair(
                        &mut live_update.indexed_fields,
                        field_name.clone(),
                        field_value.clone(),
                    );
                },
            );
            trace.rows.push(*first_location);
            live_update.row_locations.push(*first_location);
            return Some(live_update);
        }

        let mut min_start_unix_nano = first_location.start_unix_nano;
        let mut max_end_unix_nano = first_location.end_unix_nano;
        for row_location in row_locations.iter().skip(1) {
            min_start_unix_nano = min_start_unix_nano.min(row_location.start_unix_nano);
            max_end_unix_nano = max_end_unix_nano.max(row_location.end_unix_nano);
        }

        self.row_count += row_locations.len() as u64;
        self.observe_time_range(min_start_unix_nano, max_end_unix_nano);

        let trace_ref = intern_cache.trace_ref(&mut self.trace_ids, first_row.trace_id.as_ref());
        let trace = self.traces.entry(trace_ref).or_insert_with(|| {
            SegmentTraceAccumulator::new(min_start_unix_nano, max_end_unix_nano)
        });
        trace.window.observe(min_start_unix_nano, max_end_unix_nano);
        let mut live_update = LiveTraceUpdate::new(
            first_row.trace_id.clone(),
            min_start_unix_nano,
            max_end_unix_nano,
        );

        let mut batch_services = SmallVec::<[StringRef; 4]>::new();
        let mut batch_operations = SmallVec::<[StringRef; 4]>::new();
        let mut batch_fields = SmallVec::<[(StringRef, StringRef); 8]>::new();
        for prepared_row in prepared_rows {
            if let Some(service_name) = prepared_row.service.as_ref() {
                let service_ref = intern_cache.string_ref(&mut self.strings, service_name.as_ref());
                if !batch_services.contains(&service_ref) {
                    batch_services.push(service_ref);
                    live_update.services.push(service_name.clone());
                }
            }
            if let Some(operation_name) = prepared_row.operation.as_ref() {
                let operation_ref =
                    intern_cache.string_ref(&mut self.strings, operation_name.as_ref());
                if !batch_operations.contains(&operation_ref) {
                    batch_operations.push(operation_ref);
                    live_update.operations.push(operation_name.clone());
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
                        live_update
                            .indexed_fields
                            .push((field_name.clone(), field_value.clone()));
                    }
                },
            );
        }

        for service_ref in batch_services {
            push_unique_string_ref(&mut trace.services, service_ref);
        }
        for operation_ref in batch_operations {
            push_unique_string_ref(&mut trace.operations, operation_ref);
        }
        for (field_name_ref, field_value_ref) in batch_fields {
            push_unique_string_ref_pair(&mut trace.fields, field_name_ref, field_value_ref);
        }
        trace.rows.extend_from_slice(row_locations);
        live_update.row_locations.extend_from_slice(row_locations);
        Some(live_update)
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
            push_unique_string_ref(&mut trace.services, service_ref);
        }
        if let Some(operation_ref) = operation_ref {
            push_unique_string_ref(&mut trace.operations, operation_ref);
        }
        for (field_name_ref, field_value_ref) in indexed_fields {
            push_unique_string_ref_pair(&mut trace.fields, field_name_ref, field_value_ref);
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

        SegmentMeta {
            segment_id,
            segment_file,
            row_count: self.row_count,
            persisted_bytes: self.persisted_bytes,
            min_time_unix_nano,
            max_time_unix_nano,
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

    fn is_typed(&self) -> bool {
        !matches!(self, Self::String(_))
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
    segments_path: &Path,
    trace_shards: usize,
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
            SegmentFileKind::Meta => files.meta_path = Some(path),
        }
    }

    let trace_shards = trace_shards.max(1);
    let mut recovered = RecoveredDiskState {
        trace_shards: (0..trace_shards)
            .map(|_| DiskTraceShardState::default())
            .collect(),
        ..RecoveredDiskState::default()
    };
    let mut segment_ids: Vec<u64> = segment_files.keys().cloned().collect();
    segment_ids.sort_unstable();
    let next_segment_id = segment_ids
        .iter()
        .copied()
        .max()
        .map(|value| value + 1)
        .unwrap_or(1);

    for segment_id in segment_ids {
        let files = segment_files
            .remove(&segment_id)
            .ok_or_else(|| invalid_data("missing segment file set during recovery"))?;
        let source_path = files
            .part_path
            .or(files.wal_path)
            .or(files.legacy_rows_path)
            .ok_or_else(|| invalid_data("segment is missing data file"))?;
        let part_path = if source_path.extension().and_then(|value| value.to_str()) == Some("part")
        {
            source_path
        } else {
            let part_path = segment_part_path(segments_path, segment_id);
            if source_path != part_path {
                let (_, sealed_bytes, _) =
                    seal_rows_file_to_part(&source_path, &part_path, DiskSyncPolicy::None)?;
                if sealed_bytes == 0 {
                    continue;
                }
            }
            part_path
        };
        let meta_path = files
            .meta_path
            .unwrap_or_else(|| segment_meta_path(segments_path, segment_id));

        let meta = if meta_path.exists() {
            let file = File::open(&meta_path)?;
            serde_json::from_reader(file)?
        } else {
            let meta = rebuild_segment_meta(segment_id, &part_path)?;
            let _ = write_segment_meta(&meta_path, &meta, DiskSyncPolicy::None)?;
            meta
        };

        recovered.persisted_bytes += fs::metadata(&part_path)?.len();
        recovered.persisted_bytes += fs::metadata(&meta_path)?.len();
        recovered.segment_paths.insert(meta.segment_id, part_path);
        for trace in meta.traces {
            let shard_index = trace_shard_index(&trace.trace_id, trace_shards);
            recovered.trace_shards[shard_index].observe_persisted_trace(trace);
        }
    }

    recovered.next_segment_id = next_segment_id;
    Ok(recovered)
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

fn push_unique_arc(values: &mut SmallArcList, value: Arc<str>) {
    if values
        .iter()
        .any(|existing| existing.as_ref() == value.as_ref())
    {
        return;
    }
    values.push(value);
}

fn push_unique_arc_pair(values: &mut SmallArcPairList, name: Arc<str>, value: Arc<str>) {
    if values.iter().any(|(existing_name, existing_value)| {
        existing_name.as_ref() == name.as_ref() && existing_value.as_ref() == value.as_ref()
    }) {
        return;
    }
    values.push((name, value));
}

fn merge_live_updates(existing: &mut Vec<LiveTraceUpdate>, incoming: Vec<LiveTraceUpdate>) {
    for mut incoming_update in incoming {
        if let Some(existing_update) = existing.iter_mut().find(|existing_update| {
            existing_update.trace_id.as_ref() == incoming_update.trace_id.as_ref()
        }) {
            existing_update.observe_window(
                incoming_update.start_unix_nano,
                incoming_update.end_unix_nano,
            );
            for service_name in incoming_update.services.drain(..) {
                push_unique_arc(&mut existing_update.services, service_name);
            }
            for operation_name in incoming_update.operations.drain(..) {
                push_unique_arc(&mut existing_update.operations, operation_name);
            }
            for (field_name, field_value) in incoming_update.indexed_fields.drain(..) {
                push_unique_arc_pair(&mut existing_update.indexed_fields, field_name, field_value);
            }
            merge_sorted_row_locations(
                &mut existing_update.row_locations,
                incoming_update.row_locations,
            );
        } else {
            existing.push(incoming_update);
        }
    }
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
) {
    if values
        .iter()
        .any(|(existing_name, existing_value)| *existing_name == name && *existing_value == value)
    {
        return;
    }
    values.push((name, value));
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

#[cfg(test)]
fn collect_live_trace_updates_from_prepared_rows(
    prepared_rows: &[PreparedBlockRowMetadata],
    prepared_shared_groups: &[PreparedSharedGroupMetadata],
    row_locations: Vec<SegmentRowLocation>,
    trace_shards: usize,
) -> Vec<Vec<LiveTraceUpdate>> {
    if prepared_rows.first().is_some_and(|first_row| {
        prepared_rows.iter().skip(1).all(|prepared_row| {
            prepared_row.shard_index == first_row.shard_index
                && prepared_row.trace_id.as_ref() == first_row.trace_id.as_ref()
        })
    }) {
        let (shard_index, update) = collect_single_trace_live_update_from_prepared_rows(
            prepared_rows,
            prepared_shared_groups,
            row_locations,
        )
        .expect("single-trace fast path should build one update");
        let mut updates_by_shard: Vec<Vec<LiveTraceUpdate>> =
            (0..trace_shards.max(1)).map(|_| Vec::new()).collect();
        updates_by_shard[shard_index].push(update);
        return updates_by_shard;
    }

    let mut updates_by_shard: Vec<FxHashMap<Arc<str>, LiveTraceUpdate>> = (0..trace_shards.max(1))
        .map(|_| FxHashMap::default())
        .collect();
    debug_assert_eq!(prepared_rows.len(), row_locations.len());

    for (prepared_row, row_location) in prepared_rows.iter().zip(row_locations) {
        let shard_updates = &mut updates_by_shard[prepared_row.shard_index];
        let update = shard_updates
            .entry(prepared_row.trace_id.clone())
            .or_insert_with(|| {
                LiveTraceUpdate::new(
                    prepared_row.trace_id.clone(),
                    row_location.start_unix_nano,
                    row_location.end_unix_nano,
                )
            });
        update.observe_window(row_location.start_unix_nano, row_location.end_unix_nano);
        if let Some(service_name) = &prepared_row.service {
            push_unique_arc(&mut update.services, service_name.clone());
        }
        if let Some(operation_name) = &prepared_row.operation {
            push_unique_arc(&mut update.operations, operation_name.clone());
        }
        observe_prepared_row_indexed_fields(
            prepared_row,
            prepared_shared_groups,
            |field_name, field_value| {
                push_unique_arc_pair(
                    &mut update.indexed_fields,
                    field_name.clone(),
                    field_value.clone(),
                );
            },
        );
        update.row_locations.push(row_location);
    }

    updates_by_shard
        .into_iter()
        .map(|updates_by_trace| updates_by_trace.into_values().collect())
        .collect()
}

#[cfg(test)]
fn collect_single_trace_live_update_from_prepared_rows(
    prepared_rows: &[PreparedBlockRowMetadata],
    prepared_shared_groups: &[PreparedSharedGroupMetadata],
    row_locations: Vec<SegmentRowLocation>,
) -> Option<(usize, LiveTraceUpdate)> {
    let first_row = prepared_rows.first()?;
    let mut row_locations = row_locations.into_iter();
    let first_location = row_locations.next()?;
    let mut update = LiveTraceUpdate::new(
        first_row.trace_id.clone(),
        first_location.start_unix_nano,
        first_location.end_unix_nano,
    );

    if let Some(service_name) = &first_row.service {
        push_unique_arc(&mut update.services, service_name.clone());
    }
    if let Some(operation_name) = &first_row.operation {
        push_unique_arc(&mut update.operations, operation_name.clone());
    }
    observe_prepared_row_indexed_fields(
        first_row,
        prepared_shared_groups,
        |field_name, field_value| {
            push_unique_arc_pair(
                &mut update.indexed_fields,
                field_name.clone(),
                field_value.clone(),
            );
        },
    );
    update.row_locations.push(first_location);

    for (prepared_row, row_location) in prepared_rows.iter().skip(1).zip(row_locations) {
        update.observe_window(row_location.start_unix_nano, row_location.end_unix_nano);
        if let Some(service_name) = &prepared_row.service {
            push_unique_arc(&mut update.services, service_name.clone());
        }
        if let Some(operation_name) = &prepared_row.operation {
            push_unique_arc(&mut update.operations, operation_name.clone());
        }
        observe_prepared_row_indexed_fields(
            prepared_row,
            prepared_shared_groups,
            |field_name, field_value| {
                push_unique_arc_pair(
                    &mut update.indexed_fields,
                    field_name.clone(),
                    field_value.clone(),
                );
            },
        );
        update.row_locations.push(row_location);
    }

    Some((first_row.shard_index, update))
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
    push_unique_string_ref_pair(fields, field_name_ref, field_value_ref);
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

fn write_segment_meta(
    meta_path: &Path,
    meta: &SegmentMeta,
    sync_policy: DiskSyncPolicy,
) -> Result<(u64, bool), StorageError> {
    let payload = serde_json::to_vec_pretty(meta)?;
    let mut file = File::create(meta_path)?;
    file.write_all(&payload)?;
    file.flush()?;
    let synced = if sync_policy.requires_sync() {
        file.sync_data()?;
        true
    } else {
        false
    };
    Ok((payload.len() as u64, synced))
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

fn read_all_rows_from_part(path: &Path) -> Result<Vec<(TraceSpanRow, u64, u32)>, StorageError> {
    let part = read_part(path)?;
    Ok(part
        .rows()?
        .into_iter()
        .enumerate()
        .map(|(row_index, row)| (row, row_index as u64, 0))
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

fn seal_rows_file_to_part(
    source_path: &Path,
    part_path: &Path,
    sync_policy: DiskSyncPolicy,
) -> Result<(u64, u64, bool), StorageError> {
    let source_len = fs::metadata(source_path)?.len();
    let rows = read_all_rows_from_row_file(source_path, 0)?
        .into_iter()
        .map(|(row, _, _)| row)
        .collect::<Vec<_>>();
    if rows.is_empty() {
        fs::remove_file(source_path)?;
        return Ok((source_len, 0, false));
    }
    let part = ColumnarPart::from_rows(&rows);
    let (part_bytes, part_synced) = write_part(part_path, &part, sync_policy)?;
    fs::remove_file(source_path)?;
    Ok((source_len, part_bytes, part_synced))
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
            SegmentFileKind::Meta => files.meta_path = Some(path),
        }
    }

    let mut candidates = Vec::new();
    for (segment_id, files) in segment_files {
        let Some(part_path) = files.part_path else {
            continue;
        };
        let meta_path = files
            .meta_path
            .unwrap_or_else(|| segment_meta_path(segments_path, segment_id));
        let total_bytes = fs::metadata(&part_path)?.len()
            + fs::metadata(&meta_path).map(|meta| meta.len()).unwrap_or(0);
        candidates.push(CompactableSegment {
            segment_id,
            part_path,
            meta_path,
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
        rows.extend(
            read_all_rows_from_part(&segment.part_path)?
                .into_iter()
                .map(|(row, _, _)| row),
        );
    }

    if rows.is_empty() {
        return Ok(());
    }

    let compacted_segment_id = *next_segment_id;
    *next_segment_id += 1;
    let compacted_part_path = segment_part_path(segments_path, compacted_segment_id);
    let compacted_meta_path = segment_meta_path(segments_path, compacted_segment_id);
    let compacted_part = ColumnarPart::from_rows(&rows);
    let _ = write_part(&compacted_part_path, &compacted_part, DiskSyncPolicy::None)?;
    let compacted_meta =
        build_segment_meta_from_rows(compacted_segment_id, &compacted_part_path, &rows)?;
    let _ = write_segment_meta(&compacted_meta_path, &compacted_meta, DiskSyncPolicy::None)?;

    for segment in group {
        fs::remove_file(&segment.part_path)?;
        if segment.meta_path.exists() {
            fs::remove_file(&segment.meta_path)?;
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

fn write_i64_vec(writer: &mut File, values: &[i64]) -> Result<(), StorageError> {
    write_u32(writer, values.len() as u32)?;
    for value in values {
        writer.write_all(&value.to_le_bytes())?;
    }
    Ok(())
}

fn read_i64_vec(reader: &mut File) -> Result<Vec<i64>, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut values = Vec::with_capacity(len);
    for _ in 0..len {
        let mut bytes = [0u8; 8];
        reader.read_exact(&mut bytes)?;
        values.push(i64::from_le_bytes(bytes));
    }
    Ok(values)
}

fn write_string(writer: &mut File, value: &str) -> Result<(), StorageError> {
    write_u32(writer, value.len() as u32)?;
    writer.write_all(value.as_bytes())?;
    Ok(())
}

fn read_string(reader: &mut File) -> Result<String, StorageError> {
    let len = read_u32(reader)? as usize;
    let mut bytes = vec![0u8; len];
    reader.read_exact(&mut bytes)?;
    String::from_utf8(bytes).map_err(|error| invalid_data(error.to_string()).into())
}

fn write_u32(writer: &mut File, value: u32) -> Result<(), StorageError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_u8(writer: &mut File, value: u8) -> Result<(), StorageError> {
    writer.write_all(&[value])?;
    Ok(())
}

fn read_u32(reader: &mut File) -> Result<u32, StorageError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(u32::from_le_bytes(bytes))
}

fn read_u8(reader: &mut File) -> Result<u8, StorageError> {
    let mut bytes = [0u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0])
}

fn write_i32(writer: &mut File, value: i32) -> Result<(), StorageError> {
    writer.write_all(&value.to_le_bytes())?;
    Ok(())
}

fn write_i8(writer: &mut File, value: i8) -> Result<(), StorageError> {
    writer.write_all(&[value as u8])?;
    Ok(())
}

fn read_i8(reader: &mut File) -> Result<i8, StorageError> {
    let mut bytes = [0u8; 1];
    reader.read_exact(&mut bytes)?;
    Ok(bytes[0] as i8)
}

fn read_i32(reader: &mut File) -> Result<i32, StorageError> {
    let mut bytes = [0u8; 4];
    reader.read_exact(&mut bytes)?;
    Ok(i32::from_le_bytes(bytes))
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

fn count_field_column_encodings<'a, I>(paths: I) -> (u64, u64)
where
    I: IntoIterator<Item = &'a PathBuf>,
{
    let mut typed = 0u64;
    let mut string = 0u64;
    for path in paths {
        if path.extension().and_then(|value| value.to_str()) != Some("part") {
            continue;
        }
        let Ok(part) = read_part(path) else {
            continue;
        };
        for field_column in part.field_columns {
            if field_column.values.is_typed() {
                typed += 1;
            } else {
                string += 1;
            }
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

fn segment_part_path(segments_path: &Path, segment_id: u64) -> PathBuf {
    segments_path.join(format!("{SEGMENT_PREFIX}{segment_id:020}{PART_SUFFIX}"))
}

fn segment_meta_path(segments_path: &Path, segment_id: u64) -> PathBuf {
    segments_path.join(format!("{SEGMENT_PREFIX}{segment_id:020}{META_SUFFIX}"))
}

#[derive(Debug, Clone, Copy)]
enum SegmentFileKind {
    Wal,
    Part,
    LegacyRows,
    Meta,
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
        .map(|id| (id, SegmentFileKind::Meta))
}

fn invalid_data(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use super::{
        collect_live_trace_updates_from_prepared_rows, merge_sorted_row_locations,
        prepare_block_row_metadata, prepare_shared_group_metadata, trace_block_shard_index,
        trace_shard_index, BatchInternCache, PreparedTraceBlockAppend, PreparedTraceShardBatch,
        SegmentAccumulator, SegmentRowLocation, SegmentTraceAccumulator,
    };
    use serde_json::Value;
    use vtcore::{Field, TraceBlock, TraceSpanRow};

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
    fn prepared_block_row_metadata_can_feed_live_updates_without_rescanning_block() {
        let block = TraceBlock::from_rows(vec![
            trace_row(
                "trace-prepared-1",
                "span-1",
                "op-a",
                100,
                150,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "GET"),
                    ("span.kind", "server"),
                ],
            ),
            trace_row(
                "trace-prepared-1",
                "span-2",
                "op-a",
                160,
                210,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "GET"),
                    ("http.status_code", "200"),
                ],
            ),
            trace_row(
                "trace-prepared-1",
                "span-3",
                "op-b",
                220,
                260,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "POST"),
                    ("http.status_code", "200"),
                ],
            ),
        ]);

        let prepared_shared_groups = prepare_shared_group_metadata(&block);
        let prepared_rows = prepare_block_row_metadata(&block, &prepared_shared_groups, 4);
        assert_eq!(prepared_rows.len(), 3);
        let shard_index = trace_shard_index("trace-prepared-1", 4);
        assert!(prepared_rows
            .iter()
            .all(|prepared_row| prepared_row.shard_index == shard_index));

        let updates_by_shard = collect_live_trace_updates_from_prepared_rows(
            &prepared_rows,
            &prepared_shared_groups,
            vec![
                row(7, 0, 100, 150),
                row(7, 1, 160, 210),
                row(7, 2, 220, 260),
            ],
            4,
        );

        let updates = &updates_by_shard[shard_index];
        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        assert_eq!(update.trace_id.as_ref(), "trace-prepared-1");
        assert_eq!(update.start_unix_nano, 100);
        assert_eq!(update.end_unix_nano, 260);
        assert_eq!(
            update
                .services
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>(),
            vec!["checkout"]
        );
        assert_eq!(
            update
                .operations
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>(),
            vec!["op-a", "op-b"]
        );
        let mut indexed_fields = update
            .indexed_fields
            .iter()
            .map(|(name, value)| (name.as_ref(), value.as_ref()))
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
            update
                .row_locations
                .iter()
                .map(|location| location.row_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
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
        accumulator.observe_prepared_block_rows(
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

    #[test]
    fn segment_accumulator_returns_live_updates_without_rescanning_prepared_rows() {
        let trace_shards = 4;
        let block = TraceBlock::from_rows(vec![
            trace_row(
                "trace-live-1",
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
                "trace-live-1",
                "span-2",
                "op-a",
                160,
                210,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "GET"),
                    ("http.status_code", "200"),
                ],
            ),
            trace_row(
                "trace-live-1",
                "span-3",
                "op-b",
                220,
                260,
                &[
                    ("resource_attr:service.name", "checkout"),
                    ("http.method", "POST"),
                    ("span.kind", "server"),
                ],
            ),
        ]);
        let prepared_shared_groups = prepare_shared_group_metadata(&block);
        let prepared_rows =
            prepare_block_row_metadata(&block, &prepared_shared_groups, trace_shards);
        let row_locations = vec![
            row(17, 0, 100, 150),
            row(17, 1, 160, 210),
            row(17, 2, 220, 260),
        ];
        let expected_updates_by_shard = collect_live_trace_updates_from_prepared_rows(
            &prepared_rows,
            &prepared_shared_groups,
            row_locations.clone(),
            trace_shards,
        );
        let shard_index = trace_shard_index("trace-live-1", trace_shards);

        let mut accumulator = SegmentAccumulator::default();
        let mut intern_cache = BatchInternCache::default();
        let updates = accumulator.observe_prepared_block_rows(
            &prepared_rows,
            &prepared_shared_groups,
            &row_locations,
            &mut intern_cache,
        );

        assert_eq!(updates.len(), 1);
        let update = &updates[0];
        let expected = &expected_updates_by_shard[shard_index][0];
        assert_eq!(update.trace_id.as_ref(), expected.trace_id.as_ref());
        assert_eq!(update.start_unix_nano, expected.start_unix_nano);
        assert_eq!(update.end_unix_nano, expected.end_unix_nano);
        assert_eq!(
            update
                .row_locations
                .iter()
                .map(|location| {
                    (
                        location.segment_id,
                        location.row_index,
                        location.start_unix_nano,
                        location.end_unix_nano,
                    )
                })
                .collect::<Vec<_>>(),
            expected
                .row_locations
                .iter()
                .map(|location| {
                    (
                        location.segment_id,
                        location.row_index,
                        location.start_unix_nano,
                        location.end_unix_nano,
                    )
                })
                .collect::<Vec<_>>()
        );
        assert_eq!(
            update
                .services
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>(),
            expected
                .services
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            update
                .operations
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>(),
            expected
                .operations
                .iter()
                .map(|value| value.as_ref())
                .collect::<Vec<_>>()
        );
        assert_eq!(
            update
                .indexed_fields
                .iter()
                .map(|(name, value)| (name.as_ref(), value.as_ref()))
                .collect::<Vec<_>>(),
            expected
                .indexed_fields
                .iter()
                .map(|(name, value)| (name.as_ref(), value.as_ref()))
                .collect::<Vec<_>>()
        );
    }
}
