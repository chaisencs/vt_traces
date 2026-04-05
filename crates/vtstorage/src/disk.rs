use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{BufWriter, ErrorKind, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, OnceLock},
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use vtcore::{
    decode_log_row, decode_trace_row, encode_log_row, encode_trace_row, FieldFilter, LogRow,
    LogSearchRequest, TraceSearchHit, TraceSearchRequest, TraceSpanRow, TraceWindow,
};

use crate::bloom::StringBloomFilter;
use crate::log_state::LogIndexedState;
use crate::{StorageEngine, StorageError, StorageStatsSnapshot};

const SEGMENTS_DIRNAME: &str = "segments";
const SEGMENT_PREFIX: &str = "segment-";
const LEGACY_ROWS_SUFFIX: &str = ".rows";
const WAL_SUFFIX: &str = ".wal";
const PART_SUFFIX: &str = ".part";
const META_SUFFIX: &str = ".meta.json";
const WAL_MAGIC: &[u8] = b"VTWAL1";
const WAL_VERSION_JSON: u8 = 1;
const WAL_VERSION_BINARY: u8 = 2;
const LOG_WAL_MAGIC: &[u8] = b"VTLOGW1";
const LOG_WAL_VERSION_BINARY: u8 = 1;
const PART_MAGIC: &[u8] = b"VTPART1";
const PART_VERSION: u8 = 2;
const LENGTH_PREFIX_BYTES: u64 = 4;
const WAL_CHECKSUM_BYTES: u64 = 4;
const WAL_HEADER_BYTES: u64 = (WAL_MAGIC.len() + 1) as u64;
const LOG_WAL_HEADER_BYTES: u64 = (LOG_WAL_MAGIC.len() + 1) as u64;
const DEFAULT_TARGET_SEGMENT_SIZE_BYTES: u64 = 8 * 1024 * 1024;
const COMPACTION_TARGET_MULTIPLIER: u64 = 16;
const PART_FIELD_TYPE_STRING: u8 = 0;
const PART_FIELD_TYPE_I64: u8 = 1;
const PART_FIELD_TYPE_BOOL: u8 = 2;

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
}

impl Default for DiskStorageConfig {
    fn default() -> Self {
        Self {
            target_segment_size_bytes: DEFAULT_TARGET_SEGMENT_SIZE_BYTES,
            sync_policy: DiskSyncPolicy::None,
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
}

#[derive(Debug)]
pub struct DiskStorageEngine {
    root_path: PathBuf,
    segments_path: PathBuf,
    state: RwLock<DiskIndexState>,
    active_segment: Mutex<ActiveSegment>,
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
struct DiskIndexState {
    segment_paths: HashMap<u64, PathBuf>,
    windows_by_trace: HashMap<String, TraceWindow>,
    services_by_trace: HashMap<String, HashSet<String>>,
    trace_ids_by_service: HashMap<String, HashSet<String>>,
    operations_by_trace: HashMap<String, HashSet<String>>,
    operation_bloom_by_trace: HashMap<String, StringBloomFilter>,
    indexed_fields_by_trace: HashMap<String, HashMap<String, HashSet<String>>>,
    field_token_bloom_by_trace: HashMap<String, StringBloomFilter>,
    trace_ids_by_field_name_value: HashMap<String, HashMap<String, HashSet<String>>>,
    field_values_by_name: HashMap<String, HashSet<String>>,
    row_refs_by_trace: HashMap<String, Vec<SegmentRowLocation>>,
    rows_ingested: u64,
}

#[derive(Debug)]
struct ActiveSegment {
    segment_id: u64,
    wal_path: PathBuf,
    writer: BufWriter<File>,
    accumulator: SegmentAccumulator,
    cached_payload_ranges: Vec<std::ops::Range<usize>>,
    cached_payload_bytes: Vec<u8>,
}

#[derive(Debug, Default)]
struct SegmentAccumulator {
    row_count: u64,
    persisted_bytes: u64,
    min_time_unix_nano: Option<i64>,
    max_time_unix_nano: Option<i64>,
    traces: HashMap<String, SegmentTraceAccumulator>,
}

#[derive(Debug)]
struct SegmentTraceAccumulator {
    window: TraceWindow,
    services: BTreeSet<String>,
    operations: BTreeSet<String>,
    fields: HashMap<String, BTreeSet<String>>,
    rows: Vec<SegmentRowLocation>,
}

#[derive(Debug)]
struct LiveTraceUpdate {
    trace_id: String,
    start_unix_nano: i64,
    end_unix_nano: i64,
    services: HashSet<String>,
    operations: HashSet<String>,
    indexed_fields: HashMap<Arc<str>, HashSet<Arc<str>>>,
    row_locations: Vec<SegmentRowLocation>,
}

impl LiveTraceUpdate {
    fn new(trace_id: String, start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            trace_id,
            start_unix_nano,
            end_unix_nano,
            services: HashSet::new(),
            operations: HashSet::new(),
            indexed_fields: HashMap::new(),
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

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentRowLocation {
    segment_id: u64,
    row_index: u32,
    offset: u64,
    len: u32,
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

        let (mut state, mut persisted_bytes, recovered_next_segment_id) =
            recover_state(&segments_path)?;
        let log_path = root_path.join("logs.wal");
        let (log_state, log_persisted_bytes) = recover_log_state(&log_path)?;
        persisted_bytes += log_persisted_bytes;
        let mut next_segment_id = recovered_next_segment_id;
        if compact_persisted_segments(
            &segments_path,
            &mut next_segment_id,
            compaction_target_segment_size_bytes(&config),
        )? {
            let (compacted_state, compacted_bytes, compacted_next_segment_id) =
                recover_state(&segments_path)?;
            state = compacted_state;
            persisted_bytes = compacted_bytes;
            next_segment_id = next_segment_id.max(compacted_next_segment_id);
        }
        let active_segment = ActiveSegment::open(&segments_path, next_segment_id)?;
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
            state: RwLock::new(state),
            active_segment: Mutex::new(active_segment),
            log_state: RwLock::new(log_state),
            log_writer: Mutex::new(log_writer),
            next_segment_id: AtomicU64::new(next_segment_id + 1),
            config,
            persisted_bytes: AtomicU64::new(persisted_bytes),
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
        self.state
            .write()
            .replace_segment_path(active_segment.segment_id, part_path);
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
}

impl Drop for DiskStorageEngine {
    fn drop(&mut self) {
        let mut active_segment = self.active_segment.lock();
        let _ = self.finalize_active_segment(&mut active_segment);
        let _ = self.flush_log_writer();
    }
}

impl StorageEngine for DiskStorageEngine {
    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }

        let encoded_rows: Vec<Vec<u8>> = rows.iter().map(encode_trace_row).collect();
        let mut active_segment = self.active_segment.lock();
        let mut row_locations = Vec::with_capacity(rows.len());
        let mut observed_segment_paths = HashMap::new();

        let mut batch_start = 0usize;
        let mut pending_batch_bytes = 0u64;
        for (index, _) in rows.iter().enumerate() {
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
                    let batch_locations = active_segment
                        .append_rows(&rows[batch_start..index], &encoded_rows[batch_start..index])?;
                    for row_location in batch_locations {
                        self.persisted_bytes.fetch_add(
                            wal_record_storage_bytes(row_location.len),
                            Ordering::Relaxed,
                        );
                        row_locations.push(row_location);
                    }
                }
                self.rotate_active_segment(&mut active_segment)?;
                batch_start = index;
                pending_batch_bytes = 0;
            }
            pending_batch_bytes += wal_record_storage_bytes(encoded_rows[index].len() as u32);
        }
        if batch_start < rows.len() {
            observed_segment_paths
                .entry(active_segment.segment_id)
                .or_insert_with(|| active_segment.wal_path.clone());
            let batch_locations =
                active_segment.append_rows(&rows[batch_start..], &encoded_rows[batch_start..])?;
            for row_location in batch_locations {
                self.persisted_bytes.fetch_add(
                    wal_record_storage_bytes(row_location.len),
                    Ordering::Relaxed,
                );
                row_locations.push(row_location);
            }
        }

        let flushed = if self.config.sync_policy.requires_sync() {
            active_segment.flush(self.config.sync_policy)?
        } else {
            false
        };
        drop(active_segment);

        self.state
            .write()
            .observe_live_rows(&rows, observed_segment_paths, row_locations);

        if flushed {
            self.fsync_operations.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
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
        self.state.read().trace_window(trace_id)
    }

    fn list_trace_ids(&self) -> Vec<String> {
        self.state.read().list_trace_ids()
    }

    fn list_services(&self) -> Vec<String> {
        self.state.read().list_services()
    }

    fn list_field_names(&self) -> Vec<String> {
        self.state.read().list_field_names()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.state.read().list_field_values(field_name)
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.state.read().search_traces(request)
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

        let (row_refs, segment_paths) = {
            let state = self.state.read();
            (
                state.row_refs_for_trace(trace_id, start_unix_nano, end_unix_nano),
                state.segment_paths.clone(),
            )
        };

        let mut rows = Vec::with_capacity(row_refs.len());
        let mut refs_by_segment: HashMap<u64, Vec<SegmentRowLocation>> = HashMap::new();
        for row_ref in row_refs {
            refs_by_segment
                .entry(row_ref.segment_id)
                .or_default()
                .push(row_ref);
        }

        self.segment_read_batches
            .fetch_add(refs_by_segment.len() as u64, Ordering::Relaxed);

        for (segment_id, mut segment_refs) in refs_by_segment {
            let Some(path) = segment_paths.get(&segment_id) else {
                continue;
            };
            segment_refs.sort_by_key(|row| row.offset);
            {
                let active_segment = self.active_segment.lock();
                if active_segment.segment_id == segment_id {
                    if let Ok(mut segment_rows) = active_segment.read_rows(&segment_refs) {
                        rows.append(&mut segment_rows);
                    }
                    continue;
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
        let state = self.state.read();
        let (typed_field_columns, string_field_columns) =
            count_field_column_encodings(state.segment_paths.values());
        StorageStatsSnapshot {
            rows_ingested: state.rows_ingested(),
            traces_tracked: state.traces_tracked(),
            persisted_bytes: self.persisted_bytes.load(Ordering::Relaxed),
            segment_count: state.segment_count(),
            typed_field_columns,
            string_field_columns,
            trace_window_lookups: self.trace_window_lookups.load(Ordering::Relaxed),
            row_queries: self.row_queries.load(Ordering::Relaxed),
            segment_read_batches: self.segment_read_batches.load(Ordering::Relaxed),
            part_selective_decodes: self.part_selective_decodes.load(Ordering::Relaxed),
            fsync_operations: self.fsync_operations.load(Ordering::Relaxed),
        }
    }
}

impl DiskIndexState {
    fn observe_live_rows(
        &mut self,
        rows: &[TraceSpanRow],
        segment_paths: HashMap<u64, PathBuf>,
        row_locations: Vec<SegmentRowLocation>,
    ) {
        if rows.is_empty() {
            return;
        }

        for (segment_id, segment_path) in segment_paths {
            self.segment_paths.entry(segment_id).or_insert(segment_path);
        }

        let mut updates_by_trace: HashMap<String, LiveTraceUpdate> = HashMap::new();
        for (row, row_location) in rows.iter().zip(row_locations) {
            let update = updates_by_trace
                .entry(row.trace_id.clone())
                .or_insert_with(|| {
                    LiveTraceUpdate::new(
                        row.trace_id.clone(),
                        row.start_unix_nano,
                        row.end_unix_nano,
                    )
                });
            update.observe_window(row.start_unix_nano, row.end_unix_nano);
            if let Some(service_name) = row.service_name() {
                if !service_name.is_empty() && service_name != "-" {
                    update.services.insert(service_name.to_string());
                }
            }
            if !row.name.is_empty() {
                update.operations.insert(row.name.clone());
            }
            collect_live_indexed_fields(&mut update.indexed_fields, row);
            update.row_locations.push(row_location);
            self.rows_ingested += 1;
        }

        for (_, update) in updates_by_trace {
            self.observe_trace_metadata(
                update.trace_id.clone(),
                update.start_unix_nano,
                update.end_unix_nano,
            );
            self.merge_trace_services(&update.trace_id, update.services);
            self.merge_trace_operations(&update.trace_id, update.operations);
            self.merge_trace_fields(&update.trace_id, update.indexed_fields);
            let row_refs = self.row_refs_by_trace.entry(update.trace_id).or_default();
            merge_sorted_row_locations(row_refs, update.row_locations);
        }
    }

    fn observe_persisted_segment(&mut self, rows_path: PathBuf, meta: SegmentMeta) {
        self.segment_paths.insert(meta.segment_id, rows_path);

        for trace in meta.traces {
            self.observe_trace_metadata(
                trace.trace_id.clone(),
                trace.start_unix_nano,
                trace.end_unix_nano,
            );

            self.merge_trace_services(&trace.trace_id, trace.services.iter().cloned().collect());
            self.merge_trace_operations(
                &trace.trace_id,
                trace.operations.iter().cloned().collect(),
            );
            let mut indexed_fields = HashMap::new();
            for (field_name, values) in &trace.fields {
                indexed_fields.insert(
                    Arc::<str>::from(field_name.as_str()),
                    values
                        .iter()
                        .map(|value| Arc::<str>::from(value.as_str()))
                        .collect(),
                );
            }
            self.merge_trace_fields(&trace.trace_id, indexed_fields);

            let row_refs = self
                .row_refs_by_trace
                .entry(trace.trace_id.clone())
                .or_default();
            merge_sorted_row_locations(row_refs, trace.rows.clone());
            self.rows_ingested += trace.rows.len() as u64;
        }
    }

    fn observe_trace_metadata(
        &mut self,
        trace_id: String,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) {
        self.windows_by_trace
            .entry(trace_id.clone())
            .and_modify(|window| window.observe(start_unix_nano, end_unix_nano))
            .or_insert_with(|| TraceWindow::new(trace_id, start_unix_nano, end_unix_nano));
    }

    fn merge_trace_services(&mut self, trace_id: &str, services: HashSet<String>) {
        if services.is_empty() {
            return;
        }

        let trace_services = self.services_by_trace.entry(trace_id.to_string()).or_default();
        for service_name in services {
            if trace_services.contains(service_name.as_str()) {
                continue;
            }
            trace_services.insert(service_name.clone());
                self.trace_ids_by_service
                    .entry(service_name)
                    .or_default()
                    .insert(trace_id.to_string());
        }
    }

    fn merge_trace_operations(&mut self, trace_id: &str, operations: HashSet<String>) {
        if operations.is_empty() {
            return;
        }

        let trace_operations = self
            .operations_by_trace
            .entry(trace_id.to_string())
            .or_default();
        let bloom = self
            .operation_bloom_by_trace
            .entry(trace_id.to_string())
            .or_default();
        for operation_name in operations {
            if trace_operations.contains(operation_name.as_str()) {
                continue;
            }
            trace_operations.insert(operation_name.clone());
            bloom.insert(&operation_name);
        }
    }

    fn replace_segment_path(&mut self, segment_id: u64, segment_path: PathBuf) {
        self.segment_paths.insert(segment_id, segment_path);
    }

    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        self.windows_by_trace.get(trace_id).cloned()
    }

    fn list_services(&self) -> Vec<String> {
        let mut services: Vec<String> = self.trace_ids_by_service.keys().cloned().collect();
        services.sort();
        services
    }

    fn list_field_names(&self) -> Vec<String> {
        let mut field_names: Vec<String> = self.field_values_by_name.keys().cloned().collect();
        field_names.sort();
        field_names
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        let mut values: Vec<String> = self
            .field_values_by_name
            .get(field_name)
            .map(|values| values.iter().cloned().collect())
            .unwrap_or_default();
        values.sort();
        values
    }

    fn list_trace_ids(&self) -> Vec<String> {
        let mut trace_ids: Vec<String> = self.windows_by_trace.keys().cloned().collect();
        trace_ids.sort();
        trace_ids
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        let candidate_trace_ids = self.candidate_trace_ids(request);

        let mut hits: Vec<TraceSearchHit> = candidate_trace_ids
            .into_iter()
            .filter_map(|trace_id| {
                let window = self.windows_by_trace.get(trace_id)?;
                let overlaps = window.end_unix_nano >= request.start_unix_nano
                    && window.start_unix_nano <= request.end_unix_nano;
                if !overlaps {
                    return None;
                }

                if let Some(operation_name) = &request.operation_name {
                    let bloom = self.operation_bloom_by_trace.get(trace_id)?;
                    if !bloom.might_contain(operation_name) {
                        return None;
                    }
                    let operations = self.operations_by_trace.get(trace_id)?;
                    if !operations.contains(operation_name) {
                        return None;
                    }
                }

                for field_filter in &request.field_filters {
                    if !self.trace_matches_field_filter(trace_id, field_filter) {
                        return None;
                    }
                }

                let services = self
                    .services_by_trace
                    .get(trace_id)
                    .map(|values| {
                        let mut values: Vec<String> = values.iter().cloned().collect();
                        values.sort();
                        values
                    })
                    .unwrap_or_default();

                Some(TraceSearchHit {
                    trace_id: trace_id.clone(),
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
        self.row_refs_by_trace
            .get(trace_id)
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

    fn segment_count(&self) -> u64 {
        self.segment_paths.len() as u64
    }

    fn merge_trace_fields(
        &mut self,
        trace_id: &str,
        indexed_fields: HashMap<Arc<str>, HashSet<Arc<str>>>,
    ) {
        if indexed_fields.is_empty() {
            return;
        }

        let trace_fields = self
            .indexed_fields_by_trace
            .entry(trace_id.to_string())
            .or_default();
        let bloom = self
            .field_token_bloom_by_trace
            .entry(trace_id.to_string())
            .or_default();
        for (field_name, values) in indexed_fields {
            let field_name_ref = field_name.as_ref();
            let trace_values = match trace_fields.get_mut(field_name_ref) {
                Some(trace_values) => trace_values,
                None => trace_fields.entry(field_name_ref.to_string()).or_default(),
            };
            for field_value in values {
                let field_value_ref = field_value.as_ref();
                if trace_values.contains(field_value_ref) {
                    continue;
                }
                trace_values.insert(field_value_ref.to_string());
                bloom.insert(&field_bloom_token(field_name_ref, field_value_ref));
                self.trace_ids_by_field_name_value
                    .entry(field_name_ref.to_string())
                    .or_default()
                    .entry(field_value_ref.to_string())
                    .or_default()
                    .insert(trace_id.to_string());
                self.field_values_by_name
                    .entry(field_name_ref.to_string())
                    .or_default()
                    .insert(field_value_ref.to_string());
            }
        }
    }

    fn candidate_trace_ids(&self, request: &TraceSearchRequest) -> Vec<&String> {
        let mut candidate_ids: Option<HashSet<String>> =
            request.service_name.as_ref().map(|service_name| {
                self.trace_ids_by_service
                    .get(service_name)
                    .cloned()
                    .unwrap_or_default()
            });

        for field_filter in &request.field_filters {
            let matching = self
                .trace_ids_by_field_name_value
                .get(&field_filter.name)
                .and_then(|values| values.get(&field_filter.value))
                .cloned()
                .unwrap_or_default();
            candidate_ids = Some(match candidate_ids {
                Some(current) => current.intersection(&matching).cloned().collect(),
                None => matching,
            });
        }

        match candidate_ids {
            Some(candidate_ids) => candidate_ids
                .iter()
                .filter_map(|trace_id| {
                    self.windows_by_trace
                        .get_key_value(trace_id)
                        .map(|(trace_id, _)| trace_id)
                })
                .collect(),
            None => self.windows_by_trace.keys().collect(),
        }
    }

    fn trace_matches_field_filter(&self, trace_id: &str, field_filter: &FieldFilter) -> bool {
        let bloom = match self.field_token_bloom_by_trace.get(trace_id) {
            Some(bloom) => bloom,
            None => return false,
        };
        if !bloom.might_contain(&field_bloom_token(&field_filter.name, &field_filter.value)) {
            return false;
        }
        self.indexed_fields_by_trace
            .get(trace_id)
            .and_then(|fields| fields.get(&field_filter.name))
            .map(|values| values.contains(&field_filter.value))
            .unwrap_or(false)
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
            writer: BufWriter::new(file),
            accumulator: SegmentAccumulator::default(),
            cached_payload_ranges: Vec::new(),
            cached_payload_bytes: Vec::new(),
        })
    }

    fn append_rows(
        &mut self,
        rows: &[TraceSpanRow],
        encoded_rows: &[Vec<u8>],
    ) -> Result<Vec<SegmentRowLocation>, StorageError> {
        if rows.is_empty() {
            return Ok(Vec::new());
        }
        if rows.len() != encoded_rows.len() {
            return Err(StorageError::Message(
                "row and payload batch length mismatch".to_string(),
            ));
        }

        let mut buffered_bytes = Vec::new();
        if self.accumulator.row_count == 0 && self.accumulator.persisted_bytes == 0 {
            buffered_bytes.extend_from_slice(WAL_MAGIC);
            buffered_bytes.push(WAL_VERSION_BINARY);
            self.accumulator.persisted_bytes = WAL_HEADER_BYTES;
        }

        let mut row_locations = Vec::with_capacity(rows.len());
        for (row, payload) in rows.iter().zip(encoded_rows.iter()) {
            let checksum = crc32fast::hash(&payload);
            let payload_start = self.cached_payload_bytes.len();
            self.cached_payload_bytes.extend_from_slice(payload);
            self.cached_payload_ranges
                .push(payload_start..self.cached_payload_bytes.len());
            let row_location = SegmentRowLocation {
                segment_id: self.segment_id,
                row_index: self.accumulator.row_count as u32,
                offset: self.accumulator.persisted_bytes,
                len: payload.len() as u32,
                start_unix_nano: row.start_unix_nano,
                end_unix_nano: row.end_unix_nano,
            };

            buffered_bytes.extend_from_slice(&row_location.len.to_le_bytes());
            buffered_bytes.extend_from_slice(&checksum.to_le_bytes());
            buffered_bytes.extend_from_slice(&payload);
            self.accumulator.observe_row(row, row_location.clone());
            row_locations.push(row_location);
        }

        self.writer.write_all(&buffered_bytes)?;
        Ok(row_locations)
    }

    fn read_rows(&self, row_refs: &[SegmentRowLocation]) -> Result<Vec<TraceSpanRow>, StorageError> {
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
    fn observe_row(&mut self, row: &TraceSpanRow, row_location: SegmentRowLocation) {
        self.row_count += 1;
        self.persisted_bytes += wal_record_storage_bytes(row_location.len);
        self.min_time_unix_nano = Some(
            self.min_time_unix_nano
                .map(|value| value.min(row.start_unix_nano))
                .unwrap_or(row.start_unix_nano),
        );
        self.max_time_unix_nano = Some(
            self.max_time_unix_nano
                .map(|value| value.max(row.end_unix_nano))
                .unwrap_or(row.end_unix_nano),
        );

        let trace =
            self.traces
                .entry(row.trace_id.clone())
                .or_insert_with(|| SegmentTraceAccumulator {
                    window: TraceWindow::new(
                        row.trace_id.clone(),
                        row.start_unix_nano,
                        row.end_unix_nano,
                    ),
                    services: BTreeSet::new(),
                    operations: BTreeSet::new(),
                    fields: HashMap::new(),
                    rows: Vec::new(),
                });
        trace.window.observe(row.start_unix_nano, row.end_unix_nano);
        if let Some(service_name) = row.service_name() {
            if !service_name.is_empty() && service_name != "-" {
                trace.services.insert(service_name.to_string());
            }
        }
        if !row.name.is_empty() {
            trace.operations.insert(row.name.clone());
        }
        observe_segment_trace_field(&mut trace.fields, "name", &row.name);
        observe_segment_trace_field(
            &mut trace.fields,
            "duration",
            &row.duration_nanos().to_string(),
        );
        for field in &row.fields {
            if should_index_field(&field.name) {
                observe_segment_trace_field(&mut trace.fields, &field.name, &field.value);
            }
        }
        trace.rows.push(row_location);
        trace.rows.sort_by_key(|location| location.end_unix_nano);
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
            .map(|(trace_id, trace)| {
                services.extend(trace.services.iter().cloned());
                SegmentTraceMeta {
                    trace_id: trace_id.clone(),
                    start_unix_nano: trace.window.start_unix_nano,
                    end_unix_nano: trace.window.end_unix_nano,
                    services: trace.services.iter().cloned().collect(),
                    operations: trace.operations.iter().cloned().collect(),
                    fields: trace
                        .fields
                        .iter()
                        .map(|(name, values)| {
                            (name.clone(), values.iter().cloned().collect::<Vec<_>>())
                        })
                        .collect(),
                    rows: trace.rows.clone(),
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

fn recover_state(segments_path: &Path) -> Result<(DiskIndexState, u64, u64), StorageError> {
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

    let mut state = DiskIndexState::default();
    let mut persisted_bytes = 0u64;
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

        persisted_bytes += fs::metadata(&part_path)?.len();
        persisted_bytes += fs::metadata(&meta_path)?.len();
        state.observe_persisted_segment(part_path, meta);
    }

    Ok((state, persisted_bytes, next_segment_id))
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

fn field_bloom_token(field_name: &str, field_value: &str) -> String {
    format!("{field_name}\u{1f}{field_value}")
}

fn name_field_name() -> Arc<str> {
    static NAME: OnceLock<Arc<str>> = OnceLock::new();
    NAME.get_or_init(|| Arc::<str>::from("name")).clone()
}

fn duration_field_name() -> Arc<str> {
    static DURATION: OnceLock<Arc<str>> = OnceLock::new();
    DURATION
        .get_or_init(|| Arc::<str>::from("duration"))
        .clone()
}

fn collect_live_indexed_fields(
    indexed_fields: &mut HashMap<Arc<str>, HashSet<Arc<str>>>,
    row: &TraceSpanRow,
) {
    observe_live_indexed_field(indexed_fields, name_field_name(), Arc::<str>::from(row.name.as_str()));
    observe_live_indexed_field(
        indexed_fields,
        duration_field_name(),
        Arc::<str>::from(row.duration_nanos().to_string()),
    );
    for field in &row.fields {
        if should_index_field(&field.name) {
            observe_live_indexed_field(indexed_fields, field.name.clone(), field.value.clone());
        }
    }
}

fn observe_live_indexed_field(
    indexed_fields: &mut HashMap<Arc<str>, HashSet<Arc<str>>>,
    field_name: Arc<str>,
    field_value: Arc<str>,
) {
    if field_name.is_empty() || field_value.is_empty() {
        return;
    }
    indexed_fields
        .entry(field_name)
        .or_default()
        .insert(field_value);
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
        .map(|(current_last, incoming_first)| current_last.end_unix_nano <= incoming_first.end_unix_nano)
        .unwrap_or(false);
    if append_only {
        existing.reserve(incoming.len());
        existing.extend(incoming);
        return;
    }

    let prepend_only = existing
        .first()
        .zip(incoming.last())
        .map(|(current_first, incoming_last)| incoming_last.end_unix_nano <= current_first.end_unix_nano)
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

fn observe_segment_trace_field(
    fields: &mut HashMap<String, BTreeSet<String>>,
    field_name: &str,
    field_value: &str,
) {
    if field_name.is_empty() || field_value.is_empty() {
        return;
    }
    fields
        .entry(field_name.to_string())
        .or_default()
        .insert(field_value.to_string());
}

fn rebuild_segment_meta(segment_id: u64, segment_path: &Path) -> Result<SegmentMeta, StorageError> {
    let rows = if segment_path.extension().and_then(|value| value.to_str()) == Some("part") {
        read_all_rows_from_part(segment_path)?
    } else {
        read_all_rows_from_row_file(segment_path, segment_id)?
    };

    let mut accumulator = SegmentAccumulator::default();
    for (row_index, (row, offset, len)) in rows.into_iter().enumerate() {
        accumulator.observe_row(
            &row,
            SegmentRowLocation {
                segment_id,
                row_index: row_index as u32,
                offset,
                len,
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

    let mut file = File::open(path)?;
    let (format, _) = detect_row_file_format(&mut file)?;

    let mut rows = Vec::with_capacity(row_refs.len());
    for range in wal_read_ranges(format, row_refs) {
        file.seek(SeekFrom::Start(range.start_offset))?;
        let mut buffer = vec![0u8; (range.end_offset - range.start_offset) as usize];
        file.read_exact(&mut buffer)?;

        for row_ref in range.row_refs {
            let local_offset = (row_ref.offset - range.start_offset) as usize;
            let payload = decode_row_payload_from_range(format, &buffer, local_offset)?;
            rows.push(payload);
        }
    }
    Ok(rows)
}

#[derive(Debug)]
struct WALReadRange {
    start_offset: u64,
    end_offset: u64,
    row_refs: Vec<SegmentRowLocation>,
}

fn wal_read_ranges(format: RowFileFormat, row_refs: &[SegmentRowLocation]) -> Vec<WALReadRange> {
    let mut ranges = Vec::new();
    let mut current_range: Option<WALReadRange> = None;

    for row_ref in row_refs.iter().cloned() {
        let record_end = row_ref.offset + wal_record_storage_bytes_for_format(format, row_ref.len);
        match current_range.as_mut() {
            Some(range) if range.end_offset == row_ref.offset => {
                range.end_offset = record_end;
                range.row_refs.push(row_ref);
            }
            Some(range) => {
                ranges.push(WALReadRange {
                    start_offset: range.start_offset,
                    end_offset: range.end_offset,
                    row_refs: std::mem::take(&mut range.row_refs),
                });
                *range = WALReadRange {
                    start_offset: row_ref.offset,
                    end_offset: record_end,
                    row_refs: vec![row_ref],
                };
            }
            None => {
                current_range = Some(WALReadRange {
                    start_offset: row_ref.offset,
                    end_offset: record_end,
                    row_refs: vec![row_ref],
                });
            }
        }
    }

    if let Some(range) = current_range {
        ranges.push(range);
    }

    ranges
}

fn decode_row_payload_from_range(
    format: RowFileFormat,
    range_bytes: &[u8],
    offset: usize,
) -> Result<TraceSpanRow, StorageError> {
    let len_end = offset + LENGTH_PREFIX_BYTES as usize;
    let len = u32::from_le_bytes(
        range_bytes[offset..len_end]
            .try_into()
            .map_err(|_| invalid_data("wal row length prefix truncated"))?,
    );
    let mut payload_offset = len_end;
    let expected_checksum = if format.uses_checksum() {
        let checksum_end = payload_offset + WAL_CHECKSUM_BYTES as usize;
        let checksum = u32::from_le_bytes(
            range_bytes[payload_offset..checksum_end]
                .try_into()
                .map_err(|_| invalid_data("wal row checksum truncated"))?,
        );
        payload_offset = checksum_end;
        Some(checksum)
    } else {
        None
    };
    let payload_end = payload_offset + len as usize;
    let payload = range_bytes
        .get(payload_offset..payload_end)
        .ok_or_else(|| invalid_data("wal row payload truncated"))?;
    if let Some(expected_checksum) = expected_checksum {
        if crc32fast::hash(payload) != expected_checksum {
            return Err(invalid_data("wal row checksum mismatch").into());
        }
    }
    decode_row_payload(format, payload)
}

fn decode_row_payload(format: RowFileFormat, payload: &[u8]) -> Result<TraceSpanRow, StorageError> {
    match format {
        RowFileFormat::Legacy | RowFileFormat::ChecksummedJson => {
            Ok(serde_json::from_slice(payload)?)
        }
        RowFileFormat::ChecksummedBinary => decode_trace_row(payload)
            .map_err(|error| StorageError::Io(invalid_data(error.to_string()))),
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
                segment_id,
                row_index: row_index as u32,
                offset: row_index as u64,
                len: 0,
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
    let checksum_bytes = if format.uses_checksum() {
        WAL_CHECKSUM_BYTES
    } else {
        0
    };
    LENGTH_PREFIX_BYTES + checksum_bytes + payload_len as u64
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
    use super::{merge_sorted_row_locations, SegmentRowLocation};

    fn row(segment_id: u64, row_index: u32, start: i64, end: i64) -> SegmentRowLocation {
        SegmentRowLocation {
            segment_id,
            row_index,
            offset: u64::from(row_index),
            len: 1,
            start_unix_nano: start,
            end_unix_nano: end,
        }
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
}
