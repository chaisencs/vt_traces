use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use vtcore::{
    LogRow, LogSearchRequest, TraceSearchHit, TraceSearchRequest, TraceSpanRow, TraceWindow,
};

use crate::{
    log_state::LogIndexedState, state::IndexedState, StorageEngine, StorageError,
    StorageStatsSnapshot,
};

#[derive(Debug, Default)]
pub struct MemoryStorageEngine {
    trace_state: RwLock<IndexedState>,
    log_state: RwLock<LogIndexedState>,
    trace_window_lookups: AtomicU64,
    row_queries: AtomicU64,
}

impl MemoryStorageEngine {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn trace_window_lookups(&self) -> u64 {
        self.trace_window_lookups.load(Ordering::Relaxed)
    }

    pub fn row_queries(&self) -> u64 {
        self.row_queries.load(Ordering::Relaxed)
    }
}

impl StorageEngine for MemoryStorageEngine {
    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        self.trace_state.write().ingest_rows(rows);
        Ok(())
    }

    fn append_logs(&self, rows: Vec<LogRow>) -> Result<(), StorageError> {
        self.log_state.write().ingest_rows(rows);
        Ok(())
    }

    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        self.trace_window_lookups.fetch_add(1, Ordering::Relaxed);
        self.trace_state.read().trace_window(trace_id)
    }

    fn list_trace_ids(&self) -> Vec<String> {
        self.trace_state.read().list_trace_ids()
    }

    fn list_services(&self) -> Vec<String> {
        self.trace_state.read().list_services()
    }

    fn list_field_names(&self) -> Vec<String> {
        self.trace_state.read().list_field_names()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.trace_state.read().list_field_values(field_name)
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.trace_state.read().search_traces(request)
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
        self.trace_state
            .read()
            .rows_for_trace(trace_id, start_unix_nano, end_unix_nano)
    }

    fn stats(&self) -> StorageStatsSnapshot {
        let state = self.trace_state.read();
        StorageStatsSnapshot {
            rows_ingested: state.rows_ingested(),
            traces_tracked: state.traces_tracked(),
            persisted_bytes: 0,
            segment_count: 0,
            typed_field_columns: 0,
            string_field_columns: 0,
            trace_window_lookups: self.trace_window_lookups.load(Ordering::Relaxed),
            row_queries: self.row_queries.load(Ordering::Relaxed),
            segment_read_batches: 0,
            part_selective_decodes: 0,
            fsync_operations: 0,
        }
    }
}
