use thiserror::Error;
use vtcore::{
    LogRow, LogSearchRequest, TraceBlock, TraceSearchHit, TraceSearchRequest, TraceSpanRow,
    TraceWindow,
};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TraceBatchPayloadMode {
    #[default]
    Coalesce,
    Passthrough,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StorageStatsSnapshot {
    pub rows_ingested: u64,
    pub traces_tracked: u64,
    pub retained_trace_blocks: u64,
    pub persisted_bytes: u64,
    pub segment_count: u64,
    pub typed_field_columns: u64,
    pub string_field_columns: u64,
    pub trace_window_lookups: u64,
    pub row_queries: u64,
    pub segment_read_batches: u64,
    pub part_selective_decodes: u64,
    pub fsync_operations: u64,
    pub trace_batch_queue_depth: u64,
    pub trace_batch_max_queue_depth: u64,
    pub trace_batch_flushes: u64,
    pub trace_batch_rows: u64,
    pub trace_batch_input_blocks: u64,
    pub trace_batch_output_blocks: u64,
    pub trace_batch_wait_micros_total: u64,
    pub trace_batch_wait_micros_max: u64,
    pub trace_batch_flush_due_to_rows: u64,
    pub trace_batch_flush_due_to_blocks: u64,
    pub trace_batch_flush_due_to_wait: u64,
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("{0}")]
    Message(String),
}

pub trait StorageEngine: Send + Sync + 'static {
    fn append_trace_block(&self, block: TraceBlock) -> Result<(), StorageError> {
        self.append_rows(block.rows())
    }
    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        for block in blocks {
            self.append_trace_block(block)?;
        }
        Ok(())
    }
    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError>;
    fn append_logs(&self, rows: Vec<LogRow>) -> Result<(), StorageError>;
    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow>;
    fn list_trace_ids(&self) -> Vec<String>;
    fn list_services(&self) -> Vec<String>;
    fn list_field_names(&self) -> Vec<String>;
    fn list_field_values(&self, field_name: &str) -> Vec<String>;
    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit>;
    fn search_logs(&self, request: &LogSearchRequest) -> Vec<LogRow>;
    fn rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<TraceSpanRow>;
    fn stats(&self) -> StorageStatsSnapshot;
    fn preferred_trace_ingest_shards(&self) -> usize {
        1
    }
    fn trace_batch_payload_mode(&self) -> TraceBatchPayloadMode {
        TraceBatchPayloadMode::Coalesce
    }
}
