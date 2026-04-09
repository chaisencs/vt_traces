use serde::{Deserialize, Serialize};
use thiserror::Error;
use vtcore::{
    LogRow, LogSearchRequest, TraceBlock, TraceSearchHit, TraceSearchRequest, TraceSpanRow,
    TraceWindow,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRowsRequest {
    pub trace_id: String,
    pub start_unix_nano: i64,
    pub end_unix_nano: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceRowsResponse {
    pub trace_id: String,
    pub rows: Vec<TraceSpanRow>,
}

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
    pub persisted_segment_count: u64,
    pub segment_count: u64,
    pub trace_head_segments: u64,
    pub trace_head_rows: u64,
    pub typed_field_columns: u64,
    pub string_field_columns: u64,
    pub trace_window_lookups: u64,
    pub row_queries: u64,
    pub segment_read_batches: u64,
    pub part_selective_decodes: u64,
    pub fsync_operations: u64,
    pub trace_group_commit_flushes: u64,
    pub trace_group_commit_rows: u64,
    pub trace_group_commit_bytes: u64,
    pub trace_batch_queue_depth: u64,
    pub trace_batch_max_queue_depth: u64,
    pub trace_batch_flushes: u64,
    pub trace_batch_rows: u64,
    pub trace_batch_input_blocks: u64,
    pub trace_batch_output_blocks: u64,
    pub trace_batch_wait_micros_total: u64,
    pub trace_batch_wait_micros_max: u64,
    pub trace_live_update_queue_depth: u64,
    pub trace_seal_queue_depth: u64,
    pub trace_seal_completed: u64,
    pub trace_seal_rows: u64,
    pub trace_seal_bytes: u64,
    pub trace_retention_deleted_segments: u64,
    pub trace_retention_deleted_bytes: u64,
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
    fn list_operations(
        &self,
        service_name: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
        limit: usize,
    ) -> Vec<String> {
        let hits = self.search_traces(&TraceSearchRequest {
            start_unix_nano,
            end_unix_nano,
            service_name: Some(service_name.to_string()),
            operation_name: None,
            field_filters: Vec::new(),
            limit: usize::MAX,
        });

        let mut operations = std::collections::BTreeSet::new();
        for hit in hits {
            for row in self.rows_for_trace(&hit.trace_id, hit.start_unix_nano, hit.end_unix_nano) {
                if row.service_name() == Some(service_name) {
                    operations.insert(row.name);
                    if operations.len() >= limit {
                        return operations.into_iter().collect();
                    }
                }
            }
        }
        operations.into_iter().collect()
    }
    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit>;
    fn search_logs(&self, request: &LogSearchRequest) -> Vec<LogRow>;
    fn rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<TraceSpanRow>;
    fn rows_for_traces(&self, requests: &[TraceRowsRequest]) -> Vec<TraceRowsResponse> {
        requests
            .iter()
            .map(|request| TraceRowsResponse {
                trace_id: request.trace_id.clone(),
                rows: self.rows_for_trace(
                    &request.trace_id,
                    request.start_unix_nano,
                    request.end_unix_nano,
                ),
            })
            .collect()
    }
    fn stats(&self) -> StorageStatsSnapshot;
    fn preferred_trace_ingest_shards(&self) -> usize {
        1
    }
    fn trace_batch_payload_mode(&self) -> TraceBatchPayloadMode {
        TraceBatchPayloadMode::Coalesce
    }
}
