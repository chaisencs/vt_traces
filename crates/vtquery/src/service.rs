use std::sync::Arc;

use vtcore::{LogRow, LogSearchRequest, TraceSearchHit, TraceSearchRequest, TraceSpanRow};
use vtstorage::StorageEngine;

#[derive(Clone)]
pub struct QueryService {
    storage: Arc<dyn StorageEngine>,
}

impl QueryService {
    pub fn new(storage: Arc<dyn StorageEngine>) -> Self {
        Self { storage }
    }

    pub fn get_trace(&self, trace_id: &str) -> Vec<TraceSpanRow> {
        let Some(window) = self.storage.trace_window(trace_id) else {
            return Vec::new();
        };

        let mut rows =
            self.storage
                .rows_for_trace(trace_id, window.start_unix_nano, window.end_unix_nano);
        rows.sort_by_key(|row| row.end_unix_nano);
        rows
    }

    pub fn list_services(&self) -> Vec<String> {
        self.storage.list_services()
    }

    pub fn list_field_names(&self) -> Vec<String> {
        self.storage.list_field_names()
    }

    pub fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.storage.list_field_values(field_name)
    }

    pub fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.storage.search_traces(request)
    }

    pub fn search_logs(&self, request: &LogSearchRequest) -> Vec<LogRow> {
        let mut rows = self.storage.search_logs(request);
        rows.sort_by(|left, right| {
            right
                .time_unix_nano
                .cmp(&left.time_unix_nano)
                .then_with(|| left.log_id.cmp(&right.log_id))
        });
        rows.truncate(request.limit);
        rows
    }
}
