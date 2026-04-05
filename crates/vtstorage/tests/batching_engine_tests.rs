use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex,
    },
    thread,
    time::Duration,
};

use vtcore::{Field, LogRow, LogSearchRequest, TraceSearchHit, TraceSearchRequest, TraceSpanRow, TraceWindow};
use vtstorage::{
    BatchingStorageConfig, BatchingStorageEngine, MemoryStorageEngine, StorageEngine, StorageError,
    StorageStatsSnapshot,
};

fn make_row(trace_id: &str, span_id: &str, start: i64, end: i64) -> TraceSpanRow {
    TraceSpanRow::new(
        trace_id.to_string(),
        span_id.to_string(),
        None,
        format!("span-{span_id}"),
        start,
        end,
        vec![Field::new("resource_attr:service.name", "checkout")],
    )
    .expect("valid row")
}

#[derive(Debug, Default)]
struct CountingEngine {
    trace_appends: AtomicUsize,
    rows: Mutex<Vec<TraceSpanRow>>,
}

impl StorageEngine for CountingEngine {
    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        self.trace_appends.fetch_add(1, Ordering::Relaxed);
        self.rows.lock().unwrap().extend(rows);
        Ok(())
    }

    fn append_logs(&self, _rows: Vec<LogRow>) -> Result<(), StorageError> {
        Ok(())
    }

    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        let rows = self.rows.lock().unwrap();
        let mut matching = rows.iter().filter(|row| row.trace_id == trace_id);
        let first = matching.next()?;
        let mut window = TraceWindow::new(trace_id.to_string(), first.start_unix_nano, first.end_unix_nano);
        for row in matching {
            window.observe(row.start_unix_nano, row.end_unix_nano);
        }
        Some(window)
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
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        self.rows
            .lock()
            .unwrap()
            .iter()
            .filter(|row| {
                row.trace_id == trace_id
                    && row.end_unix_nano >= start_unix_nano
                    && row.start_unix_nano <= end_unix_nano
            })
            .cloned()
            .collect()
    }

    fn stats(&self) -> StorageStatsSnapshot {
        StorageStatsSnapshot::default()
    }
}

#[test]
fn batching_engine_coalesces_concurrent_trace_appends() {
    let inner = Arc::new(CountingEngine::default());
    let engine = Arc::new(BatchingStorageEngine::with_config(
        inner.clone(),
        BatchingStorageConfig::default()
            .with_max_batch_rows(128)
            .with_max_batch_wait(Duration::from_millis(2)),
    ));

    let workers = 16;
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::new();
    for worker in 0..workers {
        let engine = engine.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            engine
                .append_rows(vec![make_row("trace-batch", &format!("span-{worker}"), 100, 200)])
                .expect("append rows");
        }));
    }

    for handle in handles {
        handle.join().expect("join worker");
    }

    let window = engine.trace_window("trace-batch").expect("trace window");
    let rows = engine.rows_for_trace("trace-batch", window.start_unix_nano, window.end_unix_nano);

    assert_eq!(rows.len(), workers);
    assert!(inner.trace_appends.load(Ordering::Relaxed) < workers);
}

#[test]
fn batching_engine_preserves_memory_storage_visibility() {
    let inner: Arc<dyn StorageEngine> = Arc::new(MemoryStorageEngine::new());
    let engine = BatchingStorageEngine::with_config(
        inner,
        BatchingStorageConfig::default()
            .with_max_batch_rows(64)
            .with_max_batch_wait(Duration::from_millis(1)),
    );

    engine
        .append_rows(vec![
            make_row("trace-visible", "span-1", 100, 150),
            make_row("trace-visible", "span-2", 160, 210),
        ])
        .expect("append rows");

    let window = engine.trace_window("trace-visible").expect("trace window");
    let rows = engine.rows_for_trace("trace-visible", window.start_unix_nano, window.end_unix_nano);

    assert_eq!(rows.len(), 2);
}
