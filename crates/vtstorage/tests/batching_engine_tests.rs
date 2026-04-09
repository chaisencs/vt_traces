use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Barrier, Mutex,
    },
    thread,
    time::Duration,
};
use vtcore::{
    Field, LogRow, LogSearchRequest, TraceBlock, TraceSearchHit, TraceSearchRequest, TraceSpanRow,
    TraceWindow,
};
use vtstorage::{
    BatchingStorageConfig, BatchingStorageEngine, MemoryStorageEngine, StorageEngine, StorageError,
    StorageStatsSnapshot, TraceBatchPayloadMode, TraceRowsRequest, TraceRowsResponse,
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
    input_blocks: AtomicUsize,
    inflight_appends: AtomicUsize,
    max_inflight_appends: AtomicUsize,
    rows: Mutex<Vec<TraceSpanRow>>,
}

impl StorageEngine for CountingEngine {
    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        let inflight = self.inflight_appends.fetch_add(1, Ordering::SeqCst) + 1;
        let _ =
            self.max_inflight_appends
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                    (inflight > current).then_some(inflight)
                });
        thread::sleep(Duration::from_millis(10));
        self.trace_appends.fetch_add(1, Ordering::Relaxed);
        self.input_blocks.fetch_add(blocks.len(), Ordering::Relaxed);
        let mut rows = self.rows.lock().unwrap();
        for block in blocks {
            rows.extend(block.rows());
        }
        self.inflight_appends.fetch_sub(1, Ordering::SeqCst);
        Ok(())
    }

    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
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
        let mut window = TraceWindow::new(
            trace_id.to_string(),
            first.start_unix_nano,
            first.end_unix_nano,
        );
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

#[derive(Debug, Default)]
struct DirectTraceBlockEngine {
    append_trace_blocks_calls: AtomicUsize,
    appended_block_count: AtomicUsize,
    appended_row_count: AtomicUsize,
}

impl StorageEngine for DirectTraceBlockEngine {
    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        self.append_trace_blocks_calls
            .fetch_add(1, Ordering::Relaxed);
        self.appended_block_count
            .fetch_add(blocks.len(), Ordering::Relaxed);
        let rows = blocks.iter().map(TraceBlock::row_count).sum::<usize>();
        self.appended_row_count.fetch_add(rows, Ordering::Relaxed);
        Ok(())
    }

    fn append_rows(&self, _rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        panic!("trace fast path should bypass append_rows");
    }

    fn append_logs(&self, _rows: Vec<LogRow>) -> Result<(), StorageError> {
        Ok(())
    }

    fn trace_window(&self, _trace_id: &str) -> Option<TraceWindow> {
        None
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
        _trace_id: &str,
        _start_unix_nano: i64,
        _end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        Vec::new()
    }

    fn stats(&self) -> StorageStatsSnapshot {
        StorageStatsSnapshot::default()
    }
}

#[derive(Debug, Default)]
struct PassthroughTraceBlockEngine {
    append_trace_blocks_calls: AtomicUsize,
    appended_block_count: AtomicUsize,
    appended_row_count: AtomicUsize,
}

impl StorageEngine for PassthroughTraceBlockEngine {
    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        self.append_trace_blocks_calls
            .fetch_add(1, Ordering::Relaxed);
        self.appended_block_count
            .fetch_add(blocks.len(), Ordering::Relaxed);
        let rows = blocks.iter().map(TraceBlock::row_count).sum::<usize>();
        self.appended_row_count.fetch_add(rows, Ordering::Relaxed);
        Ok(())
    }

    fn append_rows(&self, _rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        panic!("trace fast path should bypass append_rows");
    }

    fn append_logs(&self, _rows: Vec<LogRow>) -> Result<(), StorageError> {
        Ok(())
    }

    fn trace_window(&self, _trace_id: &str) -> Option<TraceWindow> {
        None
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
        _trace_id: &str,
        _start_unix_nano: i64,
        _end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        Vec::new()
    }

    fn stats(&self) -> StorageStatsSnapshot {
        StorageStatsSnapshot::default()
    }

    fn trace_batch_payload_mode(&self) -> TraceBatchPayloadMode {
        TraceBatchPayloadMode::Passthrough
    }
}

#[derive(Debug, Default)]
struct QueryForwardingEngine {
    list_operations_calls: AtomicUsize,
    search_traces_calls: AtomicUsize,
    rows_for_trace_calls: AtomicUsize,
    rows_for_traces_calls: AtomicUsize,
}

impl StorageEngine for QueryForwardingEngine {
    fn append_rows(&self, _rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        Ok(())
    }

    fn append_logs(&self, _rows: Vec<LogRow>) -> Result<(), StorageError> {
        Ok(())
    }

    fn trace_window(&self, _trace_id: &str) -> Option<TraceWindow> {
        None
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

    fn list_operations(
        &self,
        _service_name: &str,
        _start_unix_nano: i64,
        _end_unix_nano: i64,
        _limit: usize,
    ) -> Vec<String> {
        self.list_operations_calls.fetch_add(1, Ordering::Relaxed);
        vec!["GET /checkout".to_string()]
    }

    fn search_traces(&self, _request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.search_traces_calls.fetch_add(1, Ordering::Relaxed);
        Vec::new()
    }

    fn search_logs(&self, _request: &LogSearchRequest) -> Vec<LogRow> {
        Vec::new()
    }

    fn rows_for_trace(
        &self,
        _trace_id: &str,
        _start_unix_nano: i64,
        _end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        self.rows_for_trace_calls.fetch_add(1, Ordering::Relaxed);
        Vec::new()
    }

    fn rows_for_traces(&self, requests: &[TraceRowsRequest]) -> Vec<TraceRowsResponse> {
        self.rows_for_traces_calls.fetch_add(1, Ordering::Relaxed);
        requests
            .iter()
            .map(|request| TraceRowsResponse {
                trace_id: request.trace_id.clone(),
                rows: vec![make_row(&request.trace_id, "span-forwarded", 100, 150)],
            })
            .collect()
    }

    fn stats(&self) -> StorageStatsSnapshot {
        StorageStatsSnapshot::default()
    }
}

#[test]
fn batching_engine_trace_fast_path_preserves_all_concurrent_writes() {
    let inner = Arc::new(CountingEngine::default());
    let engine = Arc::new(BatchingStorageEngine::with_config(
        inner.clone(),
        BatchingStorageConfig::default()
            .with_trace_shards(1)
            .with_max_trace_batch_blocks(64)
            .with_max_batch_rows(128)
            .with_max_batch_wait(Duration::from_millis(5)),
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
                .append_rows(vec![make_row(
                    "trace-batch",
                    &format!("span-{worker}"),
                    100,
                    200,
                )])
                .expect("append rows");
        }));
    }

    for handle in handles {
        handle.join().expect("join worker");
    }

    let window = engine.trace_window("trace-batch").expect("trace window");
    let rows = engine.rows_for_trace("trace-batch", window.start_unix_nano, window.end_unix_nano);

    assert_eq!(rows.len(), workers);
    assert!(
        inner.trace_appends.load(Ordering::Relaxed) < workers,
        "trace writes should be batched before reaching the inner engine",
    );
    let stats = engine.stats();
    assert!(stats.trace_batch_flushes > 0);
    assert!(stats.trace_batch_input_blocks >= workers as u64);
    assert!(stats.trace_batch_output_blocks < stats.trace_batch_input_blocks);
}

#[test]
fn batching_engine_wait_window_coalesces_staggered_requests() {
    let inner = Arc::new(CountingEngine::default());
    let start_barrier = Arc::new(Barrier::new(2));
    let engine = Arc::new(BatchingStorageEngine::with_config(
        inner.clone(),
        BatchingStorageConfig::default()
            .with_trace_shards(1)
            .with_max_trace_batch_blocks(64)
            .with_max_batch_rows(128)
            .with_max_batch_wait(Duration::from_secs(1)),
    ));

    let first = {
        let engine = engine.clone();
        let start_barrier = start_barrier.clone();
        thread::spawn(move || {
            start_barrier.wait();
            engine
                .append_rows(vec![make_row("trace-wait", "span-1", 100, 150)])
                .expect("append first rows");
        })
    };

    start_barrier.wait();
    thread::sleep(Duration::from_millis(20));
    engine
        .append_rows(vec![make_row("trace-wait", "span-2", 160, 210)])
        .expect("append second rows");
    first.join().expect("join first append");

    let window = engine.trace_window("trace-wait").expect("trace window");
    let rows = engine.rows_for_trace("trace-wait", window.start_unix_nano, window.end_unix_nano);

    assert_eq!(rows.len(), 2);
    assert_eq!(
        inner.trace_appends.load(Ordering::Relaxed),
        1,
        "requests arriving within the batch wait window should share a single inner append",
    );
    let stats = engine.stats();
    assert_eq!(stats.trace_batch_flushes, 1);
    assert_eq!(stats.trace_batch_input_blocks, 2);
    assert_eq!(stats.trace_batch_output_blocks, 1);
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
    let rows = engine.rows_for_trace(
        "trace-visible",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 2);
    let stats = engine.stats();
    assert!(stats.trace_batch_flushes > 0);
    assert_eq!(stats.trace_batch_rows, 2);
}

#[test]
fn batching_engine_parallelizes_distinct_trace_writes_across_shards() {
    let inner = Arc::new(CountingEngine::default());
    let engine = Arc::new(BatchingStorageEngine::with_config(
        inner.clone(),
        BatchingStorageConfig::default()
            .with_trace_shards(4)
            .with_max_trace_batch_blocks(64)
            .with_max_batch_rows(64)
            .with_max_batch_wait(Duration::from_millis(5)),
    ));

    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::new();
    for worker in 0..workers {
        let engine = engine.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            engine
                .append_rows(vec![make_row(
                    &format!("trace-parallel-{worker}"),
                    &format!("span-{worker}"),
                    100,
                    200,
                )])
                .expect("append rows");
        }));
    }

    for handle in handles {
        handle.join().expect("join worker");
    }

    assert!(
        inner.max_inflight_appends.load(Ordering::SeqCst) > 1,
        "distinct traces should hit multiple batch workers",
    );
}

#[test]
fn batching_engine_trace_blocks_use_direct_inner_append_path() {
    let inner = Arc::new(DirectTraceBlockEngine::default());
    let engine = BatchingStorageEngine::with_config(
        inner.clone(),
        BatchingStorageConfig::default()
            .with_trace_shards(1)
            .with_max_batch_rows(64)
            .with_max_batch_wait(Duration::from_millis(1)),
    );

    engine
        .append_rows(vec![
            make_row("trace-direct", "span-1", 100, 150),
            make_row("trace-direct", "span-2", 160, 210),
        ])
        .expect("append rows");

    assert_eq!(inner.append_trace_blocks_calls.load(Ordering::Relaxed), 1);
    assert_eq!(inner.appended_block_count.load(Ordering::Relaxed), 1);
    assert_eq!(inner.appended_row_count.load(Ordering::Relaxed), 2);
}

#[test]
fn batching_engine_bypasses_outer_trace_batch_for_passthrough_engines() {
    let inner = Arc::new(PassthroughTraceBlockEngine::default());
    let engine = Arc::new(BatchingStorageEngine::with_config(
        inner.clone(),
        BatchingStorageConfig::default()
            .with_trace_shards(1)
            .with_max_trace_batch_blocks(64)
            .with_max_batch_rows(64)
            .with_max_batch_wait(Duration::from_millis(25)),
    ));

    let first = {
        let engine = engine.clone();
        thread::spawn(move || {
            engine
                .append_rows(vec![make_row("trace-pass", "span-1", 100, 150)])
                .expect("append first rows");
        })
    };

    thread::sleep(Duration::from_millis(5));
    engine
        .append_rows(vec![make_row("trace-pass", "span-2", 160, 210)])
        .expect("append second rows");
    first.join().expect("join first append");

    assert_eq!(
        inner.append_trace_blocks_calls.load(Ordering::Relaxed),
        2,
        "passthrough engines should bypass outer trace batching and append each request directly",
    );
    assert_eq!(
        inner.appended_block_count.load(Ordering::Relaxed),
        2,
        "passthrough engines should still preserve the caller's block boundaries",
    );
    assert_eq!(inner.appended_row_count.load(Ordering::Relaxed), 2);
    let stats = engine.stats();
    assert_eq!(stats.trace_batch_flushes, 0);
    assert_eq!(stats.trace_batch_input_blocks, 0);
    assert_eq!(stats.trace_batch_output_blocks, 0);
}

#[test]
fn batching_engine_forwards_fast_query_methods_to_inner_storage() {
    let inner = Arc::new(QueryForwardingEngine::default());
    let engine =
        BatchingStorageEngine::with_config(inner.clone(), BatchingStorageConfig::default());

    assert_eq!(
        engine.list_operations("checkout", 0, 1_000, 10),
        vec!["GET /checkout".to_string()]
    );

    let responses = engine.rows_for_traces(&[TraceRowsRequest {
        trace_id: "trace-forwarded".to_string(),
        start_unix_nano: 100,
        end_unix_nano: 150,
    }]);
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].trace_id, "trace-forwarded");
    assert_eq!(responses[0].rows.len(), 1);

    assert_eq!(inner.list_operations_calls.load(Ordering::Relaxed), 1);
    assert_eq!(inner.rows_for_traces_calls.load(Ordering::Relaxed), 1);
    assert_eq!(inner.search_traces_calls.load(Ordering::Relaxed), 0);
    assert_eq!(inner.rows_for_trace_calls.load(Ordering::Relaxed), 0);
}

#[test]
fn batching_engine_reduces_retained_blocks_for_memory_storage() {
    let inner: Arc<dyn StorageEngine> = Arc::new(MemoryStorageEngine::with_trace_shards(1));
    let engine = Arc::new(BatchingStorageEngine::with_config(
        inner,
        BatchingStorageConfig::default()
            .with_trace_shards(1)
            .with_max_trace_batch_blocks(64)
            .with_max_batch_rows(256)
            .with_max_batch_wait(Duration::from_millis(5)),
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
                .append_rows(vec![make_row(
                    &format!("trace-memory-{worker}"),
                    &format!("span-{worker}"),
                    100,
                    200,
                )])
                .expect("append rows");
        }));
    }

    for handle in handles {
        handle.join().expect("join worker");
    }

    let stats = engine.stats();
    assert!(
        stats.retained_trace_blocks < workers as u64,
        "worker-side coalescing should reduce retained block count"
    );
    assert!(stats.trace_batch_input_blocks >= workers as u64);
    assert!(stats.trace_batch_output_blocks < stats.trace_batch_input_blocks);
}
