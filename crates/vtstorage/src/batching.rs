use std::{
    collections::VecDeque,
    hash::{Hash, Hasher},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use parking_lot::{Condvar, Mutex};
use rustc_hash::FxHasher;
use vtcore::{
    LogRow, LogSearchRequest, TraceBlock, TraceBlockBuilder, TraceSearchHit, TraceSearchRequest,
    TraceSpanRow, TraceWindow,
};

use crate::{StorageEngine, StorageError, StorageStatsSnapshot, TraceBatchPayloadMode};

const DEFAULT_MAX_BATCH_ROWS: usize = 8_192;
const DEFAULT_MAX_TRACE_BATCH_BLOCKS: usize = 256;
const DEFAULT_MAX_BATCH_WAIT: Duration = Duration::from_micros(0);

#[derive(Debug, Clone, Copy)]
pub struct BatchingStorageConfig {
    max_batch_rows: usize,
    max_trace_batch_blocks: usize,
    max_batch_wait: Duration,
    trace_shards: usize,
}

impl Default for BatchingStorageConfig {
    fn default() -> Self {
        Self {
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
            max_trace_batch_blocks: DEFAULT_MAX_TRACE_BATCH_BLOCKS,
            max_batch_wait: DEFAULT_MAX_BATCH_WAIT,
            trace_shards: default_trace_shards(),
        }
    }
}

impl BatchingStorageConfig {
    pub fn with_max_batch_rows(mut self, max_batch_rows: usize) -> Self {
        self.max_batch_rows = max_batch_rows.max(1);
        self
    }

    pub fn with_max_trace_batch_blocks(mut self, max_trace_batch_blocks: usize) -> Self {
        self.max_trace_batch_blocks = max_trace_batch_blocks.max(1);
        self
    }

    pub fn with_max_batch_wait(mut self, max_batch_wait: Duration) -> Self {
        self.max_batch_wait = max_batch_wait;
        self
    }

    pub fn with_trace_shards(mut self, trace_shards: usize) -> Self {
        self.trace_shards = trace_shards.max(1);
        self
    }
}

pub struct BatchingStorageEngine {
    inner: Arc<dyn StorageEngine>,
    config: BatchingStorageConfig,
    log_tx: Sender<AppendLogRequest>,
    trace_batches: Vec<Arc<TraceBatchShard>>,
    trace_shards: usize,
}

#[derive(Debug)]
struct AppendLogRequest {
    rows: Vec<LogRow>,
    result_tx: Sender<Result<(), String>>,
}

#[derive(Debug)]
struct AppendTraceRequest {
    blocks: Vec<TraceBlock>,
    row_count: usize,
    block_count: usize,
    enqueued_at: Instant,
    result_tx: Sender<Result<(), String>>,
}

#[derive(Debug, Default)]
struct TraceBatchShardState {
    pending: VecDeque<AppendTraceRequest>,
    flushing: bool,
}

#[derive(Debug, Default)]
struct TraceBatchShardStats {
    queue_depth: AtomicU64,
    max_queue_depth: AtomicU64,
    flushes: AtomicU64,
    rows: AtomicU64,
    input_blocks: AtomicU64,
    output_blocks: AtomicU64,
    wait_micros_total: AtomicU64,
    wait_micros_max: AtomicU64,
    flush_due_to_rows: AtomicU64,
    flush_due_to_blocks: AtomicU64,
    flush_due_to_wait: AtomicU64,
}

#[derive(Debug)]
struct TraceBatchShard {
    state: Mutex<TraceBatchShardState>,
    ready: Condvar,
    stats: Arc<TraceBatchShardStats>,
}

#[derive(Debug, Clone, Copy)]
enum TraceBatchFlushReason {
    Rows,
    Blocks,
    Wait,
}

impl TraceBatchShardStats {
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

    fn record_flush(
        &self,
        rows: usize,
        input_blocks: usize,
        output_blocks: usize,
        wait_micros: u64,
        reason: TraceBatchFlushReason,
    ) {
        self.flushes.fetch_add(1, Ordering::Relaxed);
        self.rows.fetch_add(rows as u64, Ordering::Relaxed);
        self.input_blocks
            .fetch_add(input_blocks as u64, Ordering::Relaxed);
        self.output_blocks
            .fetch_add(output_blocks as u64, Ordering::Relaxed);
        self.wait_micros_total
            .fetch_add(wait_micros, Ordering::Relaxed);
        let _ =
            self.wait_micros_max
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                    (wait_micros > current).then_some(wait_micros)
                });
        match reason {
            TraceBatchFlushReason::Rows => {
                self.flush_due_to_rows.fetch_add(1, Ordering::Relaxed);
            }
            TraceBatchFlushReason::Blocks => {
                self.flush_due_to_blocks.fetch_add(1, Ordering::Relaxed);
            }
            TraceBatchFlushReason::Wait => {
                self.flush_due_to_wait.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

impl TraceBatchShard {
    fn new() -> Self {
        Self {
            state: Mutex::new(TraceBatchShardState::default()),
            ready: Condvar::new(),
            stats: Arc::new(TraceBatchShardStats::default()),
        }
    }

    fn submit(
        &self,
        request: AppendTraceRequest,
        inner: &Arc<dyn StorageEngine>,
        config: BatchingStorageConfig,
        payload_mode: TraceBatchPayloadMode,
    ) {
        self.stats.record_enqueue();
        let should_flush = {
            let mut state = self.state.lock();
            state.pending.push_back(request);
            self.ready.notify_one();
            if state.flushing {
                false
            } else {
                state.flushing = true;
                true
            }
        };

        if should_flush {
            self.flush_loop(inner, config, payload_mode);
        }
    }

    fn flush_loop(
        &self,
        inner: &Arc<dyn StorageEngine>,
        config: BatchingStorageConfig,
        payload_mode: TraceBatchPayloadMode,
    ) {
        loop {
            let Some(batch) = self.take_batch(config) else {
                return;
            };
            let total_rows = batch.iter().map(|request| request.row_count).sum::<usize>();
            let total_blocks = batch
                .iter()
                .map(|request| request.block_count)
                .sum::<usize>();
            let flush_reason = if total_rows >= config.max_batch_rows {
                TraceBatchFlushReason::Rows
            } else if total_blocks >= config.max_trace_batch_blocks {
                TraceBatchFlushReason::Blocks
            } else {
                TraceBatchFlushReason::Wait
            };
            let batch_wait_micros = saturating_micros(batch[0].enqueued_at.elapsed().as_micros());
            let (blocks, input_blocks, responders) = build_trace_batch_payload(batch, payload_mode);
            let output_blocks = blocks.len();
            self.stats.record_flush(
                total_rows,
                input_blocks,
                output_blocks,
                batch_wait_micros,
                flush_reason,
            );

            let result = inner
                .append_trace_blocks(blocks)
                .map_err(|error| error.to_string());
            for responder in responders {
                let _ = responder.send(result.clone());
            }
        }
    }

    fn take_batch(&self, config: BatchingStorageConfig) -> Option<Vec<AppendTraceRequest>> {
        let mut state = self.state.lock();
        if state.pending.is_empty() {
            state.flushing = false;
            return None;
        }

        let mut batch = Vec::new();
        let mut total_rows = 0usize;
        let mut total_blocks = 0usize;
        drain_trace_batch_locked(
            &mut state.pending,
            &mut batch,
            &mut total_rows,
            &mut total_blocks,
            config,
        );
        if config.max_batch_wait.is_zero()
            || total_rows >= config.max_batch_rows
            || total_blocks >= config.max_trace_batch_blocks
        {
            self.stats.record_dequeue(batch.len());
            return Some(batch);
        }

        let deadline = batch[0].enqueued_at + config.max_batch_wait;
        loop {
            if !state.pending.is_empty() {
                drain_trace_batch_locked(
                    &mut state.pending,
                    &mut batch,
                    &mut total_rows,
                    &mut total_blocks,
                    config,
                );
                if total_rows >= config.max_batch_rows
                    || total_blocks >= config.max_trace_batch_blocks
                {
                    self.stats.record_dequeue(batch.len());
                    return Some(batch);
                }
            }

            let now = Instant::now();
            if now >= deadline {
                self.stats.record_dequeue(batch.len());
                return Some(batch);
            }
            self.ready
                .wait_for(&mut state, deadline.saturating_duration_since(now));
        }
    }
}

impl BatchingStorageEngine {
    pub fn new(inner: Arc<dyn StorageEngine>) -> Self {
        Self::with_config(inner, BatchingStorageConfig::default())
    }

    pub fn with_config(inner: Arc<dyn StorageEngine>, config: BatchingStorageConfig) -> Self {
        let trace_shards = config.trace_shards.max(1);
        let (log_tx, log_rx) = mpsc::channel();
        let mut trace_batches = Vec::with_capacity(trace_shards);

        spawn_log_batch_worker(inner.clone(), log_rx, config);
        for _ in 0..trace_shards {
            trace_batches.push(Arc::new(TraceBatchShard::new()));
        }

        Self {
            inner,
            config,
            log_tx,
            trace_batches,
            trace_shards,
        }
    }

    fn enqueue_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        let payload_mode = self.inner.trace_batch_payload_mode();
        let mut blocks_by_shard = partition_trace_blocks_by_shard(blocks, self.trace_shards);
        let mut receivers = Vec::new();

        for (shard_index, shard_blocks) in blocks_by_shard.iter_mut().enumerate() {
            if shard_blocks.is_empty() {
                continue;
            }
            let row_count = shard_blocks.iter().map(TraceBlock::row_count).sum();
            let block_count = shard_blocks.len();
            let (result_tx, result_rx) = mpsc::channel();
            let request = AppendTraceRequest {
                blocks: std::mem::take(shard_blocks),
                row_count,
                block_count,
                enqueued_at: Instant::now(),
                result_tx,
            };
            self.trace_batches[shard_index].submit(request, &self.inner, self.config, payload_mode);
            receivers.push(result_rx);
        }

        for receiver in receivers {
            receiver
                .recv()
                .map_err(|error| {
                    StorageError::Message(format!("trace batch worker dropped: {error}"))
                })?
                .map_err(StorageError::Message)?;
        }
        Ok(())
    }

    fn append_trace_blocks_direct(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        let blocks: Vec<TraceBlock> = blocks
            .into_iter()
            .filter(|block| !block.is_empty())
            .collect();
        if blocks.is_empty() {
            return Ok(());
        }
        self.inner.append_trace_blocks(blocks)
    }

    fn bypass_trace_batching(&self) -> bool {
        self.inner.trace_batch_payload_mode() == TraceBatchPayloadMode::Passthrough
    }
}

impl StorageEngine for BatchingStorageEngine {
    fn append_trace_block(&self, block: TraceBlock) -> Result<(), StorageError> {
        if block.is_empty() {
            return Ok(());
        }
        if self.bypass_trace_batching() {
            return self.inner.append_trace_blocks(vec![block]);
        }
        self.enqueue_trace_blocks(vec![block])
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

        let (result_tx, result_rx) = mpsc::channel();
        self.log_tx
            .send(AppendLogRequest { rows, result_tx })
            .map_err(|error| StorageError::Message(format!("log batch queue closed: {error}")))?;
        result_rx
            .recv()
            .map_err(|error| StorageError::Message(format!("log batch worker dropped: {error}")))?
            .map_err(StorageError::Message)
    }

    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        self.inner.trace_window(trace_id)
    }

    fn list_trace_ids(&self) -> Vec<String> {
        self.inner.list_trace_ids()
    }

    fn list_services(&self) -> Vec<String> {
        self.inner.list_services()
    }

    fn list_field_names(&self) -> Vec<String> {
        self.inner.list_field_names()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        self.inner.list_field_values(field_name)
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        self.inner.search_traces(request)
    }

    fn search_logs(&self, request: &LogSearchRequest) -> Vec<LogRow> {
        self.inner.search_logs(request)
    }

    fn rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        self.inner
            .rows_for_trace(trace_id, start_unix_nano, end_unix_nano)
    }

    fn stats(&self) -> StorageStatsSnapshot {
        let mut stats = self.inner.stats();
        for trace_batch in &self.trace_batches {
            let shard_stats = &trace_batch.stats;
            stats.trace_batch_queue_depth += shard_stats.queue_depth.load(Ordering::Relaxed);
            stats.trace_batch_max_queue_depth = stats
                .trace_batch_max_queue_depth
                .max(shard_stats.max_queue_depth.load(Ordering::Relaxed));
            stats.trace_batch_flushes += shard_stats.flushes.load(Ordering::Relaxed);
            stats.trace_batch_rows += shard_stats.rows.load(Ordering::Relaxed);
            stats.trace_batch_input_blocks += shard_stats.input_blocks.load(Ordering::Relaxed);
            stats.trace_batch_output_blocks += shard_stats.output_blocks.load(Ordering::Relaxed);
            stats.trace_batch_wait_micros_total +=
                shard_stats.wait_micros_total.load(Ordering::Relaxed);
            stats.trace_batch_wait_micros_max = stats
                .trace_batch_wait_micros_max
                .max(shard_stats.wait_micros_max.load(Ordering::Relaxed));
            stats.trace_batch_flush_due_to_rows +=
                shard_stats.flush_due_to_rows.load(Ordering::Relaxed);
            stats.trace_batch_flush_due_to_blocks +=
                shard_stats.flush_due_to_blocks.load(Ordering::Relaxed);
            stats.trace_batch_flush_due_to_wait +=
                shard_stats.flush_due_to_wait.load(Ordering::Relaxed);
        }
        stats
    }

    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        if self.bypass_trace_batching() {
            return self.append_trace_blocks_direct(blocks);
        }
        self.enqueue_trace_blocks(blocks)
    }

    fn preferred_trace_ingest_shards(&self) -> usize {
        self.trace_shards
    }
}

fn spawn_log_batch_worker(
    inner: Arc<dyn StorageEngine>,
    rx: Receiver<AppendLogRequest>,
    config: BatchingStorageConfig,
) {
    thread::spawn(move || {
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            let mut total_rows = batch[0].rows.len();
            collect_log_batch(&rx, &mut batch, &mut total_rows, config);

            let mut rows = Vec::with_capacity(total_rows);
            let mut responders = Vec::with_capacity(batch.len());
            for request in batch {
                rows.extend(request.rows);
                responders.push(request.result_tx);
            }

            let result = inner.append_logs(rows).map_err(|error| error.to_string());
            for responder in responders {
                let _ = responder.send(result.clone());
            }
        }
    });
}

fn collect_log_batch(
    rx: &Receiver<AppendLogRequest>,
    batch: &mut Vec<AppendLogRequest>,
    total_rows: &mut usize,
    config: BatchingStorageConfig,
) {
    loop {
        if *total_rows >= config.max_batch_rows {
            break;
        }
        if config.max_batch_wait.is_zero() {
            match rx.try_recv() {
                Ok(request) => {
                    *total_rows += request.rows.len();
                    batch.push(request);
                }
                Err(TryRecvError::Empty) | Err(TryRecvError::Disconnected) => break,
            }
        } else {
            match rx.recv_timeout(config.max_batch_wait) {
                Ok(request) => {
                    *total_rows += request.rows.len();
                    batch.push(request);
                }
                Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => break,
            }
        }
    }
}

fn drain_trace_batch_locked(
    pending: &mut VecDeque<AppendTraceRequest>,
    batch: &mut Vec<AppendTraceRequest>,
    total_rows: &mut usize,
    total_blocks: &mut usize,
    config: BatchingStorageConfig,
) {
    while let Some(request) = pending.front() {
        let next_rows = *total_rows + request.row_count;
        let next_blocks = *total_blocks + request.block_count;
        if !batch.is_empty()
            && (next_rows > config.max_batch_rows || next_blocks > config.max_trace_batch_blocks)
        {
            break;
        }
        let request = pending
            .pop_front()
            .expect("front request should still exist for trace batch drain");
        *total_rows = next_rows;
        *total_blocks = next_blocks;
        batch.push(request);
        if *total_rows >= config.max_batch_rows || *total_blocks >= config.max_trace_batch_blocks {
            break;
        }
    }
}

fn build_trace_batch_payload(
    batch: Vec<AppendTraceRequest>,
    payload_mode: TraceBatchPayloadMode,
) -> (Vec<TraceBlock>, usize, Vec<Sender<Result<(), String>>>) {
    let mut input_blocks = 0usize;
    let mut blocks = Vec::new();
    let mut responders = Vec::with_capacity(batch.len());

    for request in batch {
        input_blocks += request.block_count;
        blocks.extend(request.blocks);
        responders.push(request.result_tx);
    }

    if blocks.len() <= 1 || payload_mode == TraceBatchPayloadMode::Passthrough {
        return (blocks, input_blocks, responders);
    }

    (
        vec![coalesce_trace_blocks(blocks)],
        input_blocks,
        responders,
    )
}

fn coalesce_trace_blocks(blocks: Vec<TraceBlock>) -> TraceBlock {
    let mut builder = TraceBlockBuilder::default();
    for block in blocks {
        builder.extend_block(block);
    }
    builder.finish()
}

fn saturating_micros(value: u128) -> u64 {
    value.min(u128::from(u64::MAX)) as u64
}

fn default_trace_shards() -> usize {
    std::thread::available_parallelism()
        .map(|value| value.get().clamp(1, 4))
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
    let first_trace_id = block.trace_ids.first()?;
    let shard_index = trace_shard_index(first_trace_id.as_ref(), trace_shards);
    block.trace_ids[1..]
        .iter()
        .all(|trace_id| trace_shard_index(trace_id.as_ref(), trace_shards) == shard_index)
        .then_some(shard_index)
}

fn partition_trace_blocks_by_shard(
    blocks: Vec<TraceBlock>,
    trace_shards: usize,
) -> Vec<Vec<TraceBlock>> {
    let mut blocks_by_shard = (0..trace_shards).map(|_| Vec::new()).collect::<Vec<_>>();
    for block in blocks {
        if block.is_empty() {
            continue;
        }
        if let Some(shard_index) = trace_block_shard_index(&block, trace_shards) {
            blocks_by_shard[shard_index].push(block);
            continue;
        }
        for partitioned in partition_trace_block_by_shard(block, trace_shards) {
            let shard_index = trace_shard_index(partitioned.trace_id_at(0), trace_shards);
            blocks_by_shard[shard_index].push(partitioned);
        }
    }
    blocks_by_shard
}

fn partition_trace_block_by_shard(block: TraceBlock, trace_shards: usize) -> Vec<TraceBlock> {
    if trace_shards <= 1 || block.is_empty() {
        return vec![block];
    }

    let mut shard_builders: Vec<TraceBlockBuilder> = (0..trace_shards)
        .map(|_| TraceBlockBuilder::default())
        .collect();
    let row_count = block.row_count();
    for row_index in 0..row_count {
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
