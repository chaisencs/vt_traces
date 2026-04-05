use std::{
    hash::{Hash, Hasher},
    sync::{
        mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
        Arc,
    },
    thread,
    time::Duration,
};

use rustc_hash::FxHasher;
use vtcore::{
    LogRow, LogSearchRequest, TraceBlock, TraceBlockBuilder, TraceSearchHit, TraceSearchRequest,
    TraceSpanRow, TraceWindow,
};

use crate::{StorageEngine, StorageError, StorageStatsSnapshot};

const DEFAULT_MAX_BATCH_ROWS: usize = 8_192;
const DEFAULT_MAX_BATCH_WAIT: Duration = Duration::from_micros(0);

#[derive(Debug, Clone, Copy)]
pub struct BatchingStorageConfig {
    max_batch_rows: usize,
    max_batch_wait: Duration,
    trace_shards: usize,
}

impl Default for BatchingStorageConfig {
    fn default() -> Self {
        Self {
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
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
    log_tx: Sender<AppendLogRequest>,
    trace_shards: usize,
}

#[derive(Debug)]
struct AppendLogRequest {
    rows: Vec<LogRow>,
    result_tx: Sender<Result<(), String>>,
}

impl BatchingStorageEngine {
    pub fn new(inner: Arc<dyn StorageEngine>) -> Self {
        Self::with_config(inner, BatchingStorageConfig::default())
    }

    pub fn with_config(inner: Arc<dyn StorageEngine>, config: BatchingStorageConfig) -> Self {
        let trace_shards = config.trace_shards.max(1);
        let (log_tx, log_rx) = mpsc::channel();

        spawn_log_batch_worker(inner.clone(), log_rx, config);

        Self {
            inner,
            log_tx,
            trace_shards,
        }
    }
}

impl StorageEngine for BatchingStorageEngine {
    fn append_trace_block(&self, block: TraceBlock) -> Result<(), StorageError> {
        if block.is_empty() {
            return Ok(());
        }

        self.inner
            .append_trace_blocks(partition_trace_block_by_shard(block, self.trace_shards))
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
        self.inner.stats()
    }

    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        self.inner.append_trace_blocks(
            blocks
                .into_iter()
                .filter(|block| !block.is_empty())
                .collect(),
        )
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
