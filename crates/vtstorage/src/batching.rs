use std::{
    sync::{
        mpsc::{self, Receiver, RecvTimeoutError, Sender, TryRecvError},
        Arc,
    },
    thread,
    time::Duration,
};

use vtcore::{LogRow, LogSearchRequest, TraceSearchHit, TraceSearchRequest, TraceSpanRow, TraceWindow};

use crate::{StorageEngine, StorageError, StorageStatsSnapshot};

const DEFAULT_MAX_BATCH_ROWS: usize = 8_192;
const DEFAULT_MAX_BATCH_WAIT: Duration = Duration::from_micros(0);

#[derive(Debug, Clone, Copy)]
pub struct BatchingStorageConfig {
    max_batch_rows: usize,
    max_batch_wait: Duration,
}

impl Default for BatchingStorageConfig {
    fn default() -> Self {
        Self {
            max_batch_rows: DEFAULT_MAX_BATCH_ROWS,
            max_batch_wait: DEFAULT_MAX_BATCH_WAIT,
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
}

pub struct BatchingStorageEngine {
    inner: Arc<dyn StorageEngine>,
    trace_tx: Sender<AppendTraceRequest>,
    log_tx: Sender<AppendLogRequest>,
}

#[derive(Debug)]
struct AppendTraceRequest {
    rows: Vec<TraceSpanRow>,
    result_tx: Sender<Result<(), String>>,
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
        let (trace_tx, trace_rx) = mpsc::channel();
        let (log_tx, log_rx) = mpsc::channel();

        spawn_trace_batch_worker(inner.clone(), trace_rx, config);
        spawn_log_batch_worker(inner.clone(), log_rx, config);

        Self {
            inner,
            trace_tx,
            log_tx,
        }
    }
}

impl StorageEngine for BatchingStorageEngine {
    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        if rows.is_empty() {
            return Ok(());
        }

        let (result_tx, result_rx) = mpsc::channel();
        self.trace_tx
            .send(AppendTraceRequest { rows, result_tx })
            .map_err(|error| StorageError::Message(format!("trace batch queue closed: {error}")))?;
        result_rx
            .recv()
            .map_err(|error| StorageError::Message(format!("trace batch worker dropped: {error}")))?
            .map_err(StorageError::Message)
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
        self.inner.rows_for_trace(trace_id, start_unix_nano, end_unix_nano)
    }

    fn stats(&self) -> StorageStatsSnapshot {
        self.inner.stats()
    }
}

fn spawn_trace_batch_worker(
    inner: Arc<dyn StorageEngine>,
    rx: Receiver<AppendTraceRequest>,
    config: BatchingStorageConfig,
) {
    thread::spawn(move || {
        while let Ok(first) = rx.recv() {
            let mut batch = vec![first];
            let mut total_rows = batch[0].rows.len();
            collect_trace_batch(&rx, &mut batch, &mut total_rows, config);

            let mut rows = Vec::with_capacity(total_rows);
            let mut responders = Vec::with_capacity(batch.len());
            for request in batch {
                rows.extend(request.rows);
                responders.push(request.result_tx);
            }

            let result = inner.append_rows(rows).map_err(|error| error.to_string());
            for responder in responders {
                let _ = responder.send(result.clone());
            }
        }
    });
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

fn collect_trace_batch(
    rx: &Receiver<AppendTraceRequest>,
    batch: &mut Vec<AppendTraceRequest>,
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
