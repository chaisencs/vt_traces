use std::{
    collections::BTreeSet,
    hash::{Hash, Hasher},
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::RwLock;
use rustc_hash::FxHasher;
use vtcore::{
    LogRow, LogSearchRequest, TraceBlock, TraceSearchHit, TraceSearchRequest, TraceSpanRow,
    TraceWindow,
};

use crate::{
    log_state::LogIndexedState, state::IndexedState, StorageEngine, StorageError,
    StorageStatsSnapshot,
};

#[derive(Debug)]
pub struct MemoryStorageEngine {
    trace_shards: Vec<RwLock<IndexedState>>,
    log_state: RwLock<LogIndexedState>,
    trace_window_lookups: AtomicU64,
    row_queries: AtomicU64,
}

impl MemoryStorageEngine {
    pub fn new() -> Self {
        Self::with_trace_shards(default_trace_shards())
    }

    pub fn with_trace_shards(trace_shards: usize) -> Self {
        let trace_shards = trace_shards.max(1);
        Self {
            trace_shards: (0..trace_shards)
                .map(|_| RwLock::new(IndexedState::default()))
                .collect(),
            log_state: RwLock::new(LogIndexedState::default()),
            trace_window_lookups: AtomicU64::new(0),
            row_queries: AtomicU64::new(0),
        }
    }

    pub fn trace_window_lookups(&self) -> u64 {
        self.trace_window_lookups.load(Ordering::Relaxed)
    }

    pub fn row_queries(&self) -> u64 {
        self.row_queries.load(Ordering::Relaxed)
    }

    fn ingest_partitioned_trace_blocks(
        &self,
        blocks: impl IntoIterator<Item = TraceBlock>,
    ) -> Result<(), StorageError> {
        let trace_shards = self.trace_shards.len();
        let mut blocks_by_shard: Vec<Vec<TraceBlock>> =
            (0..trace_shards).map(|_| Vec::new()).collect();

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

        for (shard_index, shard_blocks) in blocks_by_shard.into_iter().enumerate() {
            if shard_blocks.is_empty() {
                continue;
            }
            let mut state = self.trace_shards[shard_index].write();
            for block in shard_blocks {
                state.ingest_block(block);
            }
        }
        Ok(())
    }
}

impl Default for MemoryStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine for MemoryStorageEngine {
    fn append_trace_block(&self, block: TraceBlock) -> Result<(), StorageError> {
        self.ingest_partitioned_trace_blocks(std::iter::once(block))
    }

    fn append_trace_blocks(&self, blocks: Vec<TraceBlock>) -> Result<(), StorageError> {
        self.ingest_partitioned_trace_blocks(blocks)
    }

    fn append_rows(&self, rows: Vec<TraceSpanRow>) -> Result<(), StorageError> {
        self.ingest_partitioned_trace_blocks(partition_rows_by_shard(rows, self.trace_shards.len()))
    }

    fn append_logs(&self, rows: Vec<LogRow>) -> Result<(), StorageError> {
        self.log_state.write().ingest_rows(rows);
        Ok(())
    }

    fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        self.trace_window_lookups.fetch_add(1, Ordering::Relaxed);
        let shard_index = trace_shard_index(trace_id, self.trace_shards.len());
        self.trace_shards[shard_index].read().trace_window(trace_id)
    }

    fn list_trace_ids(&self) -> Vec<String> {
        let mut trace_ids = BTreeSet::new();
        for shard in &self.trace_shards {
            trace_ids.extend(shard.read().list_trace_ids());
        }
        trace_ids.into_iter().collect()
    }

    fn list_services(&self) -> Vec<String> {
        let mut services = BTreeSet::new();
        for shard in &self.trace_shards {
            services.extend(shard.read().list_services());
        }
        services.into_iter().collect()
    }

    fn list_field_names(&self) -> Vec<String> {
        let mut field_names = BTreeSet::new();
        for shard in &self.trace_shards {
            field_names.extend(shard.read().list_field_names());
        }
        field_names.into_iter().collect()
    }

    fn list_field_values(&self, field_name: &str) -> Vec<String> {
        let mut field_values = BTreeSet::new();
        for shard in &self.trace_shards {
            field_values.extend(shard.read().list_field_values(field_name));
        }
        field_values.into_iter().collect()
    }

    fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        let mut hits = Vec::new();
        for shard in &self.trace_shards {
            hits.extend(shard.read().search_traces(request));
        }
        hits.sort_by(|left, right| {
            right
                .end_unix_nano
                .cmp(&left.end_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        hits.truncate(request.limit);
        hits
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
        let shard_index = trace_shard_index(trace_id, self.trace_shards.len());
        self.trace_shards[shard_index].read().rows_for_trace(
            trace_id,
            start_unix_nano,
            end_unix_nano,
        )
    }

    fn stats(&self) -> StorageStatsSnapshot {
        let (rows_ingested, traces_tracked, retained_trace_blocks) = self.trace_shards.iter().fold(
            (0u64, 0u64, 0u64),
            |(rows_acc, traces_acc, blocks_acc), shard| {
                let state = shard.read();
                (
                    rows_acc + state.rows_ingested(),
                    traces_acc + state.traces_tracked(),
                    blocks_acc + state.retained_blocks(),
                )
            },
        );
        StorageStatsSnapshot {
            rows_ingested,
            traces_tracked,
            retained_trace_blocks,
            persisted_bytes: 0,
            segment_count: 0,
            typed_field_columns: 0,
            string_field_columns: 0,
            trace_window_lookups: self.trace_window_lookups.load(Ordering::Relaxed),
            row_queries: self.row_queries.load(Ordering::Relaxed),
            segment_read_batches: 0,
            part_selective_decodes: 0,
            fsync_operations: 0,
            ..StorageStatsSnapshot::default()
        }
    }

    fn preferred_trace_ingest_shards(&self) -> usize {
        self.trace_shards.len()
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

fn partition_rows_by_shard(rows: Vec<TraceSpanRow>, trace_shards: usize) -> Vec<TraceBlock> {
    if rows.is_empty() {
        return Vec::new();
    }
    if trace_shards <= 1 {
        return vec![TraceBlock::from_rows(rows)];
    }

    let mut shard_builders: Vec<vtcore::TraceBlockBuilder> = (0..trace_shards)
        .map(|_| vtcore::TraceBlockBuilder::default())
        .collect();
    for row in rows {
        let shard_index = trace_shard_index(&row.trace_id, trace_shards);
        shard_builders[shard_index].push_row(row);
    }

    shard_builders
        .into_iter()
        .map(vtcore::TraceBlockBuilder::finish)
        .filter(|block| !block.is_empty())
        .collect()
}

fn partition_trace_block_by_shard(block: TraceBlock, trace_shards: usize) -> Vec<TraceBlock> {
    if block.is_empty() {
        return Vec::new();
    }
    if trace_shards <= 1 {
        return vec![block];
    }

    let mut shard_builders: Vec<vtcore::TraceBlockBuilder> = (0..trace_shards)
        .map(|_| vtcore::TraceBlockBuilder::default())
        .collect();
    for row_index in 0..block.row_count() {
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
        .map(vtcore::TraceBlockBuilder::finish)
        .filter(|block| !block.is_empty())
        .collect()
}

fn trace_block_shard_index(block: &TraceBlock, trace_shards: usize) -> Option<usize> {
    if block.is_empty() {
        return None;
    }
    let shard_index = trace_shard_index(block.trace_id_at(0), trace_shards);
    for row_index in 1..block.row_count() {
        if trace_shard_index(block.trace_id_at(row_index), trace_shards) != shard_index {
            return None;
        }
    }
    Some(shard_index)
}

#[cfg(test)]
mod tests {
    use super::{trace_block_shard_index, trace_shard_index};
    use vtcore::{Field, TraceBlock, TraceSpanRow};

    fn make_row(trace_id: &str, span_id: &str) -> TraceSpanRow {
        TraceSpanRow::new(
            trace_id.to_string(),
            span_id.to_string(),
            None,
            format!("span-{span_id}"),
            100,
            150,
            vec![Field::new("resource_attr:service.name", "checkout")],
        )
        .expect("valid row")
    }

    fn trace_ids_for_shard(trace_shards: usize, shard_index: usize, count: usize) -> Vec<String> {
        let mut trace_ids = Vec::with_capacity(count);
        let mut candidate = 0usize;
        while trace_ids.len() < count {
            let trace_id = format!("trace-shard-{candidate}");
            if trace_shard_index(&trace_id, trace_shards) == shard_index {
                trace_ids.push(trace_id);
            }
            candidate += 1;
        }
        trace_ids
    }

    #[test]
    fn trace_block_shard_index_detects_pre_sharded_multi_trace_block() {
        let trace_shards = 4;
        let trace_ids = trace_ids_for_shard(trace_shards, 2, 2);
        let block = TraceBlock::from_rows(vec![
            make_row(&trace_ids[0], "span-1"),
            make_row(&trace_ids[1], "span-2"),
        ]);

        assert_eq!(trace_block_shard_index(&block, trace_shards), Some(2));
    }

    #[test]
    fn trace_block_shard_index_rejects_cross_shard_block() {
        let block = TraceBlock::from_rows(vec![
            make_row("trace-shard-a", "span-1"),
            make_row("trace-shard-b", "span-2"),
        ]);

        let shard_index = trace_shard_index(block.trace_id_at(0), 4);
        let other_shard_index = trace_shard_index(block.trace_id_at(1), 4);
        if shard_index != other_shard_index {
            assert_eq!(trace_block_shard_index(&block, 4), None);
        }
    }
}
