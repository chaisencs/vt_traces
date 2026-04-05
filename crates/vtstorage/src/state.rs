use std::sync::Arc;

use roaring::RoaringBitmap;
use rustc_hash::{FxHashMap, FxHashSet};
use vtcore::{Field, TraceBlock, TraceSearchHit, TraceSearchRequest, TraceSpanRow, TraceWindow};

type TraceRef = u32;
type StringRef = u32;

#[derive(Default)]
struct BlockInternCache<'a> {
    trace_ids: FxHashMap<&'a str, TraceRef>,
    strings: FxHashMap<&'a str, StringRef>,
}

impl<'a> BlockInternCache<'a> {
    fn trace_ref(&mut self, table: &mut StringTable, value: &'a str) -> TraceRef {
        if let Some(existing) = self.trace_ids.get(value) {
            return *existing;
        }
        let trace_ref = table.intern(value);
        self.trace_ids.insert(value, trace_ref);
        trace_ref
    }

    fn string_ref(&mut self, table: &mut StringTable, value: &'a str) -> StringRef {
        if let Some(existing) = self.strings.get(value) {
            return *existing;
        }
        let string_ref = table.intern(value);
        self.strings.insert(value, string_ref);
        string_ref
    }
}

#[derive(Debug, Default)]
struct SharedGroupIndexUpdate {
    services: Vec<StringRef>,
    indexed_fields: Vec<(StringRef, StringRef)>,
}

impl SharedGroupIndexUpdate {
    fn record_service(&mut self, service_ref: StringRef) {
        if !self.services.contains(&service_ref) {
            self.services.push(service_ref);
        }
    }

    fn record_indexed_field(&mut self, field_name_ref: StringRef, field_value_ref: StringRef) {
        if !self.indexed_fields.iter().any(|(name_ref, value_ref)| {
            *name_ref == field_name_ref && *value_ref == field_value_ref
        }) {
            self.indexed_fields.push((field_name_ref, field_value_ref));
        }
    }
}

#[derive(Debug, Default)]
pub(crate) struct IndexedState {
    trace_ids: StringTable,
    strings: StringTable,
    all_trace_refs: RoaringBitmap,
    blocks: Vec<Arc<TraceBlock>>,
    row_refs_by_trace: FxHashMap<TraceRef, Vec<BlockRowRef>>,
    windows_by_trace: FxHashMap<TraceRef, TraceWindowBounds>,
    services_by_trace: FxHashMap<TraceRef, FxHashSet<StringRef>>,
    trace_refs_by_service: FxHashMap<StringRef, RoaringBitmap>,
    trace_refs_by_operation: FxHashMap<StringRef, RoaringBitmap>,
    trace_refs_by_field_name_value: FxHashMap<StringRef, FxHashMap<StringRef, RoaringBitmap>>,
    field_values_by_name: FxHashMap<StringRef, FxHashSet<StringRef>>,
    rows_ingested: u64,
}

impl IndexedState {
    pub(crate) fn ingest_block(&mut self, block: TraceBlock) {
        if block.is_empty() {
            return;
        }

        let row_count = block.row_count();
        let block = Arc::new(block);
        let mut block_intern_cache = BlockInternCache::default();
        let shared_group_updates = self.build_shared_group_updates(&block, &mut block_intern_cache);
        let block_id = self.blocks.len() as u32;

        if Self::block_has_single_trace(&block) {
            let trace_ref = block_intern_cache.trace_ref(&mut self.trace_ids, block.trace_id_at(0));
            let batch = self.build_single_trace_batch(
                block_id,
                &block,
                &shared_group_updates,
                &mut block_intern_cache,
            );
            self.blocks.push(block);
            self.apply_trace_batch(trace_ref, batch);
            self.rows_ingested += row_count as u64;
            return;
        }

        let mut batch_by_trace: FxHashMap<TraceRef, BatchTraceUpdate> = FxHashMap::default();
        batch_by_trace.reserve(row_count.min(8));

        for row_index in 0..row_count {
            let trace_ref =
                block_intern_cache.trace_ref(&mut self.trace_ids, block.trace_id_at(row_index));
            let batch = batch_by_trace.entry(trace_ref).or_insert_with(|| {
                BatchTraceUpdate::new(
                    block.start_unix_nano_at(row_index),
                    block.end_unix_nano_at(row_index),
                )
            });
            batch.observe_window(
                block.start_unix_nano_at(row_index),
                block.end_unix_nano_at(row_index),
            );
            batch.rows.push(BlockRowRef {
                block_id,
                row_index: row_index as u32,
            });

            self.collect_row_indexes(
                batch,
                &block,
                row_index,
                &shared_group_updates,
                &mut block_intern_cache,
            );
        }

        self.blocks.push(block);

        for (trace_ref, batch) in batch_by_trace {
            self.apply_trace_batch(trace_ref, batch);
        }

        self.rows_ingested += row_count as u64;
    }

    pub(crate) fn rows_ingested(&self) -> u64 {
        self.rows_ingested
    }

    pub(crate) fn traces_tracked(&self) -> u64 {
        self.windows_by_trace.len() as u64
    }

    pub(crate) fn trace_window(&self, trace_id: &str) -> Option<TraceWindow> {
        let trace_ref = self.trace_ids.lookup(trace_id)?;
        let bounds = self.windows_by_trace.get(&trace_ref)?;
        Some(TraceWindow::new(
            self.trace_ids.resolve(trace_ref)?.to_string(),
            bounds.start_unix_nano,
            bounds.end_unix_nano,
        ))
    }

    pub(crate) fn list_services(&self) -> Vec<String> {
        let mut services: Vec<String> = self
            .trace_refs_by_service
            .keys()
            .filter_map(|service_ref| self.strings.resolve(*service_ref).map(ToString::to_string))
            .collect();
        services.sort();
        services
    }

    pub(crate) fn list_trace_ids(&self) -> Vec<String> {
        let mut trace_ids: Vec<String> = self
            .windows_by_trace
            .keys()
            .filter_map(|trace_ref| self.trace_ids.resolve(*trace_ref).map(ToString::to_string))
            .collect();
        trace_ids.sort();
        trace_ids
    }

    pub(crate) fn list_field_names(&self) -> Vec<String> {
        let mut field_names: Vec<String> = self
            .field_values_by_name
            .keys()
            .filter_map(|field_name_ref| {
                self.strings
                    .resolve(*field_name_ref)
                    .map(ToString::to_string)
            })
            .collect();
        field_names.sort();
        field_names
    }

    pub(crate) fn list_field_values(&self, field_name: &str) -> Vec<String> {
        let Some(field_name_ref) = self.strings.lookup(field_name) else {
            return Vec::new();
        };

        let mut values: Vec<String> = self
            .field_values_by_name
            .get(&field_name_ref)
            .map(|values| {
                values
                    .iter()
                    .filter_map(|value_ref| {
                        self.strings.resolve(*value_ref).map(ToString::to_string)
                    })
                    .collect()
            })
            .unwrap_or_default();
        values.sort();
        values
    }

    pub(crate) fn search_traces(&self, request: &TraceSearchRequest) -> Vec<TraceSearchHit> {
        let candidate_trace_refs = self.candidate_trace_refs(request);
        if request
            .operation_name
            .as_ref()
            .is_some_and(|operation_name| self.strings.lookup(operation_name).is_none())
        {
            return Vec::new();
        }
        if request.field_filters.iter().any(|field_filter| {
            self.strings.lookup(&field_filter.name).is_none()
                || self.strings.lookup(&field_filter.value).is_none()
        }) {
            return Vec::new();
        }

        let mut hits: Vec<TraceSearchHit> = candidate_trace_refs
            .into_iter()
            .filter_map(|trace_ref| {
                let window = self.windows_by_trace.get(&trace_ref)?;
                let overlaps = window.end_unix_nano >= request.start_unix_nano
                    && window.start_unix_nano <= request.end_unix_nano;
                if !overlaps {
                    return None;
                }

                let mut services: Vec<String> = self
                    .services_by_trace
                    .get(&trace_ref)
                    .map(|services| {
                        services
                            .iter()
                            .filter_map(|service_ref| {
                                self.strings.resolve(*service_ref).map(ToString::to_string)
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                services.sort();

                Some(TraceSearchHit {
                    trace_id: self.trace_ids.resolve(trace_ref)?.to_string(),
                    start_unix_nano: window.start_unix_nano,
                    end_unix_nano: window.end_unix_nano,
                    services,
                })
            })
            .collect();

        hits.sort_by(|left, right| {
            right
                .end_unix_nano
                .cmp(&left.end_unix_nano)
                .then_with(|| left.trace_id.cmp(&right.trace_id))
        });
        hits.truncate(request.limit);
        hits
    }

    pub(crate) fn rows_for_trace(
        &self,
        trace_id: &str,
        start_unix_nano: i64,
        end_unix_nano: i64,
    ) -> Vec<TraceSpanRow> {
        let Some(trace_ref) = self.trace_ids.lookup(trace_id) else {
            return Vec::new();
        };
        self.row_refs_by_trace
            .get(&trace_ref)
            .map(|row_refs| {
                row_refs
                    .iter()
                    .filter_map(|row_ref| {
                        let block = self.blocks.get(row_ref.block_id as usize)?;
                        let row_index = row_ref.row_index as usize;
                        let row_end = block.end_unix_nano_at(row_index);
                        let row_start = block.start_unix_nano_at(row_index);
                        if row_end < start_unix_nano || row_start > end_unix_nano {
                            return None;
                        }
                        Some(block.row(row_index))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    fn collect_row_indexes<'a>(
        &mut self,
        batch: &mut BatchTraceUpdate,
        block: &'a TraceBlock,
        row_index: usize,
        shared_group_updates: &[SharedGroupIndexUpdate],
        block_intern_cache: &mut BlockInternCache<'a>,
    ) {
        push_unique_ref(
            &mut batch.operations,
            block_intern_cache.string_ref(&mut self.strings, block.name_at(row_index)),
        );

        let shared_group_id = block.shared_field_group_id_at(row_index);
        if shared_group_id != 0
            && push_unique_u32(&mut batch.applied_shared_groups, shared_group_id)
        {
            if let Some(shared_group_update) =
                shared_group_updates.get(shared_group_id as usize - 1)
            {
                for service_ref in &shared_group_update.services {
                    push_unique_ref(&mut batch.services, *service_ref);
                }
                for (field_name_ref, field_value_ref) in &shared_group_update.indexed_fields {
                    Self::insert_indexed_field(
                        &mut batch.indexed_fields,
                        *field_name_ref,
                        *field_value_ref,
                    );
                }
            }
        }

        for field in block.row_fields_at(row_index) {
            self.collect_row_field(batch, field, block_intern_cache);
        }
    }

    fn collect_row_field<'a>(
        &mut self,
        batch: &mut BatchTraceUpdate,
        field: &'a Field,
        block_intern_cache: &mut BlockInternCache<'a>,
    ) {
        let field_name = field.name.as_ref();
        let field_value = field.value.as_ref();
        if field_name.is_empty() || field_value.is_empty() {
            return;
        }

        let field_value_ref = block_intern_cache.string_ref(&mut self.strings, field_value);
        if field_name == "resource_attr:service.name" && field_value != "-" {
            push_unique_ref(&mut batch.services, field_value_ref);
        }
        if should_index_field(field_name) {
            let field_name_ref = block_intern_cache.string_ref(&mut self.strings, field_name);
            Self::insert_indexed_field(&mut batch.indexed_fields, field_name_ref, field_value_ref);
        }
    }

    fn build_shared_group_updates<'a>(
        &mut self,
        block: &'a TraceBlock,
        block_intern_cache: &mut BlockInternCache<'a>,
    ) -> Vec<SharedGroupIndexUpdate> {
        let mut shared_group_updates = Vec::with_capacity(block.shared_field_group_count());
        for group_id in 1..=block.shared_field_group_count() as u32 {
            let mut update = SharedGroupIndexUpdate::default();
            for field in block.shared_fields_for_group_id(group_id) {
                let field_name = field.name.as_ref();
                let field_value = field.value.as_ref();
                if field_name.is_empty() || field_value.is_empty() {
                    continue;
                }

                let field_value_ref = block_intern_cache.string_ref(&mut self.strings, field_value);
                if field_name == "resource_attr:service.name" && field_value != "-" {
                    update.record_service(field_value_ref);
                }
                if should_index_field(field_name) {
                    let field_name_ref =
                        block_intern_cache.string_ref(&mut self.strings, field_name);
                    update.record_indexed_field(field_name_ref, field_value_ref);
                }
            }
            shared_group_updates.push(update);
        }
        shared_group_updates
    }

    fn insert_indexed_field(
        indexed_fields: &mut Vec<(StringRef, StringRef)>,
        field_name_ref: StringRef,
        field_value_ref: StringRef,
    ) {
        push_unique_ref_pair(indexed_fields, field_name_ref, field_value_ref);
    }

    fn merge_trace_services(&mut self, trace_ref: TraceRef, services: Vec<StringRef>) {
        if services.is_empty() {
            return;
        }

        let trace_services = self.services_by_trace.entry(trace_ref).or_default();
        trace_services.reserve(services.len());
        for service_ref in services {
            if trace_services.insert(service_ref) {
                self.trace_refs_by_service
                    .entry(service_ref)
                    .or_default()
                    .insert(trace_ref);
            }
        }
    }

    fn merge_trace_operations(&mut self, trace_ref: TraceRef, operations: Vec<StringRef>) {
        if operations.is_empty() {
            return;
        }

        for operation_ref in operations {
            self.trace_refs_by_operation
                .entry(operation_ref)
                .or_default()
                .insert(trace_ref);
        }
    }

    fn merge_trace_fields(
        &mut self,
        trace_ref: TraceRef,
        indexed_fields: Vec<(StringRef, StringRef)>,
    ) {
        if indexed_fields.is_empty() {
            return;
        }

        for (field_name_ref, field_value_ref) in indexed_fields {
            self.trace_refs_by_field_name_value
                .entry(field_name_ref)
                .or_default()
                .entry(field_value_ref)
                .or_default()
                .insert(trace_ref);
            self.field_values_by_name
                .entry(field_name_ref)
                .or_default()
                .insert(field_value_ref);
        }
    }

    fn candidate_trace_refs(&self, request: &TraceSearchRequest) -> RoaringBitmap {
        let mut candidate_refs: Option<RoaringBitmap> = request
            .service_name
            .as_ref()
            .and_then(|service_name| self.strings.lookup(service_name))
            .map(|service_ref| {
                self.trace_refs_by_service
                    .get(&service_ref)
                    .cloned()
                    .unwrap_or_default()
            });

        if request.service_name.is_some() && candidate_refs.is_none() {
            return RoaringBitmap::new();
        }

        if let Some(operation_name) = &request.operation_name {
            let Some(operation_ref) = self.strings.lookup(operation_name) else {
                return RoaringBitmap::new();
            };
            let matching = self
                .trace_refs_by_operation
                .get(&operation_ref)
                .cloned()
                .unwrap_or_default();
            candidate_refs = Some(match candidate_refs {
                Some(current) => current & matching,
                None => matching,
            });
        }

        for field_filter in &request.field_filters {
            let Some(field_name_ref) = self.strings.lookup(&field_filter.name) else {
                return RoaringBitmap::new();
            };
            let Some(field_value_ref) = self.strings.lookup(&field_filter.value) else {
                return RoaringBitmap::new();
            };
            let matching = self
                .trace_refs_by_field_name_value
                .get(&field_name_ref)
                .and_then(|values| values.get(&field_value_ref))
                .cloned()
                .unwrap_or_default();
            candidate_refs = Some(match candidate_refs {
                Some(current) => current & matching,
                None => matching,
            });
        }

        candidate_refs.unwrap_or_else(|| self.all_trace_refs.clone())
    }

    fn block_has_single_trace(block: &TraceBlock) -> bool {
        let first_trace_id = block.trace_id_at(0);
        block
            .trace_ids
            .iter()
            .skip(1)
            .all(|trace_id| trace_id.as_ref() == first_trace_id)
    }

    fn build_single_trace_batch<'a>(
        &mut self,
        block_id: u32,
        block: &'a TraceBlock,
        shared_group_updates: &[SharedGroupIndexUpdate],
        block_intern_cache: &mut BlockInternCache<'a>,
    ) -> BatchTraceUpdate {
        let mut batch =
            BatchTraceUpdate::new(block.start_unix_nano_at(0), block.end_unix_nano_at(0));
        batch.rows.reserve(block.row_count());
        for row_index in 0..block.row_count() {
            batch.observe_window(
                block.start_unix_nano_at(row_index),
                block.end_unix_nano_at(row_index),
            );
            batch.rows.push(BlockRowRef {
                block_id,
                row_index: row_index as u32,
            });
            self.collect_row_indexes(
                &mut batch,
                block,
                row_index,
                shared_group_updates,
                block_intern_cache,
            );
        }
        batch
    }

    fn apply_trace_batch(&mut self, trace_ref: TraceRef, batch: BatchTraceUpdate) {
        self.all_trace_refs.insert(trace_ref);
        self.windows_by_trace
            .entry(trace_ref)
            .and_modify(|window| {
                window.observe(batch.start_unix_nano, batch.end_unix_nano);
            })
            .or_insert_with(|| TraceWindowBounds::new(batch.start_unix_nano, batch.end_unix_nano));

        let rows = self.row_refs_by_trace.entry(trace_ref).or_default();
        merge_sorted_block_row_refs(rows, batch.rows, &self.blocks);

        self.merge_trace_services(trace_ref, batch.services);
        self.merge_trace_operations(trace_ref, batch.operations);
        self.merge_trace_fields(trace_ref, batch.indexed_fields);
    }
}

#[derive(Debug, Default)]
struct StringTable {
    ids_by_value: FxHashMap<Arc<str>, u32>,
    values_by_id: Vec<Arc<str>>,
}

impl StringTable {
    fn intern(&mut self, value: &str) -> u32 {
        if let Some(existing) = self.ids_by_value.get(value) {
            return *existing;
        }

        let id = self.values_by_id.len() as u32;
        let owned = Arc::<str>::from(value);
        self.values_by_id.push(owned.clone());
        self.ids_by_value.insert(owned, id);
        id
    }

    fn lookup(&self, value: &str) -> Option<u32> {
        self.ids_by_value.get(value).copied()
    }

    fn resolve(&self, id: u32) -> Option<&str> {
        self.values_by_id
            .get(id as usize)
            .map(|value| value.as_ref())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BlockRowRef {
    block_id: u32,
    row_index: u32,
}

#[derive(Debug, Clone, Copy)]
struct TraceWindowBounds {
    start_unix_nano: i64,
    end_unix_nano: i64,
}

#[derive(Debug)]
struct BatchTraceUpdate {
    rows: Vec<BlockRowRef>,
    services: Vec<StringRef>,
    operations: Vec<StringRef>,
    indexed_fields: Vec<(StringRef, StringRef)>,
    applied_shared_groups: Vec<u32>,
    start_unix_nano: i64,
    end_unix_nano: i64,
}

impl BatchTraceUpdate {
    fn new(start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            rows: Vec::new(),
            services: Vec::new(),
            operations: Vec::new(),
            indexed_fields: Vec::new(),
            applied_shared_groups: Vec::new(),
            start_unix_nano,
            end_unix_nano,
        }
    }

    fn observe_window(&mut self, start_unix_nano: i64, end_unix_nano: i64) {
        if start_unix_nano < self.start_unix_nano {
            self.start_unix_nano = start_unix_nano;
        }
        if end_unix_nano > self.end_unix_nano {
            self.end_unix_nano = end_unix_nano;
        }
    }
}

impl TraceWindowBounds {
    fn new(start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            start_unix_nano,
            end_unix_nano,
        }
    }

    fn observe(&mut self, start_unix_nano: i64, end_unix_nano: i64) {
        if start_unix_nano < self.start_unix_nano {
            self.start_unix_nano = start_unix_nano;
        }
        if end_unix_nano > self.end_unix_nano {
            self.end_unix_nano = end_unix_nano;
        }
    }
}

fn push_unique_u32(values: &mut Vec<u32>, value: u32) -> bool {
    if values.contains(&value) {
        return false;
    }
    values.push(value);
    true
}

fn push_unique_ref(values: &mut Vec<StringRef>, value: StringRef) {
    if values.contains(&value) {
        return;
    }
    values.push(value);
}

fn push_unique_ref_pair(
    values: &mut Vec<(StringRef, StringRef)>,
    name: StringRef,
    value: StringRef,
) {
    if values
        .iter()
        .any(|(existing_name, existing_value)| *existing_name == name && *existing_value == value)
    {
        return;
    }
    values.push((name, value));
}

fn should_index_field(field_name: &str) -> bool {
    !matches!(
        field_name,
        "_time" | "end_time_unix_nano" | "start_time_unix_nano"
    )
}

fn merge_sorted_block_row_refs(
    existing: &mut Vec<BlockRowRef>,
    mut incoming: Vec<BlockRowRef>,
    blocks: &[Arc<TraceBlock>],
) {
    if incoming.is_empty() {
        return;
    }

    sort_block_row_refs_by_end(&mut incoming, blocks);
    if existing.is_empty() {
        *existing = incoming;
        return;
    }

    let append_only = existing
        .last()
        .zip(incoming.first())
        .map(|(current_last, incoming_first)| {
            row_ref_end_unix_nano(*current_last, blocks)
                <= row_ref_end_unix_nano(*incoming_first, blocks)
        })
        .unwrap_or(false);
    if append_only {
        existing.reserve(incoming.len());
        existing.extend(incoming);
        return;
    }

    let prepend_only = existing
        .first()
        .zip(incoming.last())
        .map(|(current_first, incoming_last)| {
            row_ref_end_unix_nano(*incoming_last, blocks)
                <= row_ref_end_unix_nano(*current_first, blocks)
        })
        .unwrap_or(false);
    if prepend_only {
        incoming.reserve(existing.len());
        incoming.extend(std::mem::take(existing));
        *existing = incoming;
        return;
    }

    existing.reserve(incoming.len());
    existing.extend(incoming);
    existing.sort_by_key(|row_ref| row_ref_end_unix_nano(*row_ref, blocks));
}

fn sort_block_row_refs_by_end(rows: &mut [BlockRowRef], blocks: &[Arc<TraceBlock>]) {
    if rows.windows(2).any(|window| {
        row_ref_end_unix_nano(window[0], blocks) > row_ref_end_unix_nano(window[1], blocks)
    }) {
        rows.sort_by_key(|row_ref| row_ref_end_unix_nano(*row_ref, blocks));
    }
}

fn row_ref_end_unix_nano(row_ref: BlockRowRef, blocks: &[Arc<TraceBlock>]) -> i64 {
    blocks[row_ref.block_id as usize].end_unix_nano_at(row_ref.row_index as usize)
}

#[cfg(test)]
mod tests {
    use super::{merge_sorted_block_row_refs, row_ref_end_unix_nano, BlockRowRef, IndexedState};
    use vtcore::{Field, TraceBlock, TraceSpanRow};

    fn make_row(trace_id: &str, span_id: &str, start: i64, end: i64) -> TraceSpanRow {
        TraceSpanRow::new_unsorted_fields(
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

    #[test]
    fn ingest_block_preserves_row_order_by_end_time_per_trace() {
        let mut state = IndexedState::default();
        state.ingest_block(TraceBlock::from_rows(vec![
            make_row("trace-1", "span-3", 300, 350),
            make_row("trace-1", "span-1", 100, 150),
            make_row("trace-1", "span-2", 200, 250),
        ]));

        let rows = state.rows_for_trace("trace-1", 0, 1_000);
        let end_times: Vec<i64> = rows.iter().map(|row| row.end_unix_nano).collect();
        assert_eq!(end_times, vec![150, 250, 350]);
    }

    #[test]
    fn ingest_block_merges_multiple_blocks_for_same_trace() {
        let mut state = IndexedState::default();
        state.ingest_block(TraceBlock::from_rows(vec![
            make_row("trace-2", "span-1", 100, 150),
            make_row("trace-2", "span-3", 300, 350),
        ]));
        state.ingest_block(TraceBlock::from_rows(vec![make_row(
            "trace-2", "span-2", 200, 250,
        )]));

        let span_ids: Vec<String> = state
            .rows_for_trace("trace-2", 0, 1_000)
            .into_iter()
            .map(|row| row.span_id)
            .collect();
        assert_eq!(span_ids, vec!["span-1", "span-2", "span-3"]);
    }

    #[test]
    fn merge_sorted_block_row_refs_sorts_unsorted_incoming_batch() {
        let mut state = IndexedState::default();
        state.ingest_block(TraceBlock::from_rows(vec![
            make_row("trace-3", "span-1", 10, 20),
            make_row("trace-3", "span-3", 40, 60),
            make_row("trace-3", "span-2", 20, 30),
        ]));

        let mut existing = vec![BlockRowRef {
            block_id: 0,
            row_index: 0,
        }];
        let incoming = vec![
            BlockRowRef {
                block_id: 0,
                row_index: 1,
            },
            BlockRowRef {
                block_id: 0,
                row_index: 2,
            },
        ];

        merge_sorted_block_row_refs(&mut existing, incoming, &state.blocks);
        let end_times: Vec<i64> = existing
            .iter()
            .map(|row_ref| row_ref_end_unix_nano(*row_ref, &state.blocks))
            .collect();
        assert_eq!(end_times, vec![20, 30, 60]);
    }

    #[test]
    fn ingest_block_indexes_shared_field_groups_without_losing_row_filters() {
        let mut state = IndexedState::default();
        let mut builder = TraceBlock::builder();
        let shared_fields = vec![
            Field::new("resource_attr:service.name", "checkout"),
            Field::new("resource_attr:deployment.environment", "prod"),
        ];
        builder.push_prevalidated_split_fields(
            "trace-shared-1",
            "span-1",
            None,
            "POST /checkout",
            100,
            150,
            150,
            &shared_fields,
            vec![Field::new("span_attr:http.method", "POST")],
        );
        builder.push_prevalidated_split_fields(
            "trace-shared-1",
            "span-2",
            Some("span-1".into()),
            "POST /checkout",
            160,
            210,
            210,
            &shared_fields,
            vec![Field::new("span_attr:http.status_code", "200")],
        );

        state.ingest_block(builder.finish());

        let hits = state.search_traces(&vtcore::TraceSearchRequest {
            start_unix_nano: 0,
            end_unix_nano: 1_000,
            service_name: Some("checkout".to_string()),
            operation_name: Some("POST /checkout".to_string()),
            field_filters: vec![vtcore::FieldFilter {
                name: "span_attr:http.status_code".to_string(),
                value: "200".to_string(),
            }],
            limit: 10,
        });

        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "trace-shared-1");
    }
}
