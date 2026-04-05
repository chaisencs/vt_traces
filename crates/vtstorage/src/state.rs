use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use roaring::RoaringBitmap;
use vtcore::{FieldFilter, TraceSearchHit, TraceSearchRequest, TraceSpanRow, TraceWindow};

use crate::bloom::StringBloomFilter;

type TraceRef = u32;
type StringRef = u32;

#[derive(Debug, Default)]
pub(crate) struct IndexedState {
    trace_ids: StringTable,
    strings: StringTable,
    all_trace_refs: RoaringBitmap,
    rows_by_trace: HashMap<TraceRef, Vec<TraceSpanRow>>,
    windows_by_trace: HashMap<TraceRef, TraceWindowBounds>,
    services_by_trace: HashMap<TraceRef, HashSet<StringRef>>,
    trace_refs_by_service: HashMap<StringRef, RoaringBitmap>,
    operations_by_trace: HashMap<TraceRef, HashSet<StringRef>>,
    operation_bloom_by_trace: HashMap<TraceRef, StringBloomFilter>,
    indexed_fields_by_trace: HashMap<TraceRef, HashMap<StringRef, HashSet<StringRef>>>,
    field_token_bloom_by_trace: HashMap<TraceRef, StringBloomFilter>,
    trace_refs_by_field_name_value: HashMap<StringRef, HashMap<StringRef, RoaringBitmap>>,
    field_values_by_name: HashMap<StringRef, HashSet<StringRef>>,
    rows_ingested: u64,
}

impl IndexedState {
    pub(crate) fn ingest_rows<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = TraceSpanRow>,
    {
        let mut batch_by_trace: HashMap<TraceRef, BatchTraceUpdate> = HashMap::new();
        let mut rows_ingested = 0u64;
        for row in rows {
            let trace_ref = self.trace_ids.intern(&row.trace_id);
            let batch = batch_by_trace
                .entry(trace_ref)
                .or_insert_with(|| BatchTraceUpdate::new(row.start_unix_nano, row.end_unix_nano));
            batch.observe_window(row.start_unix_nano, row.end_unix_nano);
            if let Some(service_name) = row.service_name() {
                if !service_name.is_empty() && service_name != "-" {
                    batch.services.insert(self.strings.intern(service_name));
                }
            }
            batch.operations.insert(self.strings.intern(&row.name));
            self.collect_indexed_fields(&mut batch.indexed_fields, &row);
            batch.rows.push(row);
            rows_ingested += 1;
        }

        for (trace_ref, batch) in batch_by_trace {
            self.all_trace_refs.insert(trace_ref);
            self.windows_by_trace
                .entry(trace_ref)
                .and_modify(|window| {
                    window.observe(batch.start_unix_nano, batch.end_unix_nano)
                })
                .or_insert_with(|| TraceWindowBounds::new(batch.start_unix_nano, batch.end_unix_nano));

            let rows = self.rows_by_trace.entry(trace_ref).or_default();
            merge_sorted_trace_rows(rows, batch.rows);

            self.merge_trace_services(trace_ref, batch.services);
            self.merge_trace_operations(trace_ref, batch.operations);
            self.merge_trace_fields(trace_ref, batch.indexed_fields);
        }
        self.rows_ingested += rows_ingested;
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
        let operation_filter = request
            .operation_name
            .as_ref()
            .map(|operation_name| self.strings.lookup(operation_name));
        if matches!(operation_filter, Some(None)) {
            return Vec::new();
        }

        let field_filters: Vec<(StringRef, StringRef, &FieldFilter)> = match request
            .field_filters
            .iter()
            .map(|field_filter| {
                Some((
                    self.strings.lookup(&field_filter.name)?,
                    self.strings.lookup(&field_filter.value)?,
                    field_filter,
                ))
            })
            .collect()
        {
            Some(field_filters) => field_filters,
            None if request.field_filters.is_empty() => Vec::new(),
            None => return Vec::new(),
        };

        let mut hits: Vec<TraceSearchHit> = candidate_trace_refs
            .into_iter()
            .filter_map(|trace_ref| {
                let window = self.windows_by_trace.get(&trace_ref)?;
                let overlaps = window.end_unix_nano >= request.start_unix_nano
                    && window.start_unix_nano <= request.end_unix_nano;
                if !overlaps {
                    return None;
                }

                if let Some(Some(operation_ref)) = operation_filter {
                    let operation_name = request.operation_name.as_deref()?;
                    let bloom = self.operation_bloom_by_trace.get(&trace_ref)?;
                    if !bloom.might_contain(operation_name) {
                        return None;
                    }
                    let operations = self.operations_by_trace.get(&trace_ref)?;
                    if !operations.contains(&operation_ref) {
                        return None;
                    }
                }

                for (field_name_ref, field_value_ref, field_filter) in &field_filters {
                    if !self.trace_matches_field_filter(
                        trace_ref,
                        *field_name_ref,
                        *field_value_ref,
                        field_filter,
                    ) {
                        return None;
                    }
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
        self.rows_by_trace
            .get(&trace_ref)
            .map(|rows| {
                rows.iter()
                    .filter(|row| {
                        row.end_unix_nano >= start_unix_nano && row.start_unix_nano <= end_unix_nano
                    })
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    fn collect_indexed_fields(
        &mut self,
        indexed_fields: &mut HashMap<StringRef, HashSet<StringRef>>,
        row: &TraceSpanRow,
    ) {
        self.collect_indexed_field(indexed_fields, "name", &row.name);
        self.collect_indexed_field(indexed_fields, "duration", &row.duration_nanos().to_string());
        for field in &row.fields {
            if should_index_field(&field.name) {
                self.collect_indexed_field(indexed_fields, &field.name, &field.value);
            }
        }
    }

    fn collect_indexed_field(
        &mut self,
        indexed_fields: &mut HashMap<StringRef, HashSet<StringRef>>,
        field_name: &str,
        field_value: &str,
    ) {
        if field_name.is_empty() || field_value.is_empty() {
            return;
        }

        let field_name_ref = self.strings.intern(field_name);
        let field_value_ref = self.strings.intern(field_value);
        indexed_fields
            .entry(field_name_ref)
            .or_default()
            .insert(field_value_ref);
    }

    fn merge_trace_services(&mut self, trace_ref: TraceRef, services: HashSet<StringRef>) {
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

    fn merge_trace_operations(&mut self, trace_ref: TraceRef, operations: HashSet<StringRef>) {
        if operations.is_empty() {
            return;
        }

        let trace_operations = self.operations_by_trace.entry(trace_ref).or_default();
        trace_operations.reserve(operations.len());
        let bloom = self.operation_bloom_by_trace.entry(trace_ref).or_default();
        for operation_ref in operations {
            if trace_operations.insert(operation_ref) {
                if let Some(operation_name) = self.strings.resolve(operation_ref) {
                    bloom.insert(operation_name);
                }
            }
        }
    }

    fn merge_trace_fields(
        &mut self,
        trace_ref: TraceRef,
        indexed_fields: HashMap<StringRef, HashSet<StringRef>>,
    ) {
        if indexed_fields.is_empty() {
            return;
        }

        let trace_fields = self.indexed_fields_by_trace.entry(trace_ref).or_default();
        let bloom = self.field_token_bloom_by_trace.entry(trace_ref).or_default();
        for (field_name_ref, values) in indexed_fields {
            let field_name = self
                .strings
                .resolve(field_name_ref)
                .unwrap_or_default()
                .to_string();
            let trace_values = trace_fields.entry(field_name_ref).or_default();
            trace_values.reserve(values.len());
            for field_value_ref in values {
                if trace_values.insert(field_value_ref) {
                    let field_value = self
                        .strings
                        .resolve(field_value_ref)
                        .unwrap_or_default()
                        .to_string();
                    bloom.insert(&field_bloom_token(&field_name, &field_value));
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

    fn trace_matches_field_filter(
        &self,
        trace_ref: TraceRef,
        field_name_ref: StringRef,
        field_value_ref: StringRef,
        field_filter: &FieldFilter,
    ) -> bool {
        let bloom = match self.field_token_bloom_by_trace.get(&trace_ref) {
            Some(bloom) => bloom,
            None => return false,
        };
        if !bloom.might_contain(&field_bloom_token(&field_filter.name, &field_filter.value)) {
            return false;
        }
        self.indexed_fields_by_trace
            .get(&trace_ref)
            .and_then(|fields| fields.get(&field_name_ref))
            .map(|values| values.contains(&field_value_ref))
            .unwrap_or(false)
    }
}

#[derive(Debug, Default)]
struct StringTable {
    ids_by_value: HashMap<Arc<str>, u32>,
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
        self.values_by_id.get(id as usize).map(|value| value.as_ref())
    }
}

#[derive(Debug, Clone, Copy)]
struct TraceWindowBounds {
    start_unix_nano: i64,
    end_unix_nano: i64,
}

#[derive(Debug)]
struct BatchTraceUpdate {
    rows: Vec<TraceSpanRow>,
    services: HashSet<StringRef>,
    operations: HashSet<StringRef>,
    indexed_fields: HashMap<StringRef, HashSet<StringRef>>,
    start_unix_nano: i64,
    end_unix_nano: i64,
}

impl BatchTraceUpdate {
    fn new(start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            rows: Vec::new(),
            services: HashSet::new(),
            operations: HashSet::new(),
            indexed_fields: HashMap::new(),
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

fn field_bloom_token(field_name: &str, field_value: &str) -> String {
    format!("{field_name}={field_value}")
}

fn should_index_field(field_name: &str) -> bool {
    !matches!(
        field_name,
        "_time" | "end_time_unix_nano" | "start_time_unix_nano"
    )
}

fn merge_sorted_trace_rows(existing: &mut Vec<TraceSpanRow>, mut incoming: Vec<TraceSpanRow>) {
    if incoming.is_empty() {
        return;
    }

    sort_trace_rows_by_end(&mut incoming);
    if existing.is_empty() {
        *existing = incoming;
        return;
    }

    let append_only = existing
        .last()
        .zip(incoming.first())
        .map(|(current_last, incoming_first)| current_last.end_unix_nano <= incoming_first.end_unix_nano)
        .unwrap_or(false);
    if append_only {
        existing.reserve(incoming.len());
        existing.extend(incoming);
        return;
    }

    let prepend_only = existing
        .first()
        .zip(incoming.last())
        .map(|(current_first, incoming_last)| incoming_last.end_unix_nano <= current_first.end_unix_nano)
        .unwrap_or(false);
    if prepend_only {
        incoming.reserve(existing.len());
        incoming.extend(std::mem::take(existing));
        *existing = incoming;
        return;
    }

    existing.reserve(incoming.len());
    existing.extend(incoming);
    existing.sort_by_key(|row| row.end_unix_nano);
}

fn sort_trace_rows_by_end(rows: &mut Vec<TraceSpanRow>) {
    if rows
        .windows(2)
        .any(|window| window[0].end_unix_nano > window[1].end_unix_nano)
    {
        rows.sort_by_key(|row| row.end_unix_nano);
    }
}

#[cfg(test)]
mod tests {
    use super::merge_sorted_trace_rows;
    use vtcore::TraceSpanRow;

    fn make_row(trace_id: &str, span_id: &str, start: i64, end: i64) -> TraceSpanRow {
        TraceSpanRow::new_unsorted_fields(
            trace_id.to_string(),
            span_id.to_string(),
            None,
            format!("span-{span_id}"),
            start,
            end,
            Vec::new(),
        )
        .expect("valid row")
    }

    #[test]
    fn merge_sorted_trace_rows_appends_sorted_tail_without_reordering_existing_rows() {
        let mut existing = vec![
            make_row("trace-1", "span-1", 100, 150),
            make_row("trace-1", "span-2", 200, 250),
        ];
        let incoming = vec![
            make_row("trace-1", "span-3", 260, 300),
            make_row("trace-1", "span-4", 310, 360),
        ];

        merge_sorted_trace_rows(&mut existing, incoming);

        let end_times: Vec<i64> = existing.iter().map(|row| row.end_unix_nano).collect();
        assert_eq!(end_times, vec![150, 250, 300, 360]);
    }

    #[test]
    fn merge_sorted_trace_rows_sorts_unsorted_incoming_batch() {
        let mut existing = vec![make_row("trace-2", "span-1", 10, 20)];
        let incoming = vec![
            make_row("trace-2", "span-3", 40, 60),
            make_row("trace-2", "span-2", 20, 30),
        ];

        merge_sorted_trace_rows(&mut existing, incoming);

        let span_ids: Vec<&str> = existing.iter().map(|row| row.span_id.as_str()).collect();
        assert_eq!(span_ids, vec!["span-1", "span-2", "span-3"]);
    }
}
