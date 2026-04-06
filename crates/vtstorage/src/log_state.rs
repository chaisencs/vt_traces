use std::collections::{HashMap, HashSet};

use roaring::RoaringBitmap;
use vtcore::{FieldFilter, LogRow, LogSearchRequest};

use crate::bloom::StringBloomFilter;

type LogRef = u32;
type StringRef = u32;

#[derive(Debug, Default)]
pub(crate) struct LogIndexedState {
    log_ids: StringTable,
    strings: StringTable,
    all_log_refs: RoaringBitmap,
    rows_by_log: HashMap<LogRef, LogRow>,
    service_by_log: HashMap<LogRef, StringRef>,
    log_refs_by_service: HashMap<StringRef, RoaringBitmap>,
    severity_by_log: HashMap<LogRef, StringRef>,
    log_refs_by_severity: HashMap<StringRef, RoaringBitmap>,
    indexed_fields_by_log: HashMap<LogRef, HashMap<StringRef, HashSet<StringRef>>>,
    field_token_bloom_by_log: HashMap<LogRef, StringBloomFilter>,
    log_refs_by_field_name_value: HashMap<StringRef, HashMap<StringRef, RoaringBitmap>>,
}

impl LogIndexedState {
    pub(crate) fn ingest_rows<I>(&mut self, rows: I)
    where
        I: IntoIterator<Item = LogRow>,
    {
        for row in rows {
            let log_ref = self.log_ids.intern(&row.log_id);
            self.all_log_refs.insert(log_ref);
            self.observe_row_indexes(log_ref, &row);
            self.rows_by_log.insert(log_ref, row);
        }
    }

    pub(crate) fn search_logs(&self, request: &LogSearchRequest) -> Vec<LogRow> {
        let mut candidate_refs = self.all_log_refs.clone();

        if let Some(service_name) = request.service_name.as_ref() {
            let Some(service_ref) = self.strings.lookup(service_name) else {
                return Vec::new();
            };
            candidate_refs &= self
                .log_refs_by_service
                .get(&service_ref)
                .cloned()
                .unwrap_or_default();
        }

        if let Some(severity_text) = request.severity_text.as_ref() {
            let Some(severity_ref) = self.strings.lookup(severity_text) else {
                return Vec::new();
            };
            candidate_refs &= self
                .log_refs_by_severity
                .get(&severity_ref)
                .cloned()
                .unwrap_or_default();
        }

        for field_filter in &request.field_filters {
            let Some(field_name_ref) = self.strings.lookup(&field_filter.name) else {
                return Vec::new();
            };
            let Some(field_value_ref) = self.strings.lookup(&field_filter.value) else {
                return Vec::new();
            };
            candidate_refs &= self
                .log_refs_by_field_name_value
                .get(&field_name_ref)
                .and_then(|values| values.get(&field_value_ref))
                .cloned()
                .unwrap_or_default();
        }

        let mut rows: Vec<LogRow> = candidate_refs
            .into_iter()
            .filter_map(|log_ref| {
                let row = self.rows_by_log.get(&log_ref)?;
                if row.time_unix_nano < request.start_unix_nano
                    || row.time_unix_nano > request.end_unix_nano
                {
                    return None;
                }
                for field_filter in &request.field_filters {
                    if !self.log_matches_field_filter(log_ref, field_filter) {
                        return None;
                    }
                }
                Some(row.clone())
            })
            .collect();

        rows.sort_by(|left, right| {
            right
                .time_unix_nano
                .cmp(&left.time_unix_nano)
                .then_with(|| left.log_id.cmp(&right.log_id))
        });
        rows.truncate(request.limit);
        rows
    }

    fn observe_row_indexes(&mut self, log_ref: LogRef, row: &LogRow) {
        if let Some(service_name) = row.service_name() {
            if !service_name.is_empty() && service_name != "-" {
                let service_ref = self.strings.intern(service_name);
                self.service_by_log.insert(log_ref, service_ref);
                self.log_refs_by_service
                    .entry(service_ref)
                    .or_default()
                    .insert(log_ref);
            }
        }

        if let Some(severity_text) = row.severity_text.as_deref() {
            if !severity_text.is_empty() && severity_text != "-" {
                let severity_ref = self.strings.intern(severity_text);
                self.severity_by_log.insert(log_ref, severity_ref);
                self.log_refs_by_severity
                    .entry(severity_ref)
                    .or_default()
                    .insert(log_ref);
            }
        }

        self.observe_indexed_field(log_ref, "log.id", &row.log_id);
        self.observe_indexed_field(log_ref, "log.body", &row.body);
        if let Some(observed_time_unix_nano) = row.observed_time_unix_nano {
            self.observe_indexed_field(
                log_ref,
                "log.observed_time_unix_nano",
                &observed_time_unix_nano.to_string(),
            );
        }
        if let Some(severity_number) = row.severity_number {
            self.observe_indexed_field(
                log_ref,
                "log.severity_number",
                &severity_number.to_string(),
            );
        }
        if let Some(severity_text) = row.severity_text.as_deref() {
            self.observe_indexed_field(log_ref, "log.severity_text", severity_text);
        }
        if let Some(trace_id) = row.trace_id.as_deref() {
            self.observe_indexed_field(log_ref, "log.trace_id", trace_id);
        }
        if let Some(span_id) = row.span_id.as_deref() {
            self.observe_indexed_field(log_ref, "log.span_id", span_id);
        }
        for field in &row.fields {
            self.observe_indexed_field(log_ref, &field.name, &field.value);
        }
    }

    fn observe_indexed_field(&mut self, log_ref: LogRef, field_name: &str, field_value: &str) {
        if field_name.is_empty() || field_value.is_empty() {
            return;
        }

        let field_name_ref = self.strings.intern(field_name);
        let field_value_ref = self.strings.intern(field_value);

        self.indexed_fields_by_log
            .entry(log_ref)
            .or_default()
            .entry(field_name_ref)
            .or_default()
            .insert(field_value_ref);
        self.field_token_bloom_by_log
            .entry(log_ref)
            .or_default()
            .insert(&field_bloom_token(field_name, field_value));
        self.log_refs_by_field_name_value
            .entry(field_name_ref)
            .or_default()
            .entry(field_value_ref)
            .or_default()
            .insert(log_ref);
    }

    fn log_matches_field_filter(&self, log_ref: LogRef, field_filter: &FieldFilter) -> bool {
        let Some(field_name_ref) = self.strings.lookup(&field_filter.name) else {
            return false;
        };
        let Some(field_value_ref) = self.strings.lookup(&field_filter.value) else {
            return false;
        };
        let bloom = match self.field_token_bloom_by_log.get(&log_ref) {
            Some(bloom) => bloom,
            None => return false,
        };
        if !bloom.might_contain(&field_bloom_token(&field_filter.name, &field_filter.value)) {
            return false;
        }
        self.indexed_fields_by_log
            .get(&log_ref)
            .and_then(|fields| fields.get(&field_name_ref))
            .map(|values| values.contains(&field_value_ref))
            .unwrap_or(false)
    }
}

#[derive(Debug, Default)]
struct StringTable {
    ids_by_value: HashMap<String, u32>,
    values_by_id: Vec<String>,
}

impl StringTable {
    fn intern(&mut self, value: &str) -> u32 {
        if let Some(existing) = self.ids_by_value.get(value) {
            return *existing;
        }

        let id = self.values_by_id.len() as u32;
        let owned = value.to_string();
        self.values_by_id.push(owned.clone());
        self.ids_by_value.insert(owned, id);
        id
    }

    fn lookup(&self, value: &str) -> Option<u32> {
        self.ids_by_value.get(value).copied()
    }
}

fn field_bloom_token(field_name: &str, field_value: &str) -> String {
    format!("{field_name}={field_value}")
}
