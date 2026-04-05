use std::{borrow::Cow, collections::HashMap, iter::Chain, slice::Iter, sync::Arc};

use serde::{Deserialize, Serialize};
use thiserror::Error;

const LOG_RECORD_KIND_FIELD: &str = "record_kind";
const LOG_RECORD_KIND_VALUE: &str = "log";
const LOG_ID_FIELD: &str = "log.id";
const LOG_BODY_FIELD: &str = "log.body";
const LOG_OBSERVED_TIME_FIELD: &str = "log.observed_time_unix_nano";
const LOG_SEVERITY_NUMBER_FIELD: &str = "log.severity_number";
const LOG_SEVERITY_TEXT_FIELD: &str = "log.severity_text";
const LOG_TRACE_ID_FIELD: &str = "log.trace_id";
const LOG_SPAN_ID_FIELD: &str = "log.span_id";
const TRACE_TIME_FIELD: &str = "_time";
const TRACE_DURATION_FIELD: &str = "duration";
const TRACE_END_TIME_FIELD: &str = "end_time_unix_nano";
const TRACE_NAME_FIELD: &str = "name";
const TRACE_PARENT_SPAN_ID_FIELD: &str = "parent_span_id";
const TRACE_SPAN_ID_FIELD: &str = "span_id";
const TRACE_START_TIME_FIELD: &str = "start_time_unix_nano";
const TRACE_ID_FIELD: &str = "trace_id";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Field {
    pub name: Arc<str>,
    pub value: Arc<str>,
}

#[derive(Debug, Clone, Copy)]
pub struct TraceBlockFields<'a> {
    shared: &'a [Field],
    row: &'a [Field],
}

impl<'a> TraceBlockFields<'a> {
    pub fn len(&self) -> usize {
        self.shared.len() + self.row.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shared.is_empty() && self.row.is_empty()
    }

    pub fn iter(self) -> Chain<Iter<'a, Field>, Iter<'a, Field>> {
        self.shared.iter().chain(self.row.iter())
    }

    pub fn to_vec(self) -> Vec<Field> {
        let mut fields = Vec::with_capacity(self.len());
        fields.extend(self.shared.iter().cloned());
        fields.extend(self.row.iter().cloned());
        fields
    }
}

impl<'a> IntoIterator for TraceBlockFields<'a> {
    type Item = &'a Field;
    type IntoIter = Chain<Iter<'a, Field>, Iter<'a, Field>>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl Field {
    pub fn new(name: impl Into<Arc<str>>, value: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum TraceModelError {
    #[error("end time {end_unix_nano} is earlier than start time {start_unix_nano}")]
    InvalidTimeRange {
        start_unix_nano: i64,
        end_unix_nano: i64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSpanRow {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub start_unix_nano: i64,
    pub end_unix_nano: i64,
    pub time_unix_nano: i64,
    pub fields: Vec<Field>,
}

impl TraceSpanRow {
    pub fn new(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
        parent_span_id: impl Into<Option<String>>,
        name: impl Into<String>,
        start_unix_nano: i64,
        end_unix_nano: i64,
        fields: Vec<Field>,
    ) -> Result<Self, TraceModelError> {
        Self::new_with_field_order(
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_unix_nano,
            end_unix_nano,
            fields,
            true,
        )
    }

    pub fn new_unsorted_fields(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
        parent_span_id: impl Into<Option<String>>,
        name: impl Into<String>,
        start_unix_nano: i64,
        end_unix_nano: i64,
        fields: Vec<Field>,
    ) -> Result<Self, TraceModelError> {
        Self::new_with_field_order(
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_unix_nano,
            end_unix_nano,
            fields,
            false,
        )
    }

    pub fn new_prevalidated_unsorted_fields(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
        parent_span_id: impl Into<Option<String>>,
        name: impl Into<String>,
        start_unix_nano: i64,
        end_unix_nano: i64,
        fields: Vec<Field>,
    ) -> Result<Self, TraceModelError> {
        Self::new_prevalidated_with_field_order(
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_unix_nano,
            end_unix_nano,
            fields,
            false,
        )
    }

    fn new_with_field_order(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
        parent_span_id: impl Into<Option<String>>,
        name: impl Into<String>,
        start_unix_nano: i64,
        end_unix_nano: i64,
        mut fields: Vec<Field>,
        sort_fields: bool,
    ) -> Result<Self, TraceModelError> {
        if end_unix_nano < start_unix_nano {
            return Err(TraceModelError::InvalidTimeRange {
                start_unix_nano,
                end_unix_nano,
            });
        }

        let trace_id = trace_id.into();
        let span_id = span_id.into();
        let parent_span_id = parent_span_id.into();
        let name = name.into();
        let time_unix_nano = end_unix_nano;
        fields.retain(|field| !is_reserved_trace_field_name(&field.name));
        if sort_fields {
            fields.sort_by(|left, right| left.name.cmp(&right.name));
        }

        Ok(Self {
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_unix_nano,
            end_unix_nano,
            time_unix_nano,
            fields,
        })
    }

    fn new_prevalidated_with_field_order(
        trace_id: impl Into<String>,
        span_id: impl Into<String>,
        parent_span_id: impl Into<Option<String>>,
        name: impl Into<String>,
        start_unix_nano: i64,
        end_unix_nano: i64,
        mut fields: Vec<Field>,
        sort_fields: bool,
    ) -> Result<Self, TraceModelError> {
        if end_unix_nano < start_unix_nano {
            return Err(TraceModelError::InvalidTimeRange {
                start_unix_nano,
                end_unix_nano,
            });
        }

        if sort_fields {
            fields.sort_by(|left, right| left.name.cmp(&right.name));
        }

        Ok(Self {
            trace_id: trace_id.into(),
            span_id: span_id.into(),
            parent_span_id: parent_span_id.into(),
            name: name.into(),
            start_unix_nano,
            end_unix_nano,
            time_unix_nano: end_unix_nano,
            fields,
        })
    }

    pub fn duration_nanos(&self) -> i64 {
        self.end_unix_nano - self.start_unix_nano
    }

    pub fn field_value(&self, field_name: &str) -> Option<Cow<'_, str>> {
        match field_name {
            TRACE_TIME_FIELD => Some(Cow::Owned(self.time_unix_nano.to_string())),
            TRACE_DURATION_FIELD => Some(Cow::Owned(self.duration_nanos().to_string())),
            TRACE_END_TIME_FIELD => Some(Cow::Owned(self.end_unix_nano.to_string())),
            TRACE_NAME_FIELD => Some(Cow::Borrowed(self.name.as_str())),
            TRACE_PARENT_SPAN_ID_FIELD => self.parent_span_id.as_deref().map(Cow::Borrowed),
            TRACE_SPAN_ID_FIELD => Some(Cow::Borrowed(self.span_id.as_str())),
            TRACE_START_TIME_FIELD => Some(Cow::Owned(self.start_unix_nano.to_string())),
            TRACE_ID_FIELD => Some(Cow::Borrowed(self.trace_id.as_str())),
            _ => self
                .fields
                .iter()
                .find(|field| field.name.as_ref() == field_name)
                .map(|field| Cow::Borrowed(field.value.as_ref())),
        }
    }

    pub fn service_name(&self) -> Option<&str> {
        self.dynamic_field_value("resource_attr:service.name")
    }

    fn dynamic_field_value(&self, field_name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|field| field.name.as_ref() == field_name)
            .map(|field| field.value.as_ref())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompactSpanId {
    Hex64(u64),
    Text(Arc<str>),
}

impl CompactSpanId {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Hex64(_) => None,
            Self::Text(value) => Some(value.as_ref()),
        }
    }

    pub fn to_cow(&self) -> Cow<'_, str> {
        match self {
            Self::Hex64(value) => Cow::Owned(render_hex_u64(*value)),
            Self::Text(value) => Cow::Borrowed(value.as_ref()),
        }
    }
}

impl From<Arc<str>> for CompactSpanId {
    fn from(value: Arc<str>) -> Self {
        parse_hex_u64(value.as_ref())
            .map(Self::Hex64)
            .unwrap_or(Self::Text(value))
    }
}

impl From<String> for CompactSpanId {
    fn from(value: String) -> Self {
        parse_hex_u64(&value)
            .map(Self::Hex64)
            .unwrap_or_else(|| Self::Text(value.into()))
    }
}

impl From<&str> for CompactSpanId {
    fn from(value: &str) -> Self {
        parse_hex_u64(value)
            .map(Self::Hex64)
            .unwrap_or_else(|| Self::Text(Arc::<str>::from(value)))
    }
}

impl ToString for CompactSpanId {
    fn to_string(&self) -> String {
        match self {
            Self::Hex64(value) => render_hex_u64(*value),
            Self::Text(value) => value.to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct TraceBlock {
    pub trace_ids: Vec<Arc<str>>,
    pub span_ids: Vec<CompactSpanId>,
    pub parent_span_ids: Vec<Option<CompactSpanId>>,
    pub names: Vec<Arc<str>>,
    pub start_unix_nanos: Vec<i64>,
    pub end_unix_nanos: Vec<i64>,
    pub time_unix_nanos: Vec<i64>,
    pub shared_field_group_ids: Vec<u32>,
    pub shared_field_offsets: Vec<u32>,
    pub shared_fields: Vec<Field>,
    pub field_offsets: Vec<u32>,
    pub fields: Vec<Field>,
}

impl TraceBlock {
    pub fn builder() -> TraceBlockBuilder {
        TraceBlockBuilder::default()
    }

    pub fn from_rows(rows: Vec<TraceSpanRow>) -> Self {
        let mut builder = TraceBlockBuilder::with_capacity(rows.len());
        for row in rows {
            builder.push_row(row);
        }
        builder.finish()
    }

    pub fn row_count(&self) -> usize {
        self.trace_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.trace_ids.is_empty()
    }

    pub fn row(&self, row_index: usize) -> TraceSpanRow {
        let fields = self.fields_at(row_index).to_vec();
        TraceSpanRow {
            trace_id: self.trace_ids[row_index].to_string(),
            span_id: self.span_ids[row_index].to_string(),
            parent_span_id: self.parent_span_ids[row_index]
                .as_ref()
                .map(ToString::to_string),
            name: self.names[row_index].to_string(),
            start_unix_nano: self.start_unix_nanos[row_index],
            end_unix_nano: self.end_unix_nanos[row_index],
            time_unix_nano: self.time_unix_nanos[row_index],
            fields,
        }
    }

    pub fn rows(&self) -> Vec<TraceSpanRow> {
        (0..self.row_count())
            .map(|row_index| self.row(row_index))
            .collect()
    }

    pub fn trace_id_at(&self, row_index: usize) -> &str {
        &self.trace_ids[row_index]
    }

    pub fn span_id_at(&self, row_index: usize) -> Cow<'_, str> {
        self.span_ids[row_index].to_cow()
    }

    pub fn parent_span_id_at(&self, row_index: usize) -> Option<Cow<'_, str>> {
        self.parent_span_ids[row_index]
            .as_ref()
            .map(CompactSpanId::to_cow)
    }

    pub fn name_at(&self, row_index: usize) -> &str {
        &self.names[row_index]
    }

    pub fn start_unix_nano_at(&self, row_index: usize) -> i64 {
        self.start_unix_nanos[row_index]
    }

    pub fn end_unix_nano_at(&self, row_index: usize) -> i64 {
        self.end_unix_nanos[row_index]
    }

    pub fn time_unix_nano_at(&self, row_index: usize) -> i64 {
        self.time_unix_nanos[row_index]
    }

    pub fn fields_at(&self, row_index: usize) -> TraceBlockFields<'_> {
        let shared = self.shared_fields_for_row(row_index);
        let row = self.row_fields_at(row_index);
        TraceBlockFields { shared, row }
    }

    pub fn row_fields_at(&self, row_index: usize) -> &[Field] {
        let start = self.field_offsets[row_index] as usize;
        let end = self.field_offsets[row_index + 1] as usize;
        &self.fields[start..end]
    }

    pub fn shared_fields_for_row(&self, row_index: usize) -> &[Field] {
        self.shared_fields_for_group_id(self.shared_field_group_id_at(row_index))
    }

    pub fn shared_field_group_id_at(&self, row_index: usize) -> u32 {
        self.shared_field_group_ids
            .get(row_index)
            .copied()
            .unwrap_or(0)
    }

    pub fn shared_fields_for_group_id(&self, group_id: u32) -> &[Field] {
        let start = self
            .shared_field_offsets
            .get(group_id as usize)
            .copied()
            .unwrap_or(0) as usize;
        let end = self
            .shared_field_offsets
            .get(group_id as usize + 1)
            .copied()
            .unwrap_or(start as u32) as usize;
        &self.shared_fields[start..end]
    }

    pub fn shared_field_group_count(&self) -> usize {
        self.shared_field_offsets.len().saturating_sub(2)
    }

    pub fn stored_field_count(&self) -> usize {
        self.shared_fields.len() + self.fields.len()
    }

    pub fn service_name_at(&self, row_index: usize) -> Option<&str> {
        self.dynamic_field_value_at(row_index, "resource_attr:service.name")
    }

    pub fn dynamic_field_value_at(&self, row_index: usize, field_name: &str) -> Option<&str> {
        self.fields_at(row_index)
            .into_iter()
            .find(|field| field.name.as_ref() == field_name)
            .map(|field| field.value.as_ref())
    }
}

#[derive(Debug, Clone)]
pub struct TraceBlockBuilder {
    trace_ids: Vec<Arc<str>>,
    span_ids: Vec<CompactSpanId>,
    parent_span_ids: Vec<Option<CompactSpanId>>,
    names: Vec<Arc<str>>,
    start_unix_nanos: Vec<i64>,
    end_unix_nanos: Vec<i64>,
    time_unix_nanos: Vec<i64>,
    shared_field_group_ids: Vec<u32>,
    shared_field_offsets: Vec<u32>,
    shared_fields: Vec<Field>,
    field_offsets: Vec<u32>,
    fields: Vec<Field>,
    shared_field_group_index: HashMap<Vec<Field>, u32>,
    last_shared_field_group_ptr: usize,
    last_shared_field_group_len: usize,
    last_shared_field_group_id: u32,
}

impl Default for TraceBlockBuilder {
    fn default() -> Self {
        Self::with_capacity(0)
    }
}

impl TraceBlockBuilder {
    pub fn with_capacity(row_capacity: usize) -> Self {
        Self {
            trace_ids: Vec::with_capacity(row_capacity),
            span_ids: Vec::with_capacity(row_capacity),
            parent_span_ids: Vec::with_capacity(row_capacity),
            names: Vec::with_capacity(row_capacity),
            start_unix_nanos: Vec::with_capacity(row_capacity),
            end_unix_nanos: Vec::with_capacity(row_capacity),
            time_unix_nanos: Vec::with_capacity(row_capacity),
            shared_field_group_ids: Vec::with_capacity(row_capacity),
            shared_field_offsets: vec![0, 0],
            shared_fields: Vec::new(),
            field_offsets: vec![0],
            fields: Vec::new(),
            shared_field_group_index: HashMap::new(),
            last_shared_field_group_ptr: 0,
            last_shared_field_group_len: 0,
            last_shared_field_group_id: 0,
        }
    }

    pub fn reserve_rows(&mut self, additional: usize) {
        self.trace_ids.reserve(additional);
        self.span_ids.reserve(additional);
        self.parent_span_ids.reserve(additional);
        self.names.reserve(additional);
        self.start_unix_nanos.reserve(additional);
        self.end_unix_nanos.reserve(additional);
        self.time_unix_nanos.reserve(additional);
        self.shared_field_group_ids.reserve(additional);
        self.field_offsets.reserve(additional);
    }

    pub fn reserve_fields(&mut self, additional: usize) {
        self.fields.reserve(additional);
        self.shared_fields.reserve(additional / 4 + 1);
        self.shared_field_group_index.reserve(additional / 8 + 1);
    }

    pub fn push_row(&mut self, row: TraceSpanRow) {
        self.trace_ids.push(row.trace_id.into());
        self.span_ids.push(row.span_id.into());
        self.parent_span_ids
            .push(row.parent_span_id.map(Into::into));
        self.names.push(row.name.into());
        self.start_unix_nanos.push(row.start_unix_nano);
        self.end_unix_nanos.push(row.end_unix_nano);
        self.time_unix_nanos.push(row.time_unix_nano);
        self.shared_field_group_ids.push(0);
        self.fields.extend(row.fields);
        self.field_offsets.push(self.fields.len() as u32);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_prevalidated(
        &mut self,
        trace_id: impl Into<Arc<str>>,
        span_id: impl Into<CompactSpanId>,
        parent_span_id: Option<CompactSpanId>,
        name: impl Into<Arc<str>>,
        start_unix_nano: i64,
        end_unix_nano: i64,
        time_unix_nano: i64,
        fields: Vec<Field>,
    ) {
        self.trace_ids.push(trace_id.into());
        self.span_ids.push(span_id.into());
        self.parent_span_ids.push(parent_span_id);
        self.names.push(name.into());
        self.start_unix_nanos.push(start_unix_nano);
        self.end_unix_nanos.push(end_unix_nano);
        self.time_unix_nanos.push(time_unix_nano);
        self.shared_field_group_ids.push(0);
        self.fields.extend(fields);
        self.field_offsets.push(self.fields.len() as u32);
    }

    #[allow(clippy::too_many_arguments)]
    pub fn push_prevalidated_split_fields(
        &mut self,
        trace_id: impl Into<Arc<str>>,
        span_id: impl Into<CompactSpanId>,
        parent_span_id: Option<CompactSpanId>,
        name: impl Into<Arc<str>>,
        start_unix_nano: i64,
        end_unix_nano: i64,
        time_unix_nano: i64,
        shared_fields: &[Field],
        fields: impl IntoIterator<Item = Field>,
    ) {
        self.trace_ids.push(trace_id.into());
        self.span_ids.push(span_id.into());
        self.parent_span_ids.push(parent_span_id);
        self.names.push(name.into());
        self.start_unix_nanos.push(start_unix_nano);
        self.end_unix_nanos.push(end_unix_nano);
        self.time_unix_nanos.push(time_unix_nano);
        let shared_group_id = self.intern_shared_field_group(shared_fields);
        self.shared_field_group_ids.push(shared_group_id);
        self.fields.extend(fields);
        self.field_offsets.push(self.fields.len() as u32);
    }

    pub fn extend_block(&mut self, block: TraceBlock) {
        for row_index in 0..block.row_count() {
            self.push_prevalidated_split_fields(
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
    }

    pub fn finish(mut self) -> TraceBlock {
        if self.field_offsets.is_empty() {
            self.field_offsets.push(0);
        }
        TraceBlock {
            trace_ids: self.trace_ids,
            span_ids: self.span_ids,
            parent_span_ids: self.parent_span_ids,
            names: self.names,
            start_unix_nanos: self.start_unix_nanos,
            end_unix_nanos: self.end_unix_nanos,
            time_unix_nanos: self.time_unix_nanos,
            shared_field_group_ids: self.shared_field_group_ids,
            shared_field_offsets: self.shared_field_offsets,
            shared_fields: self.shared_fields,
            field_offsets: self.field_offsets,
            fields: self.fields,
        }
    }

    fn intern_shared_field_group(&mut self, shared_fields: &[Field]) -> u32 {
        if shared_fields.is_empty() {
            return 0;
        }
        let shared_group_ptr = shared_fields.as_ptr() as usize;
        if self.last_shared_field_group_id != 0
            && self.last_shared_field_group_ptr == shared_group_ptr
            && self.last_shared_field_group_len == shared_fields.len()
        {
            debug_assert_eq!(
                self.builder_shared_fields_for_group_id(self.last_shared_field_group_id),
                shared_fields
            );
            return self.last_shared_field_group_id;
        }
        if let Some(existing) = self.shared_field_group_index.get(shared_fields).copied() {
            self.remember_shared_field_group(shared_fields, existing);
            return existing;
        }

        let group_id = (self.shared_field_offsets.len() - 1) as u32;
        self.shared_fields.extend(shared_fields.iter().cloned());
        self.shared_field_offsets
            .push(self.shared_fields.len() as u32);
        self.shared_field_group_index
            .insert(shared_fields.to_vec(), group_id);
        self.remember_shared_field_group(shared_fields, group_id);
        group_id
    }

    fn remember_shared_field_group(&mut self, shared_fields: &[Field], group_id: u32) {
        self.last_shared_field_group_ptr = shared_fields.as_ptr() as usize;
        self.last_shared_field_group_len = shared_fields.len();
        self.last_shared_field_group_id = group_id;
    }

    fn builder_shared_fields_for_group_id(&self, group_id: u32) -> &[Field] {
        let start = self
            .shared_field_offsets
            .get(group_id as usize)
            .copied()
            .unwrap_or(0) as usize;
        let end = self
            .shared_field_offsets
            .get(group_id as usize + 1)
            .copied()
            .unwrap_or(start as u32) as usize;
        &self.shared_fields[start..end]
    }
}

fn parse_hex_u64(value: &str) -> Option<u64> {
    if value.len() != 16 {
        return None;
    }
    u64::from_str_radix(value, 16).ok()
}

fn render_hex_u64(value: u64) -> String {
    format!("{value:016x}")
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogRow {
    pub log_id: String,
    pub time_unix_nano: i64,
    pub observed_time_unix_nano: Option<i64>,
    pub severity_number: Option<i32>,
    pub severity_text: Option<String>,
    pub body: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub fields: Vec<Field>,
}

impl LogRow {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        log_id: impl Into<String>,
        time_unix_nano: i64,
        observed_time_unix_nano: Option<i64>,
        severity_number: Option<i32>,
        severity_text: Option<String>,
        body: impl Into<String>,
        trace_id: Option<String>,
        span_id: Option<String>,
        fields: Vec<Field>,
    ) -> Self {
        Self::new_with_field_order(
            log_id,
            time_unix_nano,
            observed_time_unix_nano,
            severity_number,
            severity_text,
            body,
            trace_id,
            span_id,
            fields,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_unsorted_fields(
        log_id: impl Into<String>,
        time_unix_nano: i64,
        observed_time_unix_nano: Option<i64>,
        severity_number: Option<i32>,
        severity_text: Option<String>,
        body: impl Into<String>,
        trace_id: Option<String>,
        span_id: Option<String>,
        fields: Vec<Field>,
    ) -> Self {
        Self::new_with_field_order(
            log_id,
            time_unix_nano,
            observed_time_unix_nano,
            severity_number,
            severity_text,
            body,
            trace_id,
            span_id,
            fields,
            false,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_with_field_order(
        log_id: impl Into<String>,
        time_unix_nano: i64,
        observed_time_unix_nano: Option<i64>,
        severity_number: Option<i32>,
        severity_text: Option<String>,
        body: impl Into<String>,
        trace_id: Option<String>,
        span_id: Option<String>,
        mut fields: Vec<Field>,
        sort_fields: bool,
    ) -> Self {
        if sort_fields {
            fields.sort_by(|left, right| left.name.cmp(&right.name));
        }
        Self {
            log_id: log_id.into(),
            time_unix_nano,
            observed_time_unix_nano,
            severity_number,
            severity_text,
            body: body.into(),
            trace_id,
            span_id,
            fields,
        }
    }

    pub fn field_value(&self, field_name: &str) -> Option<&str> {
        self.fields
            .iter()
            .find(|field| field.name.as_ref() == field_name)
            .map(|field| field.value.as_ref())
    }

    pub fn service_name(&self) -> Option<&str> {
        self.field_value("resource_attr:service.name")
    }

    pub fn to_trace_row(&self) -> Result<TraceSpanRow, TraceModelError> {
        let mut fields = self.fields.clone();
        fields.push(Field::new(LOG_RECORD_KIND_FIELD, LOG_RECORD_KIND_VALUE));
        fields.push(Field::new(LOG_ID_FIELD, self.log_id.clone()));
        fields.push(Field::new(LOG_BODY_FIELD, self.body.clone()));
        if let Some(observed_time_unix_nano) = self.observed_time_unix_nano {
            fields.push(Field::new(
                LOG_OBSERVED_TIME_FIELD,
                observed_time_unix_nano.to_string(),
            ));
        }
        if let Some(severity_number) = self.severity_number {
            fields.push(Field::new(
                LOG_SEVERITY_NUMBER_FIELD,
                severity_number.to_string(),
            ));
        }
        if let Some(severity_text) = self.severity_text.as_ref() {
            fields.push(Field::new(LOG_SEVERITY_TEXT_FIELD, severity_text.clone()));
        }
        if let Some(trace_id) = self.trace_id.as_ref() {
            fields.push(Field::new(LOG_TRACE_ID_FIELD, trace_id.clone()));
        }
        if let Some(span_id) = self.span_id.as_ref() {
            fields.push(Field::new(LOG_SPAN_ID_FIELD, span_id.clone()));
        }

        TraceSpanRow::new(
            self.log_id.clone(),
            self.log_id.clone(),
            None,
            self.severity_text
                .clone()
                .unwrap_or_else(|| "log".to_string()),
            self.time_unix_nano,
            self.time_unix_nano,
            fields,
        )
    }

    pub fn from_trace_row(row: &TraceSpanRow) -> Option<Self> {
        if row.field_value(LOG_RECORD_KIND_FIELD).as_deref() != Some(LOG_RECORD_KIND_VALUE) {
            return None;
        }

        let mut fields: Vec<Field> = row
            .fields
            .iter()
            .filter(|field| {
                !matches!(
                    field.name.as_ref(),
                    TRACE_TIME_FIELD
                        | TRACE_DURATION_FIELD
                        | TRACE_END_TIME_FIELD
                        | TRACE_NAME_FIELD
                        | TRACE_PARENT_SPAN_ID_FIELD
                        | TRACE_SPAN_ID_FIELD
                        | TRACE_START_TIME_FIELD
                        | TRACE_ID_FIELD
                        | LOG_RECORD_KIND_FIELD
                        | LOG_ID_FIELD
                        | LOG_BODY_FIELD
                        | LOG_OBSERVED_TIME_FIELD
                        | LOG_SEVERITY_NUMBER_FIELD
                        | LOG_SEVERITY_TEXT_FIELD
                        | LOG_TRACE_ID_FIELD
                        | LOG_SPAN_ID_FIELD
                )
            })
            .cloned()
            .collect();
        fields.sort_by(|left, right| left.name.cmp(&right.name));

        Some(Self {
            log_id: row
                .field_value(LOG_ID_FIELD)
                .map(Cow::into_owned)
                .unwrap_or_else(|| row.trace_id.clone()),
            time_unix_nano: row.time_unix_nano,
            observed_time_unix_nano: row
                .field_value(LOG_OBSERVED_TIME_FIELD)
                .as_deref()
                .and_then(|value| value.parse().ok()),
            severity_number: row
                .field_value(LOG_SEVERITY_NUMBER_FIELD)
                .as_deref()
                .and_then(|value| value.parse().ok()),
            severity_text: row
                .field_value(LOG_SEVERITY_TEXT_FIELD)
                .map(Cow::into_owned),
            body: row
                .field_value(LOG_BODY_FIELD)
                .map(Cow::into_owned)
                .unwrap_or_default(),
            trace_id: row.field_value(LOG_TRACE_ID_FIELD).map(Cow::into_owned),
            span_id: row.field_value(LOG_SPAN_ID_FIELD).map(Cow::into_owned),
            fields,
        })
    }
}

fn is_reserved_trace_field_name(field_name: &str) -> bool {
    matches!(
        field_name,
        TRACE_TIME_FIELD
            | TRACE_DURATION_FIELD
            | TRACE_END_TIME_FIELD
            | TRACE_NAME_FIELD
            | TRACE_PARENT_SPAN_ID_FIELD
            | TRACE_SPAN_ID_FIELD
            | TRACE_START_TIME_FIELD
            | TRACE_ID_FIELD
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceWindow {
    pub trace_id: String,
    pub start_unix_nano: i64,
    pub end_unix_nano: i64,
}

impl TraceWindow {
    pub fn new(trace_id: impl Into<String>, start_unix_nano: i64, end_unix_nano: i64) -> Self {
        Self {
            trace_id: trace_id.into(),
            start_unix_nano,
            end_unix_nano,
        }
    }

    pub fn observe(&mut self, start_unix_nano: i64, end_unix_nano: i64) {
        if start_unix_nano < self.start_unix_nano {
            self.start_unix_nano = start_unix_nano;
        }
        if end_unix_nano > self.end_unix_nano {
            self.end_unix_nano = end_unix_nano;
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSearchRequest {
    pub start_unix_nano: i64,
    pub end_unix_nano: i64,
    pub service_name: Option<String>,
    pub operation_name: Option<String>,
    #[serde(default)]
    pub field_filters: Vec<FieldFilter>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogSearchRequest {
    pub start_unix_nano: i64,
    pub end_unix_nano: i64,
    pub service_name: Option<String>,
    pub severity_text: Option<String>,
    #[serde(default)]
    pub field_filters: Vec<FieldFilter>,
    pub limit: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FieldFilter {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TraceSearchHit {
    pub trace_id: String,
    pub start_unix_nano: i64,
    pub end_unix_nano: i64,
    pub services: Vec<String>,
}
