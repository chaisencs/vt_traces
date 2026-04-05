use std::{borrow::Cow, sync::Arc};

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Field {
    pub name: Arc<str>,
    pub value: Arc<str>,
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
