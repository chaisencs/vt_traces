use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use vtcore::{Field, TraceModelError, TraceSpanRow};

#[derive(Debug, thiserror::Error)]
pub enum FlattenError {
    #[error(transparent)]
    InvalidTraceRow(#[from] TraceModelError),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExportTraceServiceRequest {
    pub resource_spans: Vec<ResourceSpans>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceSpans {
    #[serde(default)]
    pub resource_attributes: Vec<KeyValue>,
    #[serde(default)]
    pub scope_spans: Vec<ScopeSpans>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScopeSpans {
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    #[serde(default)]
    pub scope_attributes: Vec<KeyValue>,
    #[serde(default)]
    pub spans: Vec<SpanRecord>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpanRecord {
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub name: String,
    pub start_time_unix_nano: i64,
    pub end_time_unix_nano: i64,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
    pub status: Option<Status>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Status {
    pub code: i32,
    pub message: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KeyValue {
    pub key: String,
    pub value: AttributeValue,
}

impl KeyValue {
    pub fn new(key: impl Into<String>, value: AttributeValue) -> Self {
        Self {
            key: key.into(),
            value,
        }
    }

    pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(key, AttributeValue::String(value.into()))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum AttributeValue {
    String(String),
    Bool(bool),
    I64(i64),
    F64(f64),
}

impl AttributeValue {
    pub(crate) fn as_flat_string(&self) -> String {
        match self {
            Self::String(value) => normalize_value(value.clone()),
            Self::Bool(value) => value.to_string(),
            Self::I64(value) => value.to_string(),
            Self::F64(value) => value.to_string(),
        }
    }
}

pub fn flatten_export_request(
    request: &ExportTraceServiceRequest,
) -> Result<Vec<TraceSpanRow>, FlattenError> {
    let row_capacity = request
        .resource_spans
        .iter()
        .map(|resource_span| {
            resource_span
                .scope_spans
                .iter()
                .map(|scope_spans| scope_spans.spans.len())
                .sum::<usize>()
        })
        .sum();
    let mut rows = Vec::with_capacity(row_capacity);

    for resource_span in &request.resource_spans {
        let resource_fields =
            flatten_key_values(&resource_span.resource_attributes, "resource_attr:");

        for scope_spans in &resource_span.scope_spans {
            let mut shared_fields = Vec::with_capacity(
                resource_fields.len()
                    + scope_spans.scope_attributes.len()
                    + usize::from(scope_spans.scope_name.is_some())
                    + usize::from(scope_spans.scope_version.is_some()),
            );
            shared_fields.extend(resource_fields.iter().cloned());
            if let Some(scope_name) = &scope_spans.scope_name {
                shared_fields.push(Field::new(
                    "instrumentation_scope.name",
                    normalize_value(scope_name.clone()),
                ));
            }
            if let Some(scope_version) = &scope_spans.scope_version {
                shared_fields.push(Field::new(
                    "instrumentation_scope.version",
                    normalize_value(scope_version.clone()),
                ));
            }
            shared_fields.extend(flatten_key_values(
                &scope_spans.scope_attributes,
                "scope_attr:",
            ));

            for span in &scope_spans.spans {
                let mut fields =
                    Vec::with_capacity(shared_fields.len() + span.attributes.len() + 2);
                fields.extend(shared_fields.iter().cloned());
                fields.extend(flatten_key_values(&span.attributes, "span_attr:"));

                if let Some(status) = &span.status {
                    fields.push(Field::new("status_code", status.code.to_string()));
                    fields.push(Field::new(
                        "status_message",
                        normalize_value(status.message.clone()),
                    ));
                }

                rows.push(TraceSpanRow::new_unsorted_fields(
                    span.trace_id.clone(),
                    span.span_id.clone(),
                    span.parent_span_id.clone(),
                    span.name.clone(),
                    span.start_time_unix_nano,
                    span.end_time_unix_nano,
                    fields,
                )?);
            }
        }
    }

    Ok(rows)
}

pub fn export_request_from_rows(rows: &[TraceSpanRow]) -> ExportTraceServiceRequest {
    let mut groups: BTreeMap<String, GroupedResourceSpans> = BTreeMap::new();

    for row in rows {
        let mut resource_attributes = Vec::new();
        let mut scope_attributes = Vec::new();
        let mut span_attributes = Vec::new();
        let mut scope_name = None;
        let mut scope_version = None;
        let mut status_code = None;
        let mut status_message = None;

        for field in &row.fields {
            if let Some(key) = field.name.strip_prefix("resource_attr:") {
                resource_attributes
                    .push(KeyValue::string(key.to_string(), field.value.to_string()));
            } else if let Some(key) = field.name.strip_prefix("scope_attr:") {
                scope_attributes.push(KeyValue::string(key.to_string(), field.value.to_string()));
            } else if let Some(key) = field.name.strip_prefix("span_attr:") {
                span_attributes.push(KeyValue::string(key.to_string(), field.value.to_string()));
            } else if field.name.as_ref() == "instrumentation_scope.name"
                && field.value.as_ref() != "-"
            {
                scope_name = Some(field.value.to_string());
            } else if field.name.as_ref() == "instrumentation_scope.version"
                && field.value.as_ref() != "-"
            {
                scope_version = Some(field.value.to_string());
            } else if field.name.as_ref() == "status_code" {
                status_code = field.value.parse::<i32>().ok();
            } else if field.name.as_ref() == "status_message" && field.value.as_ref() != "-" {
                status_message = Some(field.value.to_string());
            }
        }

        resource_attributes.sort_by(|left, right| left.key.cmp(&right.key));
        scope_attributes.sort_by(|left, right| left.key.cmp(&right.key));
        span_attributes.sort_by(|left, right| left.key.cmp(&right.key));

        let status = status_code.map(|code| Status {
            code,
            message: status_message.unwrap_or_default(),
        });
        let key = serde_json::to_string(&(
            &resource_attributes,
            &scope_name,
            &scope_version,
            &scope_attributes,
        ))
        .expect("grouping key should serialize");

        groups
            .entry(key)
            .or_insert_with(|| GroupedResourceSpans {
                resource_attributes: resource_attributes.clone(),
                scope_name: scope_name.clone(),
                scope_version: scope_version.clone(),
                scope_attributes: scope_attributes.clone(),
                spans: Vec::new(),
            })
            .spans
            .push(SpanRecord {
                trace_id: row.trace_id.clone(),
                span_id: row.span_id.clone(),
                parent_span_id: row.parent_span_id.clone(),
                name: row.name.clone(),
                start_time_unix_nano: row.start_unix_nano,
                end_time_unix_nano: row.end_unix_nano,
                attributes: span_attributes,
                status,
            });
    }

    ExportTraceServiceRequest {
        resource_spans: groups
            .into_iter()
            .map(|(_, group)| ResourceSpans {
                resource_attributes: group.resource_attributes,
                scope_spans: vec![ScopeSpans {
                    scope_name: group.scope_name,
                    scope_version: group.scope_version,
                    scope_attributes: group.scope_attributes,
                    spans: group.spans,
                }],
            })
            .collect(),
    }
}

fn flatten_key_values(values: &[KeyValue], prefix: &str) -> Vec<Field> {
    let mut fields = Vec::with_capacity(values.len());
    for key_value in values {
        fields.push(Field::new(
            format!("{prefix}{}", key_value.key),
            key_value.value.as_flat_string(),
        ));
    }
    fields
}

pub(crate) fn normalize_value(value: String) -> String {
    if value.is_empty() {
        "-".to_string()
    } else {
        value
    }
}

#[derive(Debug, Clone)]
struct GroupedResourceSpans {
    resource_attributes: Vec<KeyValue>,
    scope_name: Option<String>,
    scope_version: Option<String>,
    scope_attributes: Vec<KeyValue>,
    spans: Vec<SpanRecord>,
}
