use vtcore::{Field, LogRow};

use crate::flatten::{normalize_value, AttributeValue, FlattenError, KeyValue};

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExportLogsServiceRequest {
    pub resource_logs: Vec<ResourceLogs>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ResourceLogs {
    #[serde(default)]
    pub resource_attributes: Vec<KeyValue>,
    #[serde(default)]
    pub scope_logs: Vec<ScopeLogs>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScopeLogs {
    pub scope_name: Option<String>,
    pub scope_version: Option<String>,
    #[serde(default)]
    pub scope_attributes: Vec<KeyValue>,
    #[serde(default)]
    pub log_records: Vec<LogRecord>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LogRecord {
    pub time_unix_nano: i64,
    pub observed_time_unix_nano: Option<i64>,
    pub severity_number: Option<i32>,
    pub severity_text: Option<String>,
    pub body: AttributeValue,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    #[serde(default)]
    pub attributes: Vec<KeyValue>,
}

pub fn flatten_export_logs_request(
    request: &ExportLogsServiceRequest,
) -> Result<Vec<LogRow>, FlattenError> {
    let total_records: usize = request
        .resource_logs
        .iter()
        .map(|resource_logs| {
            resource_logs
                .scope_logs
                .iter()
                .map(|scope_logs| scope_logs.log_records.len())
                .sum::<usize>()
        })
        .sum();
    let mut rows = Vec::with_capacity(total_records);

    for (resource_index, resource_logs) in request.resource_logs.iter().enumerate() {
        let resource_fields =
            flatten_key_values(&resource_logs.resource_attributes, "resource_attr:");

        for (scope_index, scope_logs) in resource_logs.scope_logs.iter().enumerate() {
            let mut shared_fields = Vec::with_capacity(
                resource_fields.len()
                    + scope_logs.scope_attributes.len()
                    + usize::from(scope_logs.scope_name.is_some())
                    + usize::from(scope_logs.scope_version.is_some()),
            );
            shared_fields.extend(resource_fields.iter().cloned());
            if let Some(scope_name) = &scope_logs.scope_name {
                shared_fields.push(Field::new(
                    "instrumentation_scope.name",
                    normalize_value(scope_name.clone()),
                ));
            }
            if let Some(scope_version) = &scope_logs.scope_version {
                shared_fields.push(Field::new(
                    "instrumentation_scope.version",
                    normalize_value(scope_version.clone()),
                ));
            }
            shared_fields.extend(flatten_key_values(
                &scope_logs.scope_attributes,
                "scope_attr:",
            ));

            for (record_index, record) in scope_logs.log_records.iter().enumerate() {
                let mut fields = Vec::with_capacity(shared_fields.len() + record.attributes.len());
                fields.extend(shared_fields.iter().cloned());
                fields.extend(flatten_key_values(&record.attributes, "log_attr:"));
                let body = normalize_value(record.body.as_flat_string());
                let log_id = build_log_id(
                    resource_index,
                    scope_index,
                    record_index,
                    record.time_unix_nano,
                    &body,
                    record.trace_id.as_deref(),
                    record.span_id.as_deref(),
                );
                rows.push(LogRow::new_unsorted_fields(
                    log_id,
                    record.time_unix_nano,
                    record.observed_time_unix_nano,
                    record.severity_number,
                    record
                        .severity_text
                        .as_ref()
                        .map(|value| normalize_value(value.clone())),
                    body,
                    record.trace_id.clone(),
                    record.span_id.clone(),
                    fields,
                ));
            }
        }
    }

    Ok(rows)
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

fn build_log_id(
    resource_index: usize,
    scope_index: usize,
    record_index: usize,
    time_unix_nano: i64,
    body: &str,
    trace_id: Option<&str>,
    span_id: Option<&str>,
) -> String {
    let seed = format!(
        "{resource_index}:{scope_index}:{record_index}:{time_unix_nano}:{body}:{}:{}",
        trace_id.unwrap_or("-"),
        span_id.unwrap_or("-")
    );
    format!("log-{time_unix_nano}-{:016x}", stable_hash(&seed))
}

fn stable_hash(value: &str) -> u64 {
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;

    let mut hash = OFFSET_BASIS;
    for byte in value.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}
