use prost::Message;
use serde_json::{Map, Number, Value};

use crate::{
    AttributeValue, ExportLogsServiceRequest, KeyValue, LogRecord, ResourceLogs, ScopeLogs,
};

#[derive(Debug, thiserror::Error)]
pub enum LogsProtobufCodecError {
    #[error("failed to decode OTLP logs protobuf payload: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("invalid hex identifier for {field}: {value}")]
    InvalidHexId { field: &'static str, value: String },
}

pub fn decode_export_logs_service_request_protobuf(
    bytes: &[u8],
) -> Result<ExportLogsServiceRequest, LogsProtobufCodecError> {
    let request = OtlpExportLogsServiceRequest::decode(bytes)?;
    Ok(convert_export_request(request))
}

pub fn encode_export_logs_service_request_protobuf(
    request: &ExportLogsServiceRequest,
) -> Result<Vec<u8>, LogsProtobufCodecError> {
    let request = build_export_request(request)?;
    let mut bytes = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut bytes)
        .expect("encoding protobuf into vec should not fail");
    Ok(bytes)
}

fn convert_export_request(request: OtlpExportLogsServiceRequest) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: request
            .resource_logs
            .into_iter()
            .map(convert_resource_logs)
            .collect(),
    }
}

fn convert_resource_logs(resource_logs: OtlpResourceLogs) -> ResourceLogs {
    ResourceLogs {
        resource_attributes: resource_logs
            .resource
            .map(|resource| {
                resource
                    .attributes
                    .into_iter()
                    .map(convert_key_value)
                    .collect()
            })
            .unwrap_or_default(),
        scope_logs: resource_logs
            .scope_logs
            .into_iter()
            .map(convert_scope_logs)
            .collect(),
    }
}

fn convert_scope_logs(scope_logs: OtlpScopeLogs) -> ScopeLogs {
    let scope = scope_logs.scope;
    ScopeLogs {
        scope_name: scope
            .as_ref()
            .map(|scope| scope.name.clone())
            .filter(|value| !value.is_empty()),
        scope_version: scope
            .as_ref()
            .map(|scope| scope.version.clone())
            .filter(|value| !value.is_empty()),
        scope_attributes: scope
            .map(|scope| {
                scope
                    .attributes
                    .into_iter()
                    .map(convert_key_value)
                    .collect()
            })
            .unwrap_or_default(),
        log_records: scope_logs
            .log_records
            .into_iter()
            .map(convert_log_record)
            .collect(),
    }
}

fn convert_log_record(log_record: OtlpLogRecord) -> LogRecord {
    LogRecord {
        time_unix_nano: decode_unix_nano(log_record.time_unix_nano),
        observed_time_unix_nano: Some(decode_unix_nano(log_record.observed_time_unix_nano))
            .filter(|value| *value > 0),
        severity_number: Some(log_record.severity_number).filter(|value| *value > 0),
        severity_text: Some(log_record.severity_text).filter(|value| !value.is_empty()),
        body: convert_attribute_value(log_record.body),
        trace_id: if log_record.trace_id.is_empty() {
            None
        } else {
            Some(encode_hex(&log_record.trace_id))
        },
        span_id: if log_record.span_id.is_empty() {
            None
        } else {
            Some(encode_hex(&log_record.span_id))
        },
        attributes: log_record
            .attributes
            .into_iter()
            .map(convert_key_value)
            .collect(),
    }
}

fn convert_key_value(key_value: OtlpKeyValue) -> KeyValue {
    KeyValue {
        key: key_value.key,
        value: convert_attribute_value(key_value.value),
    }
}

fn convert_attribute_value(value: Option<OtlpAnyValue>) -> AttributeValue {
    match value.and_then(|value| value.value) {
        Some(otlp_any_value::Value::StringValue(value)) => AttributeValue::String(value),
        Some(otlp_any_value::Value::BoolValue(value)) => AttributeValue::Bool(value),
        Some(otlp_any_value::Value::IntValue(value)) => AttributeValue::I64(value),
        Some(otlp_any_value::Value::DoubleValue(value)) => AttributeValue::F64(value),
        Some(otlp_any_value::Value::ArrayValue(value)) => {
            AttributeValue::String(json_value_from_array(value).to_string())
        }
        Some(otlp_any_value::Value::KvlistValue(value)) => {
            AttributeValue::String(json_value_from_key_values(value.values).to_string())
        }
        Some(otlp_any_value::Value::BytesValue(value)) => {
            AttributeValue::String(encode_hex(&value))
        }
        None => AttributeValue::String(String::new()),
    }
}

fn build_export_request(
    request: &ExportLogsServiceRequest,
) -> Result<OtlpExportLogsServiceRequest, LogsProtobufCodecError> {
    Ok(OtlpExportLogsServiceRequest {
        resource_logs: request
            .resource_logs
            .iter()
            .map(build_resource_logs)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn build_resource_logs(
    resource_logs: &ResourceLogs,
) -> Result<OtlpResourceLogs, LogsProtobufCodecError> {
    Ok(OtlpResourceLogs {
        resource: Some(OtlpResource {
            attributes: resource_logs
                .resource_attributes
                .iter()
                .map(build_key_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        scope_logs: resource_logs
            .scope_logs
            .iter()
            .map(build_scope_logs)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn build_scope_logs(scope_logs: &ScopeLogs) -> Result<OtlpScopeLogs, LogsProtobufCodecError> {
    Ok(OtlpScopeLogs {
        scope: Some(OtlpInstrumentationScope {
            name: scope_logs.scope_name.clone().unwrap_or_default(),
            version: scope_logs.scope_version.clone().unwrap_or_default(),
            attributes: scope_logs
                .scope_attributes
                .iter()
                .map(build_key_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        log_records: scope_logs
            .log_records
            .iter()
            .map(build_log_record)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn build_log_record(log_record: &LogRecord) -> Result<OtlpLogRecord, LogsProtobufCodecError> {
    Ok(OtlpLogRecord {
        time_unix_nano: encode_unix_nano(log_record.time_unix_nano),
        observed_time_unix_nano: log_record
            .observed_time_unix_nano
            .map(encode_unix_nano)
            .unwrap_or_default(),
        severity_number: log_record.severity_number.unwrap_or_default(),
        severity_text: log_record.severity_text.clone().unwrap_or_default(),
        body: Some(build_attribute_value(&log_record.body)),
        attributes: log_record
            .attributes
            .iter()
            .map(build_key_value)
            .collect::<Result<Vec<_>, _>>()?,
        trace_id: log_record
            .trace_id
            .as_deref()
            .map(|value| decode_hex(value, "trace_id"))
            .transpose()?
            .unwrap_or_default(),
        span_id: log_record
            .span_id
            .as_deref()
            .map(|value| decode_hex(value, "span_id"))
            .transpose()?
            .unwrap_or_default(),
    })
}

fn build_key_value(key_value: &KeyValue) -> Result<OtlpKeyValue, LogsProtobufCodecError> {
    Ok(OtlpKeyValue {
        key: key_value.key.clone(),
        value: Some(build_attribute_value(&key_value.value)),
    })
}

fn build_attribute_value(value: &AttributeValue) -> OtlpAnyValue {
    let value = match value {
        AttributeValue::String(value) => otlp_any_value::Value::StringValue(value.clone()),
        AttributeValue::Bool(value) => otlp_any_value::Value::BoolValue(*value),
        AttributeValue::I64(value) => otlp_any_value::Value::IntValue(*value),
        AttributeValue::F64(value) => otlp_any_value::Value::DoubleValue(*value),
    };

    OtlpAnyValue { value: Some(value) }
}

fn json_value_from_array(value: OtlpArrayValue) -> Value {
    Value::Array(
        value
            .values
            .into_iter()
            .map(json_value_from_any_value)
            .collect(),
    )
}

fn json_value_from_any_value(value: OtlpAnyValue) -> Value {
    match value.value {
        Some(otlp_any_value::Value::StringValue(value)) => Value::String(value),
        Some(otlp_any_value::Value::BoolValue(value)) => Value::Bool(value),
        Some(otlp_any_value::Value::IntValue(value)) => Value::Number(Number::from(value)),
        Some(otlp_any_value::Value::DoubleValue(value)) => Number::from_f64(value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(otlp_any_value::Value::ArrayValue(value)) => json_value_from_array(value),
        Some(otlp_any_value::Value::KvlistValue(value)) => json_value_from_key_values(value.values),
        Some(otlp_any_value::Value::BytesValue(value)) => Value::String(encode_hex(&value)),
        None => Value::Null,
    }
}

fn json_value_from_key_values(values: Vec<OtlpKeyValue>) -> Value {
    let mut object = Map::new();
    for key_value in values {
        object.insert(
            key_value.key,
            key_value
                .value
                .map(json_value_from_any_value)
                .unwrap_or(Value::Null),
        );
    }
    Value::Object(object)
}

fn decode_unix_nano(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

fn encode_unix_nano(value: i64) -> u64 {
    value.max(0) as u64
}

fn encode_hex(bytes: &[u8]) -> String {
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(&mut output, "{byte:02x}");
    }
    output
}

fn decode_hex(value: &str, field: &'static str) -> Result<Vec<u8>, LogsProtobufCodecError> {
    let bytes = value.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(LogsProtobufCodecError::InvalidHexId {
            field,
            value: value.to_string(),
        });
    }

    let mut output = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0usize;
    while index < bytes.len() {
        let high = decode_hex_nibble(bytes[index]).ok_or_else(|| {
            LogsProtobufCodecError::InvalidHexId {
                field,
                value: value.to_string(),
            }
        })?;
        let low = decode_hex_nibble(bytes[index + 1]).ok_or_else(|| {
            LogsProtobufCodecError::InvalidHexId {
                field,
                value: value.to_string(),
            }
        })?;
        output.push((high << 4) | low);
        index += 2;
    }
    Ok(output)
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[derive(Clone, PartialEq, Message)]
struct OtlpExportLogsServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_logs: Vec<OtlpResourceLogs>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpResourceLogs {
    #[prost(message, optional, tag = "1")]
    resource: Option<OtlpResource>,
    #[prost(message, repeated, tag = "2")]
    scope_logs: Vec<OtlpScopeLogs>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpResource {
    #[prost(message, repeated, tag = "1")]
    attributes: Vec<OtlpKeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpScopeLogs {
    #[prost(message, optional, tag = "1")]
    scope: Option<OtlpInstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    log_records: Vec<OtlpLogRecord>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpInstrumentationScope {
    #[prost(string, tag = "1")]
    name: String,
    #[prost(string, tag = "2")]
    version: String,
    #[prost(message, repeated, tag = "3")]
    attributes: Vec<OtlpKeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpLogRecord {
    #[prost(fixed64, tag = "1")]
    time_unix_nano: u64,
    #[prost(fixed64, tag = "11")]
    observed_time_unix_nano: u64,
    #[prost(int32, tag = "2")]
    severity_number: i32,
    #[prost(string, tag = "3")]
    severity_text: String,
    #[prost(message, optional, tag = "5")]
    body: Option<OtlpAnyValue>,
    #[prost(message, repeated, tag = "6")]
    attributes: Vec<OtlpKeyValue>,
    #[prost(bytes = "vec", tag = "9")]
    trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "10")]
    span_id: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpKeyValue {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(message, optional, tag = "2")]
    value: Option<OtlpAnyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpAnyValue {
    #[prost(oneof = "otlp_any_value::Value", tags = "1, 2, 3, 4, 5, 6, 7")]
    value: Option<otlp_any_value::Value>,
}

mod otlp_any_value {
    use prost::Oneof;

    use super::{OtlpArrayValue, OtlpKeyValueList};

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Value {
        #[prost(string, tag = "1")]
        StringValue(String),
        #[prost(bool, tag = "2")]
        BoolValue(bool),
        #[prost(int64, tag = "3")]
        IntValue(i64),
        #[prost(double, tag = "4")]
        DoubleValue(f64),
        #[prost(message, tag = "5")]
        ArrayValue(OtlpArrayValue),
        #[prost(message, tag = "6")]
        KvlistValue(OtlpKeyValueList),
        #[prost(bytes = "vec", tag = "7")]
        BytesValue(Vec<u8>),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct OtlpArrayValue {
    #[prost(message, repeated, tag = "1")]
    values: Vec<OtlpAnyValue>,
}

#[derive(Clone, PartialEq, Message)]
pub struct OtlpKeyValueList {
    #[prost(message, repeated, tag = "1")]
    values: Vec<OtlpKeyValue>,
}
