use std::{collections::HashMap, sync::Arc};

use prost::Message;
use serde_json::{Map, Number, Value};
use vtcore::{Field, TraceModelError, TraceSpanRow};

use crate::{
    flatten::normalize_value,
    AttributeValue, ExportTraceServiceRequest, KeyValue, ResourceSpans, ScopeSpans, SpanRecord,
    Status,
};

const WIRE_VARINT: u8 = 0;
const WIRE_FIXED64: u8 = 1;
const WIRE_LENGTH_DELIMITED: u8 = 2;
const WIRE_FIXED32: u8 = 5;

#[derive(Debug, thiserror::Error)]
pub enum ProtobufCodecError {
    #[error("failed to decode OTLP protobuf payload: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("invalid hex identifier for {field}: {value}")]
    InvalidHexId { field: &'static str, value: String },
    #[error(transparent)]
    InvalidTraceRow(#[from] TraceModelError),
}

#[derive(Default)]
struct FieldNameCache {
    resource_attr: Vec<(String, Arc<str>)>,
    scope_attr: Vec<(String, Arc<str>)>,
    span_attr: Vec<(String, Arc<str>)>,
    instrumentation_scope_name: Option<Arc<str>>,
    instrumentation_scope_version: Option<Arc<str>>,
    status_code: Option<Arc<str>>,
    status_message: Option<Arc<str>>,
}

#[derive(Default)]
struct FieldValueCache {
    strings: Vec<Arc<str>>,
    integers: HashMap<i64, Arc<str>>,
    doubles: HashMap<u64, Arc<str>>,
    empty: Option<Arc<str>>,
    true_value: Option<Arc<str>>,
    false_value: Option<Arc<str>>,
}

#[derive(Default)]
struct FastTraceRowsDecoder {
    field_names: FieldNameCache,
    field_values: FieldValueCache,
}

#[derive(Clone, Copy)]
enum FieldPrefix {
    ResourceAttr,
    ScopeAttr,
    SpanAttr,
}

impl FieldNameCache {
    fn prefixed(&mut self, prefix: FieldPrefix, key: &str) -> Arc<str> {
        let cache = match prefix {
            FieldPrefix::ResourceAttr => &mut self.resource_attr,
            FieldPrefix::ScopeAttr => &mut self.scope_attr,
            FieldPrefix::SpanAttr => &mut self.span_attr,
        };
        if let Some((_, existing)) = cache.iter().find(|(existing_key, _)| existing_key == key) {
            return existing.clone();
        }

        let value: Arc<str> = prefixed_key(prefix.value(), key).into();
        cache.push((key.to_string(), value.clone()));
        value
    }

    fn instrumentation_scope_name(&mut self) -> Arc<str> {
        self.instrumentation_scope_name
            .get_or_insert_with(|| Arc::<str>::from("instrumentation_scope.name"))
            .clone()
    }

    fn instrumentation_scope_version(&mut self) -> Arc<str> {
        self.instrumentation_scope_version
            .get_or_insert_with(|| Arc::<str>::from("instrumentation_scope.version"))
            .clone()
    }

    fn status_code(&mut self) -> Arc<str> {
        self.status_code
            .get_or_insert_with(|| Arc::<str>::from("status_code"))
            .clone()
    }

    fn status_message(&mut self) -> Arc<str> {
        self.status_message
            .get_or_insert_with(|| Arc::<str>::from("status_message"))
            .clone()
    }
}

impl FieldValueCache {
    fn empty(&mut self) -> Arc<str> {
        self.empty
            .get_or_insert_with(|| Arc::<str>::from("-"))
            .clone()
    }

    fn boolean(&mut self, value: bool) -> Arc<str> {
        if value {
            self.true_value
                .get_or_insert_with(|| Arc::<str>::from("true"))
                .clone()
        } else {
            self.false_value
                .get_or_insert_with(|| Arc::<str>::from("false"))
                .clone()
        }
    }

    fn integer(&mut self, value: i64) -> Arc<str> {
        if let Some(existing) = self.integers.get(&value) {
            return existing.clone();
        }

        let rendered = value.to_string();
        let interned: Arc<str> = rendered.as_str().into();
        self.integers.insert(value, interned.clone());
        interned
    }

    fn double(&mut self, value: f64) -> Arc<str> {
        let key = value.to_bits();
        if let Some(existing) = self.doubles.get(&key) {
            return existing.clone();
        }

        let rendered = value.to_string();
        let interned: Arc<str> = rendered.as_str().into();
        self.doubles.insert(key, interned.clone());
        interned
    }

    fn normalized_borrowed(&mut self, value: &str) -> Arc<str> {
        if value.is_empty() {
            return self.empty();
        }
        if let Some(existing) = self
            .strings
            .iter()
            .find(|existing| existing.as_ref() == value)
        {
            return existing.clone();
        }

        let interned: Arc<str> = value.into();
        self.strings.push(interned.clone());
        interned
    }

    fn normalized_owned(&mut self, value: String) -> Arc<str> {
        if value.is_empty() {
            return self.empty();
        }
        if let Some(existing) = self
            .strings
            .iter()
            .find(|existing| existing.as_ref() == value.as_str())
        {
            return existing.clone();
        }

        let interned: Arc<str> = value.as_str().into();
        self.strings.push(interned.clone());
        interned
    }
}

impl FastTraceRowsDecoder {
    fn decode(mut self, bytes: &[u8]) -> Result<Vec<TraceSpanRow>, ProtobufCodecError> {
        let mut rows = Vec::new();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let resource_spans = decode_length_delimited(bytes, &mut cursor)?;
                    self.decode_resource_spans(resource_spans, &mut rows)?;
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        Ok(rows)
    }

    fn decode_resource_spans(
        &mut self,
        bytes: &[u8],
        rows: &mut Vec<TraceSpanRow>,
    ) -> Result<(), ProtobufCodecError> {
        let mut resource_fields = Vec::new();
        let mut scope_spans = Vec::new();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let resource = decode_length_delimited(bytes, &mut cursor)?;
                    resource_fields =
                        self.decode_attribute_fields(resource, FieldPrefix::ResourceAttr)?;
                }
                (3, WIRE_LENGTH_DELIMITED) => {
                    scope_spans.push(decode_length_delimited(bytes, &mut cursor)?);
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        for scope_spans in scope_spans {
            self.decode_scope_spans(scope_spans, &resource_fields, rows)?;
        }

        Ok(())
    }

    fn decode_attribute_fields(
        &mut self,
        bytes: &[u8],
        prefix: FieldPrefix,
    ) -> Result<Vec<Field>, ProtobufCodecError> {
        let mut fields = Vec::new();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let key_value = decode_length_delimited(bytes, &mut cursor)?;
                    if let Some(field) = self.decode_key_value(key_value, prefix)? {
                        fields.push(field);
                    }
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        Ok(fields)
    }

    fn decode_scope_spans(
        &mut self,
        bytes: &[u8],
        resource_fields: &[Field],
        rows: &mut Vec<TraceSpanRow>,
    ) -> Result<(), ProtobufCodecError> {
        let mut scope_name = None;
        let mut scope_version = None;
        let mut scope_attributes = Vec::new();
        let mut spans = Vec::new();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    let scope = decode_length_delimited(bytes, &mut cursor)?;
                    let decoded_scope = self.decode_instrumentation_scope(scope)?;
                    scope_name = decoded_scope.name;
                    scope_version = decoded_scope.version;
                    scope_attributes = decoded_scope.attributes;
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    spans.push(decode_length_delimited(bytes, &mut cursor)?);
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        let mut shared_fields = Vec::with_capacity(
            resource_fields.len()
                + scope_attributes.len()
                + usize::from(scope_name.is_some())
                + usize::from(scope_version.is_some()),
        );
        shared_fields.extend(resource_fields.iter().cloned());
        if let Some(scope_name) = scope_name {
            shared_fields.push(Field::new(
                self.field_names.instrumentation_scope_name(),
                scope_name,
            ));
        }
        if let Some(scope_version) = scope_version {
            shared_fields.push(Field::new(
                self.field_names.instrumentation_scope_version(),
                scope_version,
            ));
        }
        shared_fields.extend(scope_attributes);

        for span in spans {
            rows.push(self.decode_span(span, &shared_fields)?);
        }

        Ok(())
    }

    fn decode_instrumentation_scope(
        &mut self,
        bytes: &[u8],
    ) -> Result<DecodedInstrumentationScope, ProtobufCodecError> {
        let mut scope = DecodedInstrumentationScope::default();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    scope.name = Some(
                        self.field_values
                            .normalized_borrowed(decode_str(bytes, &mut cursor)?),
                    );
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    scope.version = Some(
                        self.field_values
                            .normalized_borrowed(decode_str(bytes, &mut cursor)?),
                    );
                }
                (3, WIRE_LENGTH_DELIMITED) => {
                    let key_value = decode_length_delimited(bytes, &mut cursor)?;
                    if let Some(field) = self.decode_key_value(key_value, FieldPrefix::ScopeAttr)? {
                        scope.attributes.push(field);
                    }
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        Ok(scope)
    }

    fn decode_span(
        &mut self,
        bytes: &[u8],
        shared_fields: &[Field],
    ) -> Result<TraceSpanRow, ProtobufCodecError> {
        let mut trace_id = String::new();
        let mut span_id = String::new();
        let mut parent_span_id = None;
        let mut name = String::new();
        let mut start_time_unix_nano = 0u64;
        let mut end_time_unix_nano = 0u64;
        let mut fields = shared_fields.to_vec();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    trace_id = encode_hex(decode_length_delimited(bytes, &mut cursor)?);
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    span_id = encode_hex(decode_length_delimited(bytes, &mut cursor)?);
                }
                (4, WIRE_LENGTH_DELIMITED) => {
                    let parent = decode_length_delimited(bytes, &mut cursor)?;
                    if !parent.is_empty() {
                        parent_span_id = Some(encode_hex(parent));
                    }
                }
                (5, WIRE_LENGTH_DELIMITED) => {
                    name = decode_str(bytes, &mut cursor)?.to_owned();
                }
                (7, WIRE_FIXED64) => {
                    start_time_unix_nano = decode_fixed64(bytes, &mut cursor)?;
                }
                (8, WIRE_FIXED64) => {
                    end_time_unix_nano = decode_fixed64(bytes, &mut cursor)?;
                }
                (9, WIRE_LENGTH_DELIMITED) => {
                    let key_value = decode_length_delimited(bytes, &mut cursor)?;
                    if let Some(field) = self.decode_key_value(key_value, FieldPrefix::SpanAttr)? {
                        fields.push(field);
                    }
                }
                (15, WIRE_LENGTH_DELIMITED) => {
                    let status = decode_length_delimited(bytes, &mut cursor)?;
                    let decoded_status = self.decode_status(status)?;
                    fields.push(Field::new(
                        self.field_names.status_code(),
                        decoded_status.code,
                    ));
                    fields.push(Field::new(
                        self.field_names.status_message(),
                        decoded_status.message,
                    ));
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        TraceSpanRow::new_prevalidated_unsorted_fields(
            trace_id,
            span_id,
            parent_span_id,
            name,
            decode_unix_nano(start_time_unix_nano),
            decode_unix_nano(end_time_unix_nano),
            fields,
        )
        .map_err(ProtobufCodecError::from)
    }

    fn decode_status(&mut self, bytes: &[u8]) -> Result<DecodedStatus, ProtobufCodecError> {
        let mut code = 0i32;
        let mut message = self.field_values.empty();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (2, WIRE_LENGTH_DELIMITED) => {
                    message = self
                        .field_values
                        .normalized_borrowed(decode_str(bytes, &mut cursor)?);
                }
                (3, WIRE_VARINT) => {
                    code = decode_varint(bytes, &mut cursor)? as i32;
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        Ok(DecodedStatus {
            code: self.field_values.integer(i64::from(code)),
            message,
        })
    }

    fn decode_key_value(
        &mut self,
        bytes: &[u8],
        prefix: FieldPrefix,
    ) -> Result<Option<Field>, ProtobufCodecError> {
        let mut key = None;
        let mut value = None;
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    key = Some(decode_str(bytes, &mut cursor)?);
                }
                (2, WIRE_LENGTH_DELIMITED) => {
                    let any_value = decode_length_delimited(bytes, &mut cursor)?;
                    value = Some(self.decode_any_value(any_value)?);
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        let Some(key) = key else {
            return Ok(None);
        };

        Ok(Some(Field::new(
            self.field_names.prefixed(prefix, key),
            value.unwrap_or_else(|| self.field_values.empty()),
        )))
    }

    fn decode_any_value(&mut self, bytes: &[u8]) -> Result<Arc<str>, ProtobufCodecError> {
        let mut value = self.field_values.empty();
        let mut cursor = 0usize;

        while cursor < bytes.len() {
            let (field_number, wire_type) = decode_key(bytes, &mut cursor)?;
            match (field_number, wire_type) {
                (1, WIRE_LENGTH_DELIMITED) => {
                    value = self
                        .field_values
                        .normalized_borrowed(decode_str(bytes, &mut cursor)?);
                }
                (2, WIRE_VARINT) => {
                    value = self.field_values.boolean(decode_varint(bytes, &mut cursor)? != 0);
                }
                (3, WIRE_VARINT) => {
                    value = self
                        .field_values
                        .integer(decode_varint(bytes, &mut cursor)? as i64);
                }
                (4, WIRE_FIXED64) => {
                    value = self
                        .field_values
                        .double(f64::from_bits(decode_fixed64(bytes, &mut cursor)?));
                }
                (5, WIRE_LENGTH_DELIMITED) | (6, WIRE_LENGTH_DELIMITED) => {
                    let nested = decode_length_delimited(bytes, &mut cursor)?;
                    let json = decode_complex_any_value_json(field_number, nested)?;
                    value = self.field_values.normalized_owned(json.to_string());
                }
                (7, WIRE_LENGTH_DELIMITED) => {
                    value = self
                        .field_values
                        .normalized_owned(encode_hex(decode_length_delimited(bytes, &mut cursor)?));
                }
                _ => skip_field(bytes, &mut cursor, wire_type)?,
            }
        }

        Ok(value)
    }
}

#[derive(Default)]
struct DecodedInstrumentationScope {
    name: Option<Arc<str>>,
    version: Option<Arc<str>>,
    attributes: Vec<Field>,
}

struct DecodedStatus {
    code: Arc<str>,
    message: Arc<str>,
}

impl FieldPrefix {
    fn value(self) -> &'static str {
        match self {
            Self::ResourceAttr => "resource_attr:",
            Self::ScopeAttr => "scope_attr:",
            Self::SpanAttr => "span_attr:",
        }
    }
}

pub fn decode_export_trace_service_request_protobuf(
    bytes: &[u8],
) -> Result<ExportTraceServiceRequest, ProtobufCodecError> {
    let request = OtlpExportTraceServiceRequest::decode(bytes)?;
    Ok(convert_export_request(request))
}

pub fn decode_trace_rows_protobuf(bytes: &[u8]) -> Result<Vec<TraceSpanRow>, ProtobufCodecError> {
    FastTraceRowsDecoder::default().decode(bytes)
}

pub fn encode_export_trace_service_request_protobuf(
    request: &ExportTraceServiceRequest,
) -> Result<Vec<u8>, ProtobufCodecError> {
    let request = build_export_request(request)?;
    let mut bytes = Vec::with_capacity(request.encoded_len());
    request
        .encode(&mut bytes)
        .expect("encoding protobuf into vec should not fail");
    Ok(bytes)
}

fn convert_export_request(request: OtlpExportTraceServiceRequest) -> ExportTraceServiceRequest {
    ExportTraceServiceRequest {
        resource_spans: request
            .resource_spans
            .into_iter()
            .map(convert_resource_spans)
            .collect(),
    }
}

#[allow(dead_code)]
fn flatten_otlp_export_request(
    request: OtlpExportTraceServiceRequest,
) -> Result<Vec<TraceSpanRow>, ProtobufCodecError> {
    let mut field_names = FieldNameCache::default();
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

    for resource_span in request.resource_spans {
        let resource_fields = flatten_otlp_key_values(
            &resource_span
                .resource
                .map(|v| v.attributes)
                .unwrap_or_default(),
            FieldPrefix::ResourceAttr,
            &mut field_names,
        );

        for scope_spans in resource_span.scope_spans {
            let scope_attributes = scope_spans
                .scope
                .as_ref()
                .map(|scope| &scope.attributes)
                .map(|attrs| flatten_otlp_key_values(attrs, FieldPrefix::ScopeAttr, &mut field_names))
                .unwrap_or_default();
            let mut shared_fields = Vec::with_capacity(
                resource_fields.len()
                    + scope_attributes.len()
                    + usize::from(
                        scope_spans
                            .scope
                            .as_ref()
                            .map(|scope| !scope.name.is_empty())
                            .unwrap_or(false),
                    )
                    + usize::from(
                        scope_spans
                            .scope
                            .as_ref()
                            .map(|scope| !scope.version.is_empty())
                            .unwrap_or(false),
                    ),
            );
            shared_fields.extend(resource_fields.iter().cloned());
            if let Some(scope) = scope_spans.scope.as_ref() {
                if !scope.name.is_empty() {
                    shared_fields.push(Field::new(
                        field_names.instrumentation_scope_name(),
                        normalize_value(scope.name.clone()),
                    ));
                }
                if !scope.version.is_empty() {
                    shared_fields.push(Field::new(
                        field_names.instrumentation_scope_version(),
                        normalize_value(scope.version.clone()),
                    ));
                }
            }
            shared_fields.extend(scope_attributes);

            for span in scope_spans.spans {
                let mut fields = shared_fields.clone();
                fields.reserve(span.attributes.len() + 2);
                append_otlp_key_values(
                    &mut fields,
                    &span.attributes,
                    FieldPrefix::SpanAttr,
                    &mut field_names,
                );
                if let Some(status) = span.status.as_ref() {
                    fields.push(Field::new(field_names.status_code(), status.code.to_string()));
                    fields.push(Field::new(
                        field_names.status_message(),
                        normalize_value(status.message.clone()),
                    ));
                }

                rows.push(TraceSpanRow::new_unsorted_fields(
                    encode_hex(&span.trace_id),
                    encode_hex(&span.span_id),
                    if span.parent_span_id.is_empty() {
                        None
                    } else {
                        Some(encode_hex(&span.parent_span_id))
                    },
                    span.name,
                    decode_unix_nano(span.start_time_unix_nano),
                    decode_unix_nano(span.end_time_unix_nano),
                    fields,
                )?);
            }
        }
    }

    Ok(rows)
}

#[allow(dead_code)]
fn flatten_otlp_key_values(
    values: &[OtlpKeyValue],
    prefix: FieldPrefix,
    field_names: &mut FieldNameCache,
) -> Vec<Field> {
    let mut fields = Vec::with_capacity(values.len());
    append_otlp_key_values(&mut fields, values, prefix, field_names);
    fields
}

#[allow(dead_code)]
fn append_otlp_key_values(
    fields: &mut Vec<Field>,
    values: &[OtlpKeyValue],
    prefix: FieldPrefix,
    field_names: &mut FieldNameCache,
) {
    fields.reserve(values.len());
    for key_value in values {
        fields.push(Field::new(
            field_names.prefixed(prefix, key_value.key.as_str()),
            flatten_otlp_any_value(key_value.value.as_ref()),
        ));
    }
}

fn flatten_otlp_any_value(value: Option<&OtlpAnyValue>) -> String {
    match value.and_then(|value| value.value.as_ref()) {
        Some(otlp_any_value::Value::StringValue(value)) => normalize_value(value.clone()),
        Some(otlp_any_value::Value::BoolValue(value)) => value.to_string(),
        Some(otlp_any_value::Value::IntValue(value)) => value.to_string(),
        Some(otlp_any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(otlp_any_value::Value::ArrayValue(value)) => json_value_from_array_ref(value).to_string(),
        Some(otlp_any_value::Value::KvlistValue(value)) => {
            json_value_from_key_values_ref(&value.values).to_string()
        }
        Some(otlp_any_value::Value::BytesValue(value)) => encode_hex(value),
        None => normalize_value(String::new()),
    }
}

fn convert_resource_spans(resource_spans: OtlpResourceSpans) -> ResourceSpans {
    ResourceSpans {
        resource_attributes: resource_spans
            .resource
            .map(|resource| {
                resource
                    .attributes
                    .into_iter()
                    .map(convert_key_value)
                    .collect()
            })
            .unwrap_or_default(),
        scope_spans: resource_spans
            .scope_spans
            .into_iter()
            .map(convert_scope_spans)
            .collect(),
    }
}

fn convert_scope_spans(scope_spans: OtlpScopeSpans) -> ScopeSpans {
    let scope = scope_spans.scope;
    ScopeSpans {
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
        spans: scope_spans.spans.into_iter().map(convert_span).collect(),
    }
}

fn convert_span(span: OtlpSpan) -> SpanRecord {
    SpanRecord {
        trace_id: encode_hex(&span.trace_id),
        span_id: encode_hex(&span.span_id),
        parent_span_id: if span.parent_span_id.is_empty() {
            None
        } else {
            Some(encode_hex(&span.parent_span_id))
        },
        name: span.name,
        start_time_unix_nano: decode_unix_nano(span.start_time_unix_nano),
        end_time_unix_nano: decode_unix_nano(span.end_time_unix_nano),
        attributes: span.attributes.into_iter().map(convert_key_value).collect(),
        status: span.status.map(convert_status),
    }
}

fn convert_status(status: OtlpStatus) -> Status {
    Status {
        code: status.code,
        message: status.message,
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
    request: &ExportTraceServiceRequest,
) -> Result<OtlpExportTraceServiceRequest, ProtobufCodecError> {
    Ok(OtlpExportTraceServiceRequest {
        resource_spans: request
            .resource_spans
            .iter()
            .map(build_resource_spans)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn build_resource_spans(
    resource_spans: &ResourceSpans,
) -> Result<OtlpResourceSpans, ProtobufCodecError> {
    Ok(OtlpResourceSpans {
        resource: Some(OtlpResource {
            attributes: resource_spans
                .resource_attributes
                .iter()
                .map(build_key_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        scope_spans: resource_spans
            .scope_spans
            .iter()
            .map(build_scope_spans)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn build_scope_spans(scope_spans: &ScopeSpans) -> Result<OtlpScopeSpans, ProtobufCodecError> {
    Ok(OtlpScopeSpans {
        scope: Some(OtlpInstrumentationScope {
            name: scope_spans.scope_name.clone().unwrap_or_default(),
            version: scope_spans.scope_version.clone().unwrap_or_default(),
            attributes: scope_spans
                .scope_attributes
                .iter()
                .map(build_key_value)
                .collect::<Result<Vec<_>, _>>()?,
        }),
        spans: scope_spans
            .spans
            .iter()
            .map(build_span)
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn build_span(span: &SpanRecord) -> Result<OtlpSpan, ProtobufCodecError> {
    Ok(OtlpSpan {
        trace_id: decode_hex(span.trace_id.as_str(), "trace_id")?,
        span_id: decode_hex(span.span_id.as_str(), "span_id")?,
        parent_span_id: span
            .parent_span_id
            .as_deref()
            .map(|value| decode_hex(value, "parent_span_id"))
            .transpose()?
            .unwrap_or_default(),
        name: span.name.clone(),
        start_time_unix_nano: encode_unix_nano(span.start_time_unix_nano),
        end_time_unix_nano: encode_unix_nano(span.end_time_unix_nano),
        attributes: span
            .attributes
            .iter()
            .map(build_key_value)
            .collect::<Result<Vec<_>, _>>()?,
        status: span.status.as_ref().map(build_status),
    })
}

fn build_status(status: &Status) -> OtlpStatus {
    OtlpStatus {
        message: status.message.clone(),
        code: status.code,
    }
}

fn build_key_value(key_value: &KeyValue) -> Result<OtlpKeyValue, ProtobufCodecError> {
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

fn json_value_from_array_ref(value: &OtlpArrayValue) -> Value {
    Value::Array(
        value
            .values
            .iter()
            .map(json_value_from_any_value_ref)
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

fn json_value_from_any_value_ref(value: &OtlpAnyValue) -> Value {
    match value.value.as_ref() {
        Some(otlp_any_value::Value::StringValue(value)) => Value::String(value.clone()),
        Some(otlp_any_value::Value::BoolValue(value)) => Value::Bool(*value),
        Some(otlp_any_value::Value::IntValue(value)) => Value::Number(Number::from(*value)),
        Some(otlp_any_value::Value::DoubleValue(value)) => Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Some(otlp_any_value::Value::ArrayValue(value)) => json_value_from_array_ref(value),
        Some(otlp_any_value::Value::KvlistValue(value)) => {
            json_value_from_key_values_ref(&value.values)
        }
        Some(otlp_any_value::Value::BytesValue(value)) => Value::String(encode_hex(value)),
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

fn json_value_from_key_values_ref(values: &[OtlpKeyValue]) -> Value {
    let mut object = Map::new();
    for key_value in values {
        object.insert(
            key_value.key.clone(),
            key_value
                .value
                .as_ref()
                .map(json_value_from_any_value_ref)
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
        const HEX: &[u8; 16] = b"0123456789abcdef";
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}

fn prefixed_key(prefix: &str, key: &str) -> String {
    let mut output = String::with_capacity(prefix.len() + key.len());
    output.push_str(prefix);
    output.push_str(key);
    output
}

fn decode_key(bytes: &[u8], cursor: &mut usize) -> Result<(u32, u8), ProtobufCodecError> {
    let key = decode_varint(bytes, cursor)?;
    if key == 0 {
        return Err(protobuf_decode_error("invalid protobuf field key"));
    }

    Ok(((key >> 3) as u32, (key & 0x07) as u8))
}

fn decode_varint(bytes: &[u8], cursor: &mut usize) -> Result<u64, ProtobufCodecError> {
    let mut shift = 0u32;
    let mut value = 0u64;

    loop {
        if *cursor >= bytes.len() {
            return Err(protobuf_decode_error("unexpected EOF while decoding varint"));
        }
        let byte = bytes[*cursor];
        *cursor += 1;
        value |= u64::from(byte & 0x7f) << shift;

        if byte & 0x80 == 0 {
            return Ok(value);
        }

        shift += 7;
        if shift >= 64 {
            return Err(protobuf_decode_error("protobuf varint overflow"));
        }
    }
}

fn decode_length_delimited<'a>(
    bytes: &'a [u8],
    cursor: &mut usize,
) -> Result<&'a [u8], ProtobufCodecError> {
    let len = decode_varint(bytes, cursor)? as usize;
    let end = cursor
        .checked_add(len)
        .ok_or_else(|| protobuf_decode_error("protobuf length overflow"))?;
    if end > bytes.len() {
        return Err(protobuf_decode_error(
            "unexpected EOF while decoding length-delimited field",
        ));
    }

    let slice = &bytes[*cursor..end];
    *cursor = end;
    Ok(slice)
}

fn decode_str<'a>(bytes: &'a [u8], cursor: &mut usize) -> Result<&'a str, ProtobufCodecError> {
    let value = decode_length_delimited(bytes, cursor)?;
    std::str::from_utf8(value).map_err(|_| protobuf_decode_error("invalid UTF-8 string"))
}

#[allow(dead_code)]
fn decode_string(bytes: &[u8], cursor: &mut usize) -> Result<String, ProtobufCodecError> {
    Ok(decode_str(bytes, cursor)?.to_owned())
}

fn decode_fixed64(bytes: &[u8], cursor: &mut usize) -> Result<u64, ProtobufCodecError> {
    let end = cursor
        .checked_add(8)
        .ok_or_else(|| protobuf_decode_error("protobuf fixed64 overflow"))?;
    if end > bytes.len() {
        return Err(protobuf_decode_error(
            "unexpected EOF while decoding fixed64 field",
        ));
    }

    let mut fixed = [0u8; 8];
    fixed.copy_from_slice(&bytes[*cursor..end]);
    *cursor = end;
    Ok(u64::from_le_bytes(fixed))
}

fn skip_field(bytes: &[u8], cursor: &mut usize, wire_type: u8) -> Result<(), ProtobufCodecError> {
    match wire_type {
        WIRE_VARINT => {
            let _ = decode_varint(bytes, cursor)?;
        }
        WIRE_FIXED64 => {
            let _ = decode_fixed64(bytes, cursor)?;
        }
        WIRE_LENGTH_DELIMITED => {
            let _ = decode_length_delimited(bytes, cursor)?;
        }
        WIRE_FIXED32 => {
            let end = cursor
                .checked_add(4)
                .ok_or_else(|| protobuf_decode_error("protobuf fixed32 overflow"))?;
            if end > bytes.len() {
                return Err(protobuf_decode_error(
                    "unexpected EOF while decoding fixed32 field",
                ));
            }
            *cursor = end;
        }
        _ => return Err(protobuf_decode_error("unsupported protobuf wire type")),
    }

    Ok(())
}

fn decode_complex_any_value_json(
    field_number: u32,
    bytes: &[u8],
) -> Result<Value, ProtobufCodecError> {
    match field_number {
        5 => {
            let array = OtlpArrayValue::decode(bytes)?;
            Ok(json_value_from_array_ref(&array))
        }
        6 => {
            let key_values = OtlpKeyValueList::decode(bytes)?;
            Ok(json_value_from_key_values_ref(&key_values.values))
        }
        _ => Err(protobuf_decode_error("unsupported OTLP AnyValue variant")),
    }
}

fn protobuf_decode_error(message: &'static str) -> ProtobufCodecError {
    ProtobufCodecError::Decode(prost::DecodeError::new(message))
}

fn decode_hex(value: &str, field: &'static str) -> Result<Vec<u8>, ProtobufCodecError> {
    if let Ok(decoded) = try_decode_hex(value, field) {
        return Ok(decoded);
    }

    Ok(fallback_id_bytes(
        value,
        if field == "trace_id" { 16 } else { 8 },
    ))
}

fn try_decode_hex(value: &str, field: &'static str) -> Result<Vec<u8>, ProtobufCodecError> {
    let bytes = value.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(ProtobufCodecError::InvalidHexId {
            field,
            value: value.to_string(),
        });
    }

    let mut output = Vec::with_capacity(bytes.len() / 2);
    let mut index = 0usize;
    while index < bytes.len() {
        let high =
            decode_hex_nibble(bytes[index]).ok_or_else(|| ProtobufCodecError::InvalidHexId {
                field,
                value: value.to_string(),
            })?;
        let low = decode_hex_nibble(bytes[index + 1]).ok_or_else(|| {
            ProtobufCodecError::InvalidHexId {
                field,
                value: value.to_string(),
            }
        })?;
        output.push((high << 4) | low);
        index += 2;
    }
    Ok(output)
}

fn fallback_id_bytes(value: &str, len: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(len);
    let mut seed = 0xcbf29ce484222325u64;
    let mut round = 0u64;

    while output.len() < len {
        let mut hash = seed ^ round;
        for byte in value.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }

        for byte in hash.to_be_bytes() {
            if output.len() == len {
                break;
            }
            output.push(byte);
        }

        seed = seed.rotate_left(9) ^ 0x9e3779b97f4a7c15;
        round = round.wrapping_add(1);
    }

    output
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
struct OtlpExportTraceServiceRequest {
    #[prost(message, repeated, tag = "1")]
    resource_spans: Vec<OtlpResourceSpans>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpResourceSpans {
    #[prost(message, optional, tag = "1")]
    resource: Option<OtlpResource>,
    #[prost(message, repeated, tag = "3")]
    scope_spans: Vec<OtlpScopeSpans>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpResource {
    #[prost(message, repeated, tag = "1")]
    attributes: Vec<OtlpKeyValue>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpScopeSpans {
    #[prost(message, optional, tag = "1")]
    scope: Option<OtlpInstrumentationScope>,
    #[prost(message, repeated, tag = "2")]
    spans: Vec<OtlpSpan>,
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
struct OtlpSpan {
    #[prost(bytes = "vec", tag = "1")]
    trace_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    span_id: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    parent_span_id: Vec<u8>,
    #[prost(string, tag = "5")]
    name: String,
    #[prost(fixed64, tag = "7")]
    start_time_unix_nano: u64,
    #[prost(fixed64, tag = "8")]
    end_time_unix_nano: u64,
    #[prost(message, repeated, tag = "9")]
    attributes: Vec<OtlpKeyValue>,
    #[prost(message, optional, tag = "15")]
    status: Option<OtlpStatus>,
}

#[derive(Clone, PartialEq, Message)]
struct OtlpStatus {
    #[prost(string, tag = "2")]
    message: String,
    #[prost(int32, tag = "3")]
    code: i32,
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
