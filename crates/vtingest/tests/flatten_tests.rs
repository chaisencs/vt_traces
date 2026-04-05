use vtingest::{
    flatten_export_logs_request, flatten_export_request, AttributeValue, ExportLogsServiceRequest,
    ExportTraceServiceRequest, KeyValue, LogRecord, ResourceLogs, ResourceSpans, ScopeLogs,
    ScopeSpans, SpanRecord, Status,
};

#[test]
fn flatten_export_request_materializes_core_trace_fields() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::string("service.name", "checkout")],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.auto".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: vec![KeyValue::string("scope.flag", "enabled")],
                spans: vec![SpanRecord {
                    trace_id: "trace-1".to_string(),
                    span_id: "span-1".to_string(),
                    parent_span_id: Some("parent-1".to_string()),
                    name: "GET /cart".to_string(),
                    start_time_unix_nano: 100,
                    end_time_unix_nano: 250,
                    attributes: vec![KeyValue::string("http.method", "GET")],
                    status: Some(Status {
                        code: 0,
                        message: "OK".to_string(),
                    }),
                }],
            }],
        }],
    };

    let rows = flatten_export_request(&request).expect("flattening should succeed");
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row.trace_id, "trace-1");
    assert_eq!(row.duration_nanos(), 150);
    assert_eq!(
        row.field_value("resource_attr:service.name").as_deref(),
        Some("checkout")
    );
    assert_eq!(
        row.field_value("scope_attr:scope.flag").as_deref(),
        Some("enabled")
    );
    assert_eq!(
        row.field_value("span_attr:http.method").as_deref(),
        Some("GET")
    );
    assert_eq!(row.field_value("status_code").as_deref(), Some("0"));
    assert_eq!(row.field_value("status_message").as_deref(), Some("OK"));
}

#[test]
fn flatten_export_request_replaces_empty_values_with_dash() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::string("service.name", "")],
            scope_spans: vec![ScopeSpans {
                scope_name: None,
                scope_version: None,
                scope_attributes: vec![],
                spans: vec![SpanRecord {
                    trace_id: "trace-2".to_string(),
                    span_id: "span-2".to_string(),
                    parent_span_id: None,
                    name: "empty".to_string(),
                    start_time_unix_nano: 10,
                    end_time_unix_nano: 20,
                    attributes: vec![KeyValue::string("db.statement", "")],
                    status: None,
                }],
            }],
        }],
    };

    let rows = flatten_export_request(&request).expect("flattening should succeed");
    let row = &rows[0];

    assert_eq!(
        row.field_value("resource_attr:service.name").as_deref(),
        Some("-")
    );
    assert_eq!(
        row.field_value("span_attr:db.statement").as_deref(),
        Some("-")
    );
}

#[test]
fn flatten_export_request_stringifies_non_string_attribute_values() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("payments".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: None,
                scope_version: None,
                scope_attributes: vec![],
                spans: vec![SpanRecord {
                    trace_id: "trace-3".to_string(),
                    span_id: "span-3".to_string(),
                    parent_span_id: None,
                    name: "bools-and-ints".to_string(),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 3,
                    attributes: vec![
                        KeyValue::new("retry", AttributeValue::Bool(true)),
                        KeyValue::new("http.status_code", AttributeValue::I64(500)),
                    ],
                    status: None,
                }],
            }],
        }],
    };

    let rows = flatten_export_request(&request).expect("flattening should succeed");
    let row = &rows[0];

    assert_eq!(row.field_value("span_attr:retry").as_deref(), Some("true"));
    assert_eq!(
        row.field_value("span_attr:http.status_code").as_deref(),
        Some("500")
    );
}

#[test]
fn flatten_logs_request_materializes_core_log_fields() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource_attributes: vec![KeyValue::string("service.name", "checkout")],
            scope_logs: vec![ScopeLogs {
                scope_name: Some("io.opentelemetry.logs".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: vec![KeyValue::string("scope.env", "prod")],
                log_records: vec![LogRecord {
                    time_unix_nano: 100,
                    observed_time_unix_nano: Some(110),
                    severity_number: Some(9),
                    severity_text: Some("INFO".to_string()),
                    body: AttributeValue::String("checkout completed".to_string()),
                    trace_id: Some("00112233445566778899aabbccddeeff".to_string()),
                    span_id: Some("0123456789abcdef".to_string()),
                    attributes: vec![KeyValue::string("http.method", "POST")],
                }],
            }],
        }],
    };

    let rows = flatten_export_logs_request(&request).expect("log flattening should succeed");
    assert_eq!(rows.len(), 1);

    let row = &rows[0];
    assert_eq!(row.body, "checkout completed");
    assert_eq!(row.time_unix_nano, 100);
    assert_eq!(row.observed_time_unix_nano, Some(110));
    assert_eq!(row.severity_number, Some(9));
    assert_eq!(row.severity_text.as_deref(), Some("INFO"));
    assert_eq!(
        row.field_value("resource_attr:service.name").as_deref(),
        Some("checkout")
    );
    assert_eq!(
        row.field_value("scope_attr:scope.env").as_deref(),
        Some("prod")
    );
    assert_eq!(
        row.field_value("log_attr:http.method").as_deref(),
        Some("POST")
    );
}
