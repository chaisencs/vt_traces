use std::sync::Arc;

use vtingest::{
    decode_export_logs_service_request_protobuf, decode_export_trace_service_request_protobuf,
    decode_trace_rows_protobuf, encode_export_logs_service_request_protobuf,
    encode_export_trace_service_request_protobuf, flatten_export_request, AttributeValue,
    ExportLogsServiceRequest, ExportTraceServiceRequest, KeyValue, LogRecord, ResourceLogs,
    ResourceSpans, ScopeLogs, ScopeSpans, SpanRecord, Status,
};

#[test]
fn protobuf_round_trip_preserves_core_trace_fields() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.rust".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: vec![KeyValue::new(
                    "scope.mode",
                    AttributeValue::String("prod".to_string()),
                )],
                spans: vec![SpanRecord {
                    trace_id: "00112233445566778899aabbccddeeff".to_string(),
                    span_id: "0123456789abcdef".to_string(),
                    parent_span_id: Some("1111111111111111".to_string()),
                    name: "GET /checkout".to_string(),
                    start_time_unix_nano: 100,
                    end_time_unix_nano: 180,
                    attributes: vec![
                        KeyValue::new("http.method", AttributeValue::String("GET".to_string())),
                        KeyValue::new("retry", AttributeValue::Bool(true)),
                    ],
                    status: Some(Status {
                        code: 0,
                        message: "OK".to_string(),
                    }),
                }],
            }],
        }],
    };

    let encoded =
        encode_export_trace_service_request_protobuf(&request).expect("encode protobuf request");
    let decoded =
        decode_export_trace_service_request_protobuf(&encoded).expect("decode protobuf request");

    assert_eq!(decoded, request);
}

#[test]
fn protobuf_encode_falls_back_for_non_hex_trace_ids() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: Vec::new(),
            scope_spans: vec![ScopeSpans {
                scope_name: None,
                scope_version: None,
                scope_attributes: Vec::new(),
                spans: vec![SpanRecord {
                    trace_id: "trace-not-hex".to_string(),
                    span_id: "0123456789abcdef".to_string(),
                    parent_span_id: None,
                    name: "broken".to_string(),
                    start_time_unix_nano: 1,
                    end_time_unix_nano: 2,
                    attributes: Vec::new(),
                    status: None,
                }],
            }],
        }],
    };

    let encoded =
        encode_export_trace_service_request_protobuf(&request).expect("non-hex IDs should encode");
    let decoded =
        decode_export_trace_service_request_protobuf(&encoded).expect("protobuf should decode");

    let decoded_span = &decoded.resource_spans[0].scope_spans[0].spans[0];
    assert_eq!(decoded_span.trace_id.len(), 32);
    assert_eq!(decoded_span.span_id, "0123456789abcdef");
    assert_ne!(decoded_span.trace_id, "trace-not-hex");
}

#[test]
fn logs_protobuf_round_trip_preserves_core_log_fields() {
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_logs: vec![ScopeLogs {
                scope_name: Some("io.opentelemetry.logs".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: vec![KeyValue::new(
                    "scope.mode",
                    AttributeValue::String("prod".to_string()),
                )],
                log_records: vec![LogRecord {
                    time_unix_nano: 100,
                    observed_time_unix_nano: Some(120),
                    severity_number: Some(13),
                    severity_text: Some("WARN".to_string()),
                    body: AttributeValue::String("inventory low".to_string()),
                    trace_id: Some("00112233445566778899aabbccddeeff".to_string()),
                    span_id: Some("0123456789abcdef".to_string()),
                    attributes: vec![
                        KeyValue::new("store.id", AttributeValue::String("1001".to_string())),
                        KeyValue::new("retry", AttributeValue::Bool(true)),
                    ],
                }],
            }],
        }],
    };

    let encoded =
        encode_export_logs_service_request_protobuf(&request).expect("encode protobuf request");
    let decoded =
        decode_export_logs_service_request_protobuf(&encoded).expect("decode protobuf request");

    assert_eq!(decoded, request);
}

#[test]
fn protobuf_fast_path_decodes_rows_equivalent_to_flattening_request() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.rust".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: vec![KeyValue::new(
                    "scope.mode",
                    AttributeValue::String("prod".to_string()),
                )],
                spans: vec![SpanRecord {
                    trace_id: "00112233445566778899aabbccddeeff".to_string(),
                    span_id: "0123456789abcdef".to_string(),
                    parent_span_id: None,
                    name: "POST /protobuf".to_string(),
                    start_time_unix_nano: 100,
                    end_time_unix_nano: 180,
                    attributes: vec![
                        KeyValue::new("http.method", AttributeValue::String("POST".to_string())),
                        KeyValue::new("retry", AttributeValue::Bool(true)),
                    ],
                    status: Some(Status {
                        code: 0,
                        message: "OK".to_string(),
                    }),
                }],
            }],
        }],
    };

    let encoded =
        encode_export_trace_service_request_protobuf(&request).expect("encode protobuf request");
    let fast_rows = decode_trace_rows_protobuf(&encoded).expect("fast path rows");
    let flattened = flatten_export_request(&request).expect("flatten request");

    assert_eq!(fast_rows, flattened);
}

#[test]
fn protobuf_fast_path_reuses_field_name_arcs_for_repeated_keys() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.rust".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: Vec::new(),
                spans: vec![
                    SpanRecord {
                        trace_id: "00112233445566778899aabbccddeeff".to_string(),
                        span_id: "0123456789abcdef".to_string(),
                        parent_span_id: None,
                        name: "POST /protobuf".to_string(),
                        start_time_unix_nano: 100,
                        end_time_unix_nano: 180,
                        attributes: vec![
                            KeyValue::new(
                                "http.method",
                                AttributeValue::String("POST".to_string()),
                            ),
                            KeyValue::new("retry", AttributeValue::Bool(true)),
                        ],
                        status: Some(Status {
                            code: 0,
                            message: "OK".to_string(),
                        }),
                    },
                    SpanRecord {
                        trace_id: "00112233445566778899aabbccddeeff".to_string(),
                        span_id: "fedcba9876543210".to_string(),
                        parent_span_id: None,
                        name: "POST /protobuf-2".to_string(),
                        start_time_unix_nano: 200,
                        end_time_unix_nano: 260,
                        attributes: vec![
                            KeyValue::new(
                                "http.method",
                                AttributeValue::String("POST".to_string()),
                            ),
                            KeyValue::new("retry", AttributeValue::Bool(false)),
                        ],
                        status: Some(Status {
                            code: 0,
                            message: "OK".to_string(),
                        }),
                    },
                ],
            }],
        }],
    };

    let encoded =
        encode_export_trace_service_request_protobuf(&request).expect("encode protobuf request");
    let rows = decode_trace_rows_protobuf(&encoded).expect("fast path rows");

    let service_name_1 = rows[0]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "resource_attr:service.name")
        .expect("resource service field")
        .name
        .clone();
    let service_name_2 = rows[1]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "resource_attr:service.name")
        .expect("resource service field")
        .name
        .clone();
    let http_method_1 = rows[0]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "span_attr:http.method")
        .expect("http.method field")
        .name
        .clone();
    let http_method_2 = rows[1]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "span_attr:http.method")
        .expect("http.method field")
        .name
        .clone();

    assert!(Arc::ptr_eq(&service_name_1, &service_name_2));
    assert!(Arc::ptr_eq(&http_method_1, &http_method_2));
}

#[test]
fn protobuf_fast_path_reuses_field_value_arcs_for_repeated_strings() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.rust".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: Vec::new(),
                spans: vec![
                    SpanRecord {
                        trace_id: "00112233445566778899aabbccddeeff".to_string(),
                        span_id: "0123456789abcdef".to_string(),
                        parent_span_id: None,
                        name: "POST /protobuf".to_string(),
                        start_time_unix_nano: 100,
                        end_time_unix_nano: 180,
                        attributes: vec![
                            KeyValue::new(
                                "http.method",
                                AttributeValue::String("POST".to_string()),
                            ),
                            KeyValue::new("retry", AttributeValue::Bool(true)),
                        ],
                        status: Some(Status {
                            code: 0,
                            message: "OK".to_string(),
                        }),
                    },
                    SpanRecord {
                        trace_id: "00112233445566778899aabbccddeeff".to_string(),
                        span_id: "fedcba9876543210".to_string(),
                        parent_span_id: None,
                        name: "POST /protobuf-2".to_string(),
                        start_time_unix_nano: 200,
                        end_time_unix_nano: 260,
                        attributes: vec![
                            KeyValue::new(
                                "http.method",
                                AttributeValue::String("POST".to_string()),
                            ),
                            KeyValue::new("retry", AttributeValue::Bool(true)),
                        ],
                        status: Some(Status {
                            code: 0,
                            message: "OK".to_string(),
                        }),
                    },
                ],
            }],
        }],
    };

    let encoded =
        encode_export_trace_service_request_protobuf(&request).expect("encode protobuf request");
    let rows = decode_trace_rows_protobuf(&encoded).expect("fast path rows");

    let service_value_1 = rows[0]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "resource_attr:service.name")
        .expect("resource service field")
        .value
        .clone();
    let service_value_2 = rows[1]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "resource_attr:service.name")
        .expect("resource service field")
        .value
        .clone();
    let method_value_1 = rows[0]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "span_attr:http.method")
        .expect("http.method field")
        .value
        .clone();
    let method_value_2 = rows[1]
        .fields
        .iter()
        .find(|field| field.name.as_ref() == "span_attr:http.method")
        .expect("http.method field")
        .value
        .clone();

    assert!(Arc::ptr_eq(&service_value_1, &service_value_2));
    assert!(Arc::ptr_eq(&method_value_1, &method_value_2));
}

#[test]
fn protobuf_fast_path_decodes_nested_any_values_equivalent_to_flattening_request() {
    let request = ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource_attributes: vec![KeyValue::new(
                "service.name",
                AttributeValue::String("checkout".to_string()),
            )],
            scope_spans: vec![ScopeSpans {
                scope_name: Some("io.opentelemetry.rust".to_string()),
                scope_version: Some("1.0.0".to_string()),
                scope_attributes: Vec::new(),
                spans: vec![SpanRecord {
                    trace_id: "00112233445566778899aabbccddeeff".to_string(),
                    span_id: "0123456789abcdef".to_string(),
                    parent_span_id: None,
                    name: "POST /protobuf".to_string(),
                    start_time_unix_nano: 100,
                    end_time_unix_nano: 180,
                    attributes: vec![
                        KeyValue::new(
                            "http.request.headers",
                            AttributeValue::String(
                                "{\"accept\":\"application/json\",\"x-env\":\"prod\"}".to_string(),
                            ),
                        ),
                        KeyValue::new(
                            "http.retry.sequence",
                            AttributeValue::String("[1,true,\"again\"]".to_string()),
                        ),
                    ],
                    status: Some(Status {
                        code: 0,
                        message: "OK".to_string(),
                    }),
                }],
            }],
        }],
    };

    let encoded =
        encode_export_trace_service_request_protobuf(&request).expect("encode protobuf request");
    let fast_rows = decode_trace_rows_protobuf(&encoded).expect("fast path rows");
    let flattened = flatten_export_request(&request).expect("flatten request");

    assert_eq!(fast_rows, flattened);
}
