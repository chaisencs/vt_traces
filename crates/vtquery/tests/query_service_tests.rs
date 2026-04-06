use std::sync::Arc;

use vtcore::{Field, FieldFilter, LogRow, LogSearchRequest, TraceSearchRequest, TraceSpanRow};
use vtquery::QueryService;
use vtstorage::{MemoryStorageEngine, StorageEngine};

fn make_row(trace_id: &str, span_id: &str, start: i64, end: i64) -> TraceSpanRow {
    TraceSpanRow::new(
        trace_id.to_string(),
        span_id.to_string(),
        None,
        format!("span-{span_id}"),
        start,
        end,
        vec![Field::new("resource_attr:service.name", "checkout")],
    )
    .expect("valid row")
}

#[test]
fn query_service_returns_complete_trace_in_time_order() {
    let engine = Arc::new(MemoryStorageEngine::new());
    engine
        .append_rows(vec![
            make_row("trace-1", "span-2", 200, 250),
            make_row("trace-1", "span-1", 100, 150),
        ])
        .expect("append rows");

    let service = QueryService::new(engine);
    let rows = service.get_trace("trace-1");
    let span_ids: Vec<&str> = rows.iter().map(|row| row.span_id.as_str()).collect();

    assert_eq!(span_ids, vec!["span-1", "span-2"]);
}

#[test]
fn query_service_returns_empty_for_unknown_trace() {
    let engine = Arc::new(MemoryStorageEngine::new());
    let service = QueryService::new(engine);

    assert!(service.get_trace("missing-trace").is_empty());
}

#[test]
fn query_service_checks_trace_window_before_row_scan() {
    let engine = Arc::new(MemoryStorageEngine::new());
    engine
        .append_rows(vec![make_row("trace-2", "span-1", 100, 150)])
        .expect("append rows");

    let service = QueryService::new(engine.clone());
    let _ = service.get_trace("trace-2");

    assert_eq!(engine.trace_window_lookups(), 1);
    assert_eq!(engine.row_queries(), 1);
}

#[test]
fn query_service_lists_services_in_sorted_order() {
    let engine = Arc::new(MemoryStorageEngine::new());
    engine
        .append_rows(vec![
            TraceSpanRow::new(
                "trace-svc-1",
                "span-1",
                None,
                "span-1",
                1,
                2,
                vec![Field::new("resource_attr:service.name", "payments")],
            )
            .expect("row"),
            TraceSpanRow::new(
                "trace-svc-2",
                "span-1",
                None,
                "span-1",
                3,
                4,
                vec![Field::new("resource_attr:service.name", "checkout")],
            )
            .expect("row"),
        ])
        .expect("append rows");

    let service = QueryService::new(engine);
    let services = service.list_services();

    assert_eq!(
        services,
        vec!["checkout".to_string(), "payments".to_string()]
    );
}

#[test]
fn query_service_searches_trace_ids_by_time_range_and_service() {
    let engine = Arc::new(MemoryStorageEngine::new());
    engine
        .append_rows(vec![
            TraceSpanRow::new(
                "trace-search-1",
                "span-1",
                None,
                "span-1",
                100,
                120,
                vec![Field::new("resource_attr:service.name", "checkout")],
            )
            .expect("row"),
            TraceSpanRow::new(
                "trace-search-2",
                "span-1",
                None,
                "span-1",
                200,
                250,
                vec![Field::new("resource_attr:service.name", "payments")],
            )
            .expect("row"),
        ])
        .expect("append rows");

    let service = QueryService::new(engine);
    let hits = service.search_traces(&TraceSearchRequest {
        start_unix_nano: 50,
        end_unix_nano: 150,
        service_name: Some("checkout".to_string()),
        operation_name: None,
        field_filters: Vec::new(),
        limit: 20,
    });

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].trace_id, "trace-search-1");
    assert_eq!(hits[0].services, vec!["checkout".to_string()]);
}

#[test]
fn query_service_searches_trace_ids_by_operation_name() {
    let engine = Arc::new(MemoryStorageEngine::new());
    engine
        .append_rows(vec![
            TraceSpanRow::new(
                "trace-op-1",
                "span-1",
                None,
                "GET /checkout",
                100,
                120,
                vec![Field::new("resource_attr:service.name", "checkout")],
            )
            .expect("row"),
            TraceSpanRow::new(
                "trace-op-2",
                "span-1",
                None,
                "POST /checkout",
                110,
                130,
                vec![Field::new("resource_attr:service.name", "checkout")],
            )
            .expect("row"),
        ])
        .expect("append rows");

    let service = QueryService::new(engine);
    let hits = service.search_traces(&TraceSearchRequest {
        start_unix_nano: 50,
        end_unix_nano: 150,
        service_name: Some("checkout".to_string()),
        operation_name: Some("POST /checkout".to_string()),
        field_filters: Vec::new(),
        limit: 20,
    });

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].trace_id, "trace-op-2");
}

#[test]
fn query_service_searches_trace_ids_by_generic_field_filter() {
    let engine = Arc::new(MemoryStorageEngine::new());
    engine
        .append_rows(vec![
            TraceSpanRow::new(
                "trace-field-1",
                "span-1",
                None,
                "GET /checkout",
                100,
                120,
                vec![
                    Field::new("resource_attr:service.name", "checkout"),
                    Field::new("span_attr:http.method", "GET"),
                    Field::new("status_code", "0"),
                ],
            )
            .expect("row"),
            TraceSpanRow::new(
                "trace-field-2",
                "span-1",
                None,
                "POST /checkout",
                110,
                130,
                vec![
                    Field::new("resource_attr:service.name", "checkout"),
                    Field::new("span_attr:http.method", "POST"),
                    Field::new("status_code", "2"),
                ],
            )
            .expect("row"),
        ])
        .expect("append rows");

    let service = QueryService::new(engine);
    let hits = service.search_traces(&TraceSearchRequest {
        start_unix_nano: 50,
        end_unix_nano: 150,
        service_name: Some("checkout".to_string()),
        operation_name: None,
        field_filters: vec![
            FieldFilter {
                name: "span_attr:http.method".to_string(),
                value: "POST".to_string(),
            },
            FieldFilter {
                name: "status_code".to_string(),
                value: "2".to_string(),
            },
        ],
        limit: 20,
    });

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].trace_id, "trace-field-2");
}

#[test]
fn query_service_searches_logs_by_service_and_severity() {
    let engine = Arc::new(MemoryStorageEngine::new());
    engine
        .append_logs(vec![
            LogRow::new(
                "log-1",
                100,
                Some(110),
                Some(9),
                Some("INFO".to_string()),
                "checkout completed",
                None,
                None,
                vec![Field::new("resource_attr:service.name", "checkout")],
            ),
            LogRow::new(
                "log-2",
                200,
                Some(210),
                Some(17),
                Some("ERROR".to_string()),
                "payment declined",
                None,
                None,
                vec![Field::new("resource_attr:service.name", "payments")],
            ),
        ])
        .expect("append log rows");

    let service = QueryService::new(engine);
    let rows = service.search_logs(&LogSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 1_000,
        service_name: Some("payments".to_string()),
        severity_text: Some("ERROR".to_string()),
        field_filters: Vec::new(),
        limit: 10,
    });

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].log_id, "log-2");
    assert_eq!(rows[0].body, "payment declined");
}
