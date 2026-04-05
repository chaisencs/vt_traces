use vtcore::{Field, LogRow, LogSearchRequest, TraceBlock, TraceSpanRow};
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
fn memory_engine_tracks_trace_window_across_appends() {
    let engine = MemoryStorageEngine::new();
    engine
        .append_rows(vec![
            make_row("trace-1", "span-1", 100, 150),
            make_row("trace-1", "span-2", 90, 175),
        ])
        .expect("append rows");

    let window = engine
        .trace_window("trace-1")
        .expect("trace window should exist");

    assert_eq!(window.start_unix_nano, 90);
    assert_eq!(window.end_unix_nano, 175);
}

#[test]
fn memory_engine_returns_rows_in_time_window() {
    let engine = MemoryStorageEngine::new();
    engine
        .append_rows(vec![
            make_row("trace-2", "span-1", 100, 150),
            make_row("trace-2", "span-2", 160, 170),
            make_row("trace-2", "span-3", 300, 320),
        ])
        .expect("append rows");

    let rows = engine.rows_for_trace("trace-2", 120, 200);
    let span_ids: Vec<&str> = rows.iter().map(|row| row.span_id.as_str()).collect();

    assert_eq!(span_ids, vec!["span-1", "span-2"]);
}

#[test]
fn memory_engine_keeps_rows_sorted_by_end_time() {
    let engine = MemoryStorageEngine::new();
    engine
        .append_rows(vec![
            make_row("trace-3", "span-3", 300, 350),
            make_row("trace-3", "span-1", 100, 150),
            make_row("trace-3", "span-2", 200, 250),
        ])
        .expect("append rows");

    let rows = engine.rows_for_trace("trace-3", 0, 1_000);
    let end_times: Vec<i64> = rows.iter().map(|row| row.end_unix_nano).collect();

    assert_eq!(end_times, vec![150, 250, 350]);
}

#[test]
fn memory_engine_accepts_trace_blocks_without_row_ingest_path() {
    let engine = MemoryStorageEngine::new();
    let block = TraceBlock::from_rows(vec![
        make_row("trace-4", "span-3", 300, 350),
        make_row("trace-4", "span-1", 100, 150),
        make_row("trace-4", "span-2", 200, 250),
    ]);

    engine
        .append_trace_block(block)
        .expect("append trace block");

    let rows = engine.rows_for_trace("trace-4", 0, 1_000);
    let end_times: Vec<i64> = rows.iter().map(|row| row.end_unix_nano).collect();

    assert_eq!(end_times, vec![150, 250, 350]);
}

#[test]
fn memory_engine_searches_logs_without_trace_row_conversion() {
    let engine = MemoryStorageEngine::new();
    engine
        .append_logs(vec![
            LogRow::new(
                "log-1",
                100,
                Some(110),
                Some(9),
                Some("INFO".to_string()),
                "checkout complete",
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
        .expect("append logs");

    let rows = engine.search_logs(&LogSearchRequest {
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

#[test]
fn memory_engine_with_multiple_trace_shards_preserves_cross_trace_queries() {
    let engine = MemoryStorageEngine::with_trace_shards(4);
    engine
        .append_rows(vec![
            make_row("trace-shard-a", "span-1", 100, 150),
            make_row("trace-shard-b", "span-1", 200, 250),
            make_row("trace-shard-c", "span-1", 300, 350),
            make_row("trace-shard-d", "span-1", 400, 450),
        ])
        .expect("append rows");

    let services = engine.list_services();
    assert_eq!(services, vec!["checkout".to_string()]);

    let trace_ids = engine.list_trace_ids();
    assert_eq!(
        trace_ids,
        vec![
            "trace-shard-a".to_string(),
            "trace-shard-b".to_string(),
            "trace-shard-c".to_string(),
            "trace-shard-d".to_string(),
        ]
    );
}
