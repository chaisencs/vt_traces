use vtcore::{Field, LogRow, TraceSpanRow, TraceWindow};

#[test]
fn trace_span_row_normalizes_core_fields_and_duration() {
    let row = TraceSpanRow::new(
        "trace-1",
        "span-1",
        Some("parent-1".to_string()),
        "checkout",
        1_000,
        1_250,
        vec![
            Field::new("resource_attr:service.name", "cart"),
            Field::new("status_code", "0"),
        ],
    )
    .expect("row should be created");

    assert_eq!(row.trace_id, "trace-1");
    assert_eq!(row.span_id, "span-1");
    assert_eq!(row.parent_span_id.as_deref(), Some("parent-1"));
    assert_eq!(row.time_unix_nano, 1_250);
    assert_eq!(row.duration_nanos(), 250);
    assert_eq!(row.field_value("name").as_deref(), Some("checkout"));
    assert_eq!(row.field_value("trace_id").as_deref(), Some("trace-1"));
    assert_eq!(row.field_value("span_id").as_deref(), Some("span-1"));
    assert_eq!(row.field_value("_time").as_deref(), Some("1250"));
    assert_eq!(
        row.fields
            .iter()
            .filter(|field| matches!(
                field.name.as_ref(),
                "name"
                    | "trace_id"
                    | "span_id"
                    | "parent_span_id"
                    | "_time"
                    | "duration"
                    | "end_time_unix_nano"
                    | "start_time_unix_nano"
            ))
            .count(),
        0
    );
}

#[test]
fn trace_span_row_sorts_user_fields_stably() {
    let row = TraceSpanRow::new(
        "trace-2",
        "span-2",
        None,
        "payment",
        10,
        20,
        vec![
            Field::new("z_field", "z"),
            Field::new("a_field", "a"),
            Field::new("m_field", "m"),
        ],
    )
    .expect("row should be created");

    let custom_field_names: Vec<&str> = row
        .fields
        .iter()
        .filter(|field| field.name.ends_with("_field"))
        .map(|field| field.name.as_ref())
        .collect();

    assert_eq!(custom_field_names, vec!["a_field", "m_field", "z_field"]);
}

#[test]
fn trace_span_row_prevalidated_constructor_preserves_unsorted_fields() {
    let row = TraceSpanRow::new_prevalidated_unsorted_fields(
        "trace-3",
        "span-3",
        None,
        "payment",
        10,
        20,
        vec![
            Field::new("z_field", "z"),
            Field::new("a_field", "a"),
            Field::new("m_field", "m"),
        ],
    )
    .expect("row should be created");

    let custom_field_names: Vec<&str> = row
        .fields
        .iter()
        .map(|field| field.name.as_ref())
        .collect();

    assert_eq!(custom_field_names, vec!["z_field", "a_field", "m_field"]);
}

#[test]
fn trace_window_expands_to_cover_multiple_spans() {
    let mut window = TraceWindow::new("trace-1", 500, 700);
    window.observe(450, 800);
    window.observe(470, 750);

    assert_eq!(window.trace_id, "trace-1");
    assert_eq!(window.start_unix_nano, 450);
    assert_eq!(window.end_unix_nano, 800);
}

#[test]
fn log_row_round_trips_through_trace_row_encoding() {
    let log = LogRow::new(
        "log-1",
        1_500,
        Some(1_550),
        Some(9),
        Some("INFO".to_string()),
        "cart checkout completed",
        Some("trace-context-1".to_string()),
        Some("span-context-1".to_string()),
        vec![
            Field::new("resource_attr:service.name", "checkout"),
            Field::new("log_attr:http.method", "POST"),
        ],
    );

    let encoded = log
        .to_trace_row()
        .expect("log row should encode into a trace-compatible row");
    let decoded = LogRow::from_trace_row(&encoded).expect("encoded row should decode as log row");

    assert_eq!(decoded.log_id, "log-1");
    assert_eq!(decoded.time_unix_nano, 1_500);
    assert_eq!(decoded.observed_time_unix_nano, Some(1_550));
    assert_eq!(decoded.severity_number, Some(9));
    assert_eq!(decoded.severity_text.as_deref(), Some("INFO"));
    assert_eq!(decoded.body, "cart checkout completed");
    assert_eq!(decoded.trace_id.as_deref(), Some("trace-context-1"));
    assert_eq!(decoded.span_id.as_deref(), Some("span-context-1"));
    assert_eq!(
        decoded.field_value("resource_attr:service.name"),
        Some("checkout".into())
    );
    assert_eq!(
        decoded.field_value("log_attr:http.method").as_deref(),
        Some("POST")
    );
}
