use vtcore::{Field, LogRow, TraceBlockBuilder, TraceSpanRow, TraceWindow};

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

    let custom_field_names: Vec<&str> =
        row.fields.iter().map(|field| field.name.as_ref()).collect();

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

#[test]
fn trace_block_builder_can_append_shared_and_row_fields_without_intermediate_row_vec() {
    let mut builder = TraceBlockBuilder::with_capacity(1);
    let shared_fields = vec![
        Field::new("resource_attr:service.name", "checkout"),
        Field::new("instrumentation_scope.name", "io.opentelemetry.rust"),
    ];

    builder.push_prevalidated_split_fields(
        "trace-4",
        "span-4",
        Some("parent-4".into()),
        "POST /checkout",
        100,
        180,
        180,
        &shared_fields,
        vec![Field::new("span_attr:http.method", "POST")],
    );

    let block = builder.finish();
    let row = block.row(0);

    assert_eq!(row.trace_id, "trace-4");
    assert_eq!(row.span_id, "span-4");
    assert_eq!(row.parent_span_id.as_deref(), Some("parent-4"));
    assert_eq!(row.name, "POST /checkout");
    assert_eq!(
        row.fields
            .iter()
            .map(|field| (field.name.as_ref(), field.value.as_ref()))
            .collect::<Vec<_>>(),
        vec![
            ("resource_attr:service.name", "checkout"),
            ("instrumentation_scope.name", "io.opentelemetry.rust"),
            ("span_attr:http.method", "POST"),
        ]
    );
}

#[test]
fn trace_block_builder_deduplicates_reused_shared_field_groups() {
    let mut builder = TraceBlockBuilder::with_capacity(2);
    let shared_fields = vec![
        Field::new("resource_attr:service.name", "checkout"),
        Field::new("instrumentation_scope.name", "io.opentelemetry.rust"),
    ];

    builder.push_prevalidated_split_fields(
        "trace-5",
        "span-5a",
        None,
        "POST /checkout",
        100,
        150,
        150,
        &shared_fields,
        vec![Field::new("span_attr:http.method", "POST")],
    );
    builder.push_prevalidated_split_fields(
        "trace-5",
        "span-5b",
        Some("span-5a".into()),
        "POST /checkout",
        160,
        220,
        220,
        &shared_fields,
        vec![Field::new("span_attr:http.status_code", "200")],
    );

    let block = builder.finish();

    assert_eq!(block.shared_field_group_count(), 1);
    assert_eq!(block.stored_field_count(), 4);
    assert_eq!(block.row(0).fields.len(), 3);
    assert_eq!(block.row(1).fields.len(), 3);
}
