use vtcore::{
    decode_log_row, decode_log_rows, decode_trace_block, decode_trace_row, decode_trace_rows,
    encode_log_row, encode_log_rows, encode_trace_block, encode_trace_block_row,
    encode_trace_block_rows_packed, encode_trace_row, encode_trace_rows,
    encode_trace_rows_from_encoded_rows, Field, LogRow, TraceBlock, TraceSpanRow,
};

fn make_row(trace_id: &str, span_id: &str, start: i64, end: i64) -> TraceSpanRow {
    TraceSpanRow::new(
        trace_id.to_string(),
        span_id.to_string(),
        Some("parent-1".to_string()),
        format!("span-{span_id}"),
        start,
        end,
        vec![
            Field::new("resource_attr:service.name", "checkout"),
            Field::new("span_attr:http.method", "POST"),
        ],
    )
    .expect("valid row")
}

#[test]
fn trace_row_binary_codec_round_trips_single_row() {
    let row = make_row("trace-1", "span-1", 100, 150);

    let encoded = encode_trace_row(&row);
    let decoded = decode_trace_row(&encoded).expect("decode row");

    assert_eq!(decoded, row);
}

#[test]
fn trace_row_binary_codec_round_trips_batches() {
    let rows = vec![
        make_row("trace-1", "span-1", 100, 150),
        make_row("trace-1", "span-2", 160, 210),
        make_row("trace-2", "span-1", 220, 250),
    ];

    let encoded = encode_trace_rows(&rows);
    let decoded = decode_trace_rows(&encoded).expect("decode rows");

    assert_eq!(decoded, rows);
}

#[test]
fn trace_row_binary_codec_reuses_preencoded_rows_for_batches() {
    let rows = vec![
        make_row("trace-1", "span-1", 100, 150),
        make_row("trace-1", "span-2", 160, 210),
        make_row("trace-2", "span-1", 220, 250),
    ];

    let encoded_rows: Vec<Vec<u8>> = rows.iter().map(encode_trace_row).collect();
    let encoded = encode_trace_rows_from_encoded_rows(
        encoded_rows
            .iter()
            .map(|encoded_row| encoded_row.as_slice()),
    );
    let decoded = decode_trace_rows(&encoded).expect("decode rows");

    assert_eq!(decoded, rows);
}

#[test]
fn trace_block_binary_codec_round_trips_rows() {
    let rows = vec![
        make_row("trace-1", "span-1", 100, 150),
        make_row("trace-1", "span-2", 160, 210),
        make_row("trace-2", "span-1", 220, 250),
    ];

    let block = TraceBlock::from_rows(rows.clone());
    let encoded = encode_trace_block(&block);
    let decoded = decode_trace_block(&encoded).expect("decode trace block");

    assert_eq!(decoded.rows(), rows);
}

#[test]
fn trace_block_binary_codec_compacts_hex_span_ids_more_than_textual_ids() {
    let hex_block = TraceBlock::from_rows(vec![TraceSpanRow::new(
        "trace-compact".to_string(),
        "0123456789abcdef".to_string(),
        Some("1111111111111111".to_string()),
        "span-compact".to_string(),
        100,
        150,
        vec![Field::new("resource_attr:service.name", "checkout")],
    )
    .expect("hex row")]);
    let text_block = TraceBlock::from_rows(vec![TraceSpanRow::new(
        "trace-compact".to_string(),
        "0123456789abcdeg".to_string(),
        Some("111111111111111g".to_string()),
        "span-compact".to_string(),
        100,
        150,
        vec![Field::new("resource_attr:service.name", "checkout")],
    )
    .expect("text row")]);

    let hex_encoded = encode_trace_block(&hex_block);
    let text_encoded = encode_trace_block(&text_block);

    assert!(
        hex_encoded.len() < text_encoded.len(),
        "hex span identifiers should encode more compactly than non-hex text",
    );
}

#[test]
fn trace_block_row_encoder_matches_trace_row_encoder() {
    let row = make_row("trace-direct", "span-direct", 100, 140);
    let block = TraceBlock::from_rows(vec![row.clone()]);

    assert_eq!(encode_trace_block_row(&block, 0), encode_trace_row(&row));
}

#[test]
fn trace_block_row_batch_encoder_packs_rows_without_losing_boundaries() {
    let rows = vec![
        make_row("trace-pack-1", "span-1", 100, 150),
        make_row("trace-pack-1", "span-2", 160, 210),
    ];
    let block = TraceBlock::from_rows(rows.clone());
    let (bytes, ranges) = encode_trace_block_rows_packed(&block);

    let decoded: Vec<TraceSpanRow> = ranges
        .iter()
        .map(|range| decode_trace_row(&bytes[range.clone()]).expect("decode packed row"))
        .collect();

    assert_eq!(decoded, rows);
}

#[test]
fn trace_row_binary_codec_round_trips_log_backed_rows() {
    let log_row = LogRow::new(
        "log-1",
        1_000,
        Some(1_010),
        Some(13),
        Some("WARN".to_string()),
        "inventory low",
        Some("trace-ctx-1".to_string()),
        Some("span-ctx-1".to_string()),
        vec![
            Field::new("resource_attr:service.name", "inventory"),
            Field::new("log_attr:sku", "10001"),
        ],
    )
    .to_trace_row()
    .expect("encode log row");

    let encoded = encode_trace_row(&log_row);
    let decoded = decode_trace_row(&encoded).expect("decode row");

    assert_eq!(decoded, log_row);
}

#[test]
fn log_row_binary_codec_round_trips_single_row() {
    let row = LogRow::new(
        "log-9",
        9_000,
        Some(9_010),
        Some(17),
        Some("ERROR".to_string()),
        "payment declined",
        Some("trace-9".to_string()),
        Some("span-9".to_string()),
        vec![
            Field::new("resource_attr:service.name", "payments"),
            Field::new("log_attr:gateway", "adyen"),
        ],
    );

    let encoded = encode_log_row(&row);
    let decoded = decode_log_row(&encoded).expect("decode log row");

    assert_eq!(decoded, row);
}

#[test]
fn log_row_binary_codec_round_trips_batches() {
    let rows = vec![
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
            Some("trace-2".to_string()),
            Some("span-2".to_string()),
            vec![Field::new("resource_attr:service.name", "payments")],
        ),
    ];

    let encoded = encode_log_rows(&rows);
    let decoded = decode_log_rows(&encoded).expect("decode log rows");

    assert_eq!(decoded, rows);
}
