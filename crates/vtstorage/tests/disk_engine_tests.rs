use std::{
    fs,
    fs::File,
    io::Write,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use vtcore::{Field, FieldFilter, LogRow, LogSearchRequest, TraceSearchRequest, TraceSpanRow};
use vtstorage::{DiskStorageConfig, DiskStorageEngine, DiskSyncPolicy, StorageEngine};

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

fn temp_test_dir(name: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time went backwards")
        .as_nanos();
    let path = std::env::temp_dir().join(format!("rust-vt-{name}-{nanos}"));
    fs::create_dir_all(&path).expect("create temp dir");
    path
}

#[test]
fn disk_engine_recovers_rows_after_reopen() {
    let path = temp_test_dir("recover");

    {
        let engine = DiskStorageEngine::open(&path).expect("open disk engine");
        engine
            .append_rows(vec![
                make_row("trace-disk-1", "span-1", 100, 150),
                make_row("trace-disk-1", "span-2", 160, 210),
            ])
            .expect("append rows");
    }

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let window = reopened
        .trace_window("trace-disk-1")
        .expect("trace window should survive restart");
    let rows =
        reopened.rows_for_trace("trace-disk-1", window.start_unix_nano, window.end_unix_nano);

    assert_eq!(window.start_unix_nano, 100);
    assert_eq!(window.end_unix_nano, 210);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].span_id, "span-1");
    assert_eq!(rows[1].span_id, "span-2");

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_stats_reflect_persisted_data() {
    let path = temp_test_dir("stats");
    let engine = DiskStorageEngine::open(&path).expect("open disk engine");

    engine
        .append_rows(vec![
            make_row("trace-disk-2", "span-1", 1, 2),
            make_row("trace-disk-2", "span-2", 3, 4),
            make_row("trace-disk-3", "span-1", 5, 6),
        ])
        .expect("append rows");

    let stats = engine.stats();

    assert_eq!(stats.rows_ingested, 3);
    assert_eq!(stats.traces_tracked, 2);
    assert!(stats.persisted_bytes > 0);
    assert_eq!(stats.segment_count, 1);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_rotates_segments_and_recovers_indexes() {
    let path = temp_test_dir("segments");
    let config = DiskStorageConfig::default().with_target_segment_size_bytes(1);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![
                make_row("trace-segment-1", "span-1", 10, 20),
                make_row("trace-segment-2", "span-1", 30, 40),
                make_row("trace-segment-3", "span-1", 50, 60),
            ])
            .expect("append rows");

        let stats = engine.stats();
        assert!(stats.segment_count >= 2);
    }

    let segments_dir = path.join("segments");
    assert!(segments_dir.exists());
    let persisted_segment_files = fs::read_dir(&segments_dir)
        .expect("read segment dir")
        .filter_map(Result::ok)
        .filter(|entry| {
            let file_name = entry.file_name().to_string_lossy().to_string();
            file_name.ends_with(".wal") || file_name.ends_with(".part")
        })
        .count();
    assert!(persisted_segment_files >= 1);

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    let hits = reopened.search_traces(&TraceSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 100,
        service_name: Some("checkout".to_string()),
        operation_name: None,
        field_filters: Vec::new(),
        limit: 10,
    });

    assert_eq!(hits.len(), 3);
    assert!(reopened.stats().segment_count >= 1);
    let first_part_path = fs::read_dir(&segments_dir)
        .expect("read segment dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .ends_with(".part")
        })
        .expect("recovery should seal wal files into part files");
    let part_bytes = fs::read(&first_part_path).expect("read part file");
    assert!(part_bytes.starts_with(b"VTPART1"));
    assert_eq!(part_bytes.get(7), Some(&2));

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_batches_row_reads_per_segment() {
    let path = temp_test_dir("read-batches");
    let engine = DiskStorageEngine::open(&path).expect("open disk engine");
    engine
        .append_rows(vec![
            make_row("trace-read-batch-1", "span-1", 100, 150),
            make_row("trace-read-batch-1", "span-2", 160, 210),
        ])
        .expect("append rows");

    let window = engine
        .trace_window("trace-read-batch-1")
        .expect("trace window exists");
    let rows = engine.rows_for_trace(
        "trace-read-batch-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );
    assert_eq!(rows.len(), 2);

    let stats = engine.stats();
    assert_eq!(stats.segment_read_batches, 1);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_reads_active_segment_rows_without_flushing_each_append() {
    let path = temp_test_dir("active-segment-visible");
    let engine = DiskStorageEngine::open(&path).expect("open disk engine");
    engine
        .append_rows(vec![
            make_row("trace-active-1", "span-1", 100, 150),
            make_row("trace-active-1", "span-2", 160, 210),
        ])
        .expect("append rows");

    let wal_path = path
        .join("segments")
        .join("segment-00000000000000000001.wal");
    assert_eq!(
        fs::metadata(&wal_path).expect("wal metadata").len(),
        0,
        "sync_policy=None should not flush every append",
    );

    let window = engine
        .trace_window("trace-active-1")
        .expect("trace window exists");
    let rows = engine.rows_for_trace(
        "trace-active-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].span_id, "span-1");
    assert_eq!(rows[1].span_id, "span-2");

    drop(engine);

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let reopened_window = reopened
        .trace_window("trace-active-1")
        .expect("trace window survives drop");
    let reopened_rows = reopened.rows_for_trace(
        "trace-active-1",
        reopened_window.start_unix_nano,
        reopened_window.end_unix_nano,
    );
    assert_eq!(reopened_rows.len(), 2);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_compacts_small_parts_after_reopen() {
    let path = temp_test_dir("compact");
    let config = DiskStorageConfig::default().with_target_segment_size_bytes(1);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![
                make_row("trace-compact-1", "span-1", 10, 20),
                make_row("trace-compact-1", "span-2", 30, 40),
                make_row("trace-compact-1", "span-3", 50, 60),
            ])
            .expect("append rows");

        assert!(engine.stats().segment_count >= 2);
    }

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    let window = reopened
        .trace_window("trace-compact-1")
        .expect("trace window exists");
    let rows = reopened.rows_for_trace(
        "trace-compact-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].span_id, "span-1");
    assert_eq!(rows[1].span_id, "span-2");
    assert_eq!(rows[2].span_id, "span-3");
    assert_eq!(reopened.stats().segment_count, 1);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_uses_selective_decode_for_part_reads() {
    let path = temp_test_dir("selective-decode");

    {
        let engine = DiskStorageEngine::open(&path).expect("open disk engine");
        engine
            .append_rows(vec![
                make_row("trace-part-read-1", "span-1", 100, 150),
                make_row("trace-part-read-1", "span-2", 160, 210),
            ])
            .expect("append rows");
    }

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let window = reopened
        .trace_window("trace-part-read-1")
        .expect("trace window exists");
    let rows = reopened.rows_for_trace(
        "trace-part-read-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 2);
    assert_eq!(reopened.stats().part_selective_decodes, 1);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_searches_traces_by_operation_name_after_reopen() {
    let path = temp_test_dir("operation-search");

    {
        let engine = DiskStorageEngine::open(&path).expect("open disk engine");
        engine
            .append_rows(vec![
                TraceSpanRow::new(
                    "trace-operation-1",
                    "span-1",
                    None,
                    "GET /checkout",
                    100,
                    150,
                    vec![Field::new("resource_attr:service.name", "checkout")],
                )
                .expect("row"),
                TraceSpanRow::new(
                    "trace-operation-2",
                    "span-1",
                    None,
                    "POST /checkout",
                    160,
                    210,
                    vec![Field::new("resource_attr:service.name", "checkout")],
                )
                .expect("row"),
            ])
            .expect("append rows");
    }

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let hits = reopened.search_traces(&TraceSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 500,
        service_name: Some("checkout".to_string()),
        operation_name: Some("POST /checkout".to_string()),
        field_filters: Vec::new(),
        limit: 10,
    });

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].trace_id, "trace-operation-2");

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_searches_traces_by_generic_field_filter_after_reopen() {
    let path = temp_test_dir("field-search");

    {
        let engine = DiskStorageEngine::open(&path).expect("open disk engine");
        engine
            .append_rows(vec![
                TraceSpanRow::new(
                    "trace-field-1",
                    "span-1",
                    None,
                    "GET /checkout",
                    100,
                    150,
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
                    160,
                    210,
                    vec![
                        Field::new("resource_attr:service.name", "checkout"),
                        Field::new("span_attr:http.method", "POST"),
                        Field::new("status_code", "2"),
                    ],
                )
                .expect("row"),
            ])
            .expect("append rows");
    }

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let hits = reopened.search_traces(&TraceSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 500,
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
        limit: 10,
    });

    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].trace_id, "trace-field-2");
    assert!(reopened
        .list_field_names()
        .contains(&"span_attr:http.method".to_string()));
    assert_eq!(
        reopened.list_field_values("span_attr:http.method"),
        vec!["GET".to_string(), "POST".to_string()]
    );

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_persists_typed_dynamic_field_columns_after_reopen() {
    let path = temp_test_dir("typed-columns");

    {
        let engine = DiskStorageEngine::open(&path).expect("open disk engine");
        let mut row = make_row("trace-typed-1", "span-1", 100, 150);
        row.fields
            .push(Field::new("span_attr:http.status_code", "200"));
        row.fields.push(Field::new("span_attr:retry", "false"));
        row.fields.sort_by(|left, right| left.name.cmp(&right.name));
        engine.append_rows(vec![row]).expect("append rows");
    }

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let window = reopened
        .trace_window("trace-typed-1")
        .expect("trace window exists");
    let rows = reopened.rows_for_trace(
        "trace-typed-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].field_value("span_attr:http.status_code").as_deref(),
        Some("200")
    );
    assert_eq!(
        rows[0].field_value("span_attr:retry").as_deref(),
        Some("false")
    );

    let stats = reopened.stats();
    assert!(stats.typed_field_columns >= 2);
    assert!(stats.string_field_columns >= 1);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_recovers_from_corrupted_wal_tail() {
    let path = temp_test_dir("wal-tail-recovery");
    let segments_path = path.join("segments");
    fs::create_dir_all(&segments_path).expect("create segments dir");
    let wal_path = segments_path.join("segment-00000000000000000001.wal");

    let row = make_row("trace-corrupt-tail-1", "span-1", 100, 150);
    let payload = serde_json::to_vec(&row).expect("serialize row");
    let mut wal = File::create(&wal_path).expect("create wal");
    wal.write_all(&(payload.len() as u32).to_le_bytes())
        .expect("write row len");
    wal.write_all(&payload).expect("write row payload");
    wal.write_all(&3u32.to_le_bytes())
        .expect("write corrupt row len");
    wal.write_all(&[0xff, 0xfe, 0xfd])
        .expect("write corrupt payload");
    wal.flush().expect("flush wal");

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let window = reopened
        .trace_window("trace-corrupt-tail-1")
        .expect("trace window should survive wal repair");
    let rows = reopened.rows_for_trace(
        "trace-corrupt-tail-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].span_id, "span-1");

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_records_fsync_operations_when_sync_policy_requires_it() {
    let path = temp_test_dir("sync-policy");
    let config = DiskStorageConfig::default()
        .with_sync_policy(DiskSyncPolicy::Data)
        .with_target_segment_size_bytes(1);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![make_row("trace-sync-1", "span-1", 10, 20)])
            .expect("append rows");

        let stats = engine.stats();
        assert!(stats.fsync_operations >= 1);
    }

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    assert!(reopened.stats().segment_count >= 1);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_recovers_native_logs_after_reopen() {
    let path = temp_test_dir("log-recover");

    {
        let engine = DiskStorageEngine::open(&path).expect("open disk engine");
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
                    Some("trace-2".to_string()),
                    Some("span-2".to_string()),
                    vec![Field::new("resource_attr:service.name", "payments")],
                ),
            ])
            .expect("append logs");
    }

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    let rows = reopened.search_logs(&LogSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 1_000,
        service_name: Some("payments".to_string()),
        severity_text: Some("ERROR".to_string()),
        field_filters: Vec::new(),
        limit: 10,
    });

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].log_id, "log-2");
    assert_eq!(rows[0].trace_id.as_deref(), Some("trace-2"));

    fs::remove_dir_all(path).expect("cleanup temp dir");
}
