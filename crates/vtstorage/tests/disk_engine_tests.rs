use std::{
    fs,
    fs::File,
    io::Write,
    path::PathBuf,
    sync::{Arc, Barrier},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use vtcore::{
    Field, FieldFilter, LogRow, LogSearchRequest, TraceBlock, TraceSearchRequest, TraceSpanRow,
};
use vtstorage::{DiskStorageConfig, DiskStorageEngine, DiskSyncPolicy, StorageEngine};

const RECOVERY_MANIFEST_FILENAME: &str = "trace-index.manifest";

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

fn wait_until(mut condition: impl FnMut() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if condition() {
            return;
        }
        thread::sleep(Duration::from_millis(10));
    }
    assert!(condition(), "condition not satisfied before timeout");
}

fn segment_files_with_suffix(path: &PathBuf, suffix: &str) -> Vec<PathBuf> {
    fs::read_dir(path)
        .expect("read segment dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|candidate| {
            candidate
                .file_name()
                .map(|value| value.to_string_lossy().ends_with(suffix))
                .unwrap_or(false)
        })
        .collect()
}

fn root_files_with_prefix(path: &PathBuf, prefix: &str) -> Vec<PathBuf> {
    fs::read_dir(path)
        .expect("read root dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|candidate| {
            candidate
                .file_name()
                .map(|value| value.to_string_lossy().starts_with(prefix))
                .unwrap_or(false)
        })
        .collect()
}

fn segment_data_files(path: &PathBuf) -> Vec<PathBuf> {
    fs::read_dir(path)
        .expect("read segment dir")
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|candidate| {
            candidate
                .file_name()
                .map(|value| {
                    let value = value.to_string_lossy();
                    value.ends_with(".wal") || value.ends_with(".part") || value.ends_with(".rows")
                })
                .unwrap_or(false)
        })
        .collect()
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
fn disk_engine_can_disable_trace_wal_and_recover_from_rows_only() {
    let path = temp_test_dir("recover-without-trace-wal");
    let config = DiskStorageConfig::default()
        .with_trace_shards(1)
        .with_target_segment_size_bytes(1)
        .with_trace_wal_enabled(false);

    {
        let engine = DiskStorageEngine::open_with_config(&path, config.clone())
            .expect("open disk engine without wal");
        engine
            .append_rows(vec![make_row("trace-no-wal-1", "span-1", 100, 150)])
            .expect("append first row");
        engine
            .append_rows(vec![make_row("trace-no-wal-2", "span-1", 200, 250)])
            .expect("append second row");

        let segments_path = path.join("segments");
        wait_until(|| !segment_files_with_suffix(&segments_path, ".rows").is_empty());
        assert!(
            segment_files_with_suffix(&segments_path, ".wal").is_empty(),
            "wal-disabled mode should not persist rotated trace segments as .wal files",
        );
        assert!(
            segment_files_with_suffix(&segments_path, ".part").is_empty(),
            "wal-disabled mode should not build .part files on the ingest hot path",
        );
    }

    let segments_path = path.join("segments");
    assert!(
        segment_files_with_suffix(&segments_path, ".wal").is_empty(),
        "wal-disabled mode should not leave trace .wal files behind after drop",
    );
    assert!(
        !segment_files_with_suffix(&segments_path, ".rows").is_empty(),
        "wal-disabled mode should materialize persisted segments as row files",
    );
    assert!(
        segment_files_with_suffix(&segments_path, ".part").is_empty(),
        "wal-disabled mode should not leave .part files behind after drop",
    );

    let reopened =
        DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine without wal");
    let first_window = reopened
        .trace_window("trace-no-wal-1")
        .expect("first trace window survives restart");
    let second_window = reopened
        .trace_window("trace-no-wal-2")
        .expect("second trace window survives restart");
    assert_eq!(
        reopened.rows_for_trace(
            "trace-no-wal-1",
            first_window.start_unix_nano,
            first_window.end_unix_nano,
        ),
        vec![make_row("trace-no-wal-1", "span-1", 100, 150)]
    );
    assert_eq!(
        reopened.rows_for_trace(
            "trace-no-wal-2",
            second_window.start_unix_nano,
            second_window.end_unix_nano,
        ),
        vec![make_row("trace-no-wal-2", "span-1", 200, 250)]
    );

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_accepts_trace_blocks_without_row_materialization_path() {
    let path = temp_test_dir("trace-block-append");
    let engine = DiskStorageEngine::open(&path).expect("open disk engine");
    engine
        .append_trace_block(TraceBlock::from_rows(vec![
            make_row("trace-block-disk-1", "span-1", 100, 150),
            make_row("trace-block-disk-1", "span-2", 160, 210),
        ]))
        .expect("append trace block");

    let window = engine
        .trace_window("trace-block-disk-1")
        .expect("trace window exists");
    let rows = engine.rows_for_trace(
        "trace-block-disk-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].span_id, "span-1");
    assert_eq!(rows[1].span_id, "span-2");

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_accepts_multiple_trace_blocks_in_one_append() {
    let path = temp_test_dir("trace-block-batch-append");
    let engine = DiskStorageEngine::open(&path).expect("open disk engine");
    engine
        .append_trace_blocks(vec![
            TraceBlock::from_rows(vec![
                make_row("trace-block-batch-1", "span-1", 100, 150),
                make_row("trace-block-batch-1", "span-2", 160, 210),
            ]),
            TraceBlock::from_rows(vec![
                make_row("trace-block-batch-2", "span-1", 300, 350),
                make_row("trace-block-batch-2", "span-2", 360, 410),
            ]),
        ])
        .expect("append trace blocks");

    let first_window = engine
        .trace_window("trace-block-batch-1")
        .expect("first trace window exists");
    let first_rows = engine.rows_for_trace(
        "trace-block-batch-1",
        first_window.start_unix_nano,
        first_window.end_unix_nano,
    );
    assert_eq!(first_rows.len(), 2);
    assert_eq!(first_rows[0].span_id, "span-1");
    assert_eq!(first_rows[1].span_id, "span-2");

    let second_window = engine
        .trace_window("trace-block-batch-2")
        .expect("second trace window exists");
    let second_rows = engine.rows_for_trace(
        "trace-block-batch-2",
        second_window.start_unix_nano,
        second_window.end_unix_nano,
    );
    assert_eq!(second_rows.len(), 2);
    assert_eq!(second_rows[0].span_id, "span-1");
    assert_eq!(second_rows[1].span_id, "span-2");

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_combines_concurrent_same_shard_block_appends() {
    let path = temp_test_dir("trace-block-combiner");
    let engine = Arc::new(
        DiskStorageEngine::open_with_config(
            &path,
            DiskStorageConfig::default().with_trace_shards(1),
        )
        .expect("open disk engine"),
    );
    let workers = 16;
    let barrier = Arc::new(Barrier::new(workers));
    let mut handles = Vec::new();

    for worker in 0..workers {
        let engine = engine.clone();
        let barrier = barrier.clone();
        handles.push(thread::spawn(move || {
            barrier.wait();
            engine
                .append_trace_blocks(vec![TraceBlock::from_rows(vec![
                    make_row(&format!("trace-combiner-{worker}"), "span-1", 100, 150),
                    make_row(&format!("trace-combiner-{worker}"), "span-2", 160, 210),
                ])])
                .expect("append trace blocks");
        }));
    }

    for handle in handles {
        handle.join().expect("join worker");
    }

    let stats = engine.stats();
    assert!(
        stats.trace_batch_flushes > 0,
        "disk combiner should report at least one combined flush",
    );
    assert!(
        stats.trace_batch_input_blocks >= workers as u64,
        "disk combiner should see all input blocks",
    );
    assert!(
        stats.trace_batch_output_blocks < stats.trace_batch_input_blocks,
        "disk combiner should reduce physical append batches below input block count",
    );

    drop(engine);
    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_wait_window_coalesces_staggered_same_shard_appends() {
    let path = temp_test_dir("trace-block-combiner-wait-window");
    let engine = Arc::new(
        DiskStorageEngine::open_with_config(
            &path,
            DiskStorageConfig::default()
                .with_trace_shards(1)
                .with_trace_combiner_wait(Duration::from_secs(1)),
        )
        .expect("open disk engine"),
    );
    let start_barrier = Arc::new(Barrier::new(2));

    let first = {
        let engine = engine.clone();
        let start_barrier = start_barrier.clone();
        thread::spawn(move || {
            start_barrier.wait();
            engine
                .append_rows(vec![make_row("trace-combiner-wait", "span-1", 100, 150)])
                .expect("append first row");
        })
    };

    start_barrier.wait();
    thread::sleep(Duration::from_millis(20));
    engine
        .append_rows(vec![make_row("trace-combiner-wait", "span-2", 160, 210)])
        .expect("append second row");
    first.join().expect("join first append");

    let window = engine
        .trace_window("trace-combiner-wait")
        .expect("trace window exists");
    let rows = engine.rows_for_trace(
        "trace-combiner-wait",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 2);
    let stats = engine.stats();
    assert_eq!(
        stats.trace_batch_flushes, 1,
        "requests arriving inside the combiner wait window should share a single flush",
    );
    assert_eq!(stats.trace_batch_input_blocks, 2);
    assert_eq!(stats.trace_batch_output_blocks, 1);

    drop(engine);
    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_group_commit_wait_batches_multiple_append_batches_into_one_flush() {
    let path = temp_test_dir("trace-group-commit-wait");
    let engine = Arc::new(
        DiskStorageEngine::open_with_config(
            &path,
            DiskStorageConfig::default()
                .with_trace_shards(1)
                .with_trace_combiner_wait(Duration::ZERO)
                .with_trace_group_commit_wait(Duration::from_secs(1)),
        )
        .expect("open disk engine"),
    );
    let start_barrier = Arc::new(Barrier::new(2));

    let first = {
        let engine = engine.clone();
        let start_barrier = start_barrier.clone();
        thread::spawn(move || {
            start_barrier.wait();
            engine
                .append_rows(vec![make_row("trace-group-commit", "span-1", 100, 150)])
                .expect("append first row");
        })
    };

    start_barrier.wait();
    thread::sleep(Duration::from_millis(20));
    engine
        .append_rows(vec![make_row("trace-group-commit", "span-2", 160, 210)])
        .expect("append second row");
    first.join().expect("join first append");

    let window = engine
        .trace_window("trace-group-commit")
        .expect("trace window exists");
    let rows = engine.rows_for_trace(
        "trace-group-commit",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 2);
    let stats = engine.stats();
    assert_eq!(
        stats.trace_batch_flushes, 2,
        "zero combiner wait should keep requests as separate append batches",
    );
    assert_eq!(stats.trace_batch_input_blocks, 2);
    assert_eq!(stats.trace_batch_output_blocks, 2);
    assert_eq!(
        stats.trace_group_commit_flushes, 1,
        "requests arriving inside the group-commit wait window should share one flush",
    );
    assert_eq!(stats.trace_group_commit_rows, 2);

    drop(engine);
    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_uses_batched_binary_wal_for_trace_block_appends() {
    let path = temp_test_dir("batched-wal");
    let config = DiskStorageConfig::default().with_sync_policy(DiskSyncPolicy::Data);
    let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk engine");

    engine
        .append_trace_block(TraceBlock::from_rows(vec![
            make_row("trace-batch-wal-1", "span-1", 100, 150),
            make_row("trace-batch-wal-1", "span-2", 160, 210),
        ]))
        .expect("append trace block");

    let wal_path = path
        .join("segments")
        .join("segment-00000000000000000001.wal");
    let wal_bytes = fs::read(&wal_path).expect("read wal file");
    assert!(wal_bytes.starts_with(b"VTWAL1"));
    assert_eq!(wal_bytes.get(6), Some(&3));

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_can_defer_wal_writes_until_drop_and_still_recover() {
    let path = temp_test_dir("deferred-wal");
    let config = DiskStorageConfig::default().with_trace_deferred_wal_writes(true);
    let wal_path = path
        .join("segments")
        .join("segment-00000000000000000001.wal");

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![
                make_row("trace-deferred-wal-1", "span-1", 100, 150),
                make_row("trace-deferred-wal-1", "span-2", 160, 210),
            ])
            .expect("append rows");

        let window = engine
            .trace_window("trace-deferred-wal-1")
            .expect("trace window should exist before wal drain");
        let rows = engine.rows_for_trace(
            "trace-deferred-wal-1",
            window.start_unix_nano,
            window.end_unix_nano,
        );
        assert_eq!(rows.len(), 2);
        assert!(
            !wal_path.exists() || fs::metadata(&wal_path).expect("wal metadata").len() == 0,
            "deferred wal mode should keep request-path appends out of the on-disk wal",
        );
    }

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    let window = reopened
        .trace_window("trace-deferred-wal-1")
        .expect("trace window should survive clean shutdown");
    let rows = reopened.rows_for_trace(
        "trace-deferred-wal-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );
    assert_eq!(rows.len(), 2);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_exposes_search_services_and_field_values_from_head_before_seal() {
    let path = temp_test_dir("head-query-visibility");
    let config = DiskStorageConfig::default()
        .with_trace_shards(1)
        .with_target_segment_size_bytes(1 << 30);
    let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk engine");

    engine
        .append_rows(vec![
            TraceSpanRow::new(
                "trace-head-query-1",
                "span-1",
                None,
                "GET /checkout",
                100,
                150,
                vec![
                    Field::new("resource_attr:service.name", "checkout"),
                    Field::new("span_attr:http.method", "GET"),
                ],
            )
            .expect("row"),
            TraceSpanRow::new(
                "trace-head-query-2",
                "span-1",
                None,
                "POST /checkout",
                160,
                220,
                vec![
                    Field::new("resource_attr:service.name", "checkout"),
                    Field::new("span_attr:http.method", "POST"),
                ],
            )
            .expect("row"),
        ])
        .expect("append rows");

    wait_until(|| {
        engine.list_services() == vec!["checkout".to_string()]
            && engine.list_field_values("span_attr:http.method")
                == vec!["GET".to_string(), "POST".to_string()]
            && engine
                .search_traces(&TraceSearchRequest {
                    start_unix_nano: 0,
                    end_unix_nano: 500,
                    service_name: Some("checkout".to_string()),
                    operation_name: Some("POST /checkout".to_string()),
                    field_filters: vec![FieldFilter {
                        name: "span_attr:http.method".to_string(),
                        value: "POST".to_string(),
                    }],
                    limit: 10,
                })
                .len()
                == 1
    });

    let hits = engine.search_traces(&TraceSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 500,
        service_name: Some("checkout".to_string()),
        operation_name: Some("POST /checkout".to_string()),
        field_filters: vec![FieldFilter {
            name: "span_attr:http.method".to_string(),
            value: "POST".to_string(),
        }],
        limit: 10,
    });
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].trace_id, "trace-head-query-2");

    let window = engine
        .trace_window("trace-head-query-2")
        .expect("trace window should exist for head trace");
    let rows = engine.rows_for_trace(
        "trace-head-query-2",
        window.start_unix_nano,
        window.end_unix_nano,
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].span_id, "span-1");

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_deferred_wal_keeps_query_indexes_visible_before_drop_and_after_reopen() {
    let path = temp_test_dir("deferred-wal-query-visibility");
    let config = DiskStorageConfig::default()
        .with_trace_shards(1)
        .with_target_segment_size_bytes(1 << 30)
        .with_trace_deferred_wal_writes(true);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![TraceSpanRow::new(
                "trace-deferred-query-1",
                "span-1",
                None,
                "POST /checkout",
                100,
                180,
                vec![
                    Field::new("resource_attr:service.name", "checkout"),
                    Field::new("span_attr:http.method", "POST"),
                ],
            )
            .expect("row")])
            .expect("append rows");

        wait_until(|| {
            engine.list_services() == vec!["checkout".to_string()]
                && engine.list_field_values("span_attr:http.method") == vec!["POST".to_string()]
                && engine
                    .search_traces(&TraceSearchRequest {
                        start_unix_nano: 0,
                        end_unix_nano: 500,
                        service_name: Some("checkout".to_string()),
                        operation_name: Some("POST /checkout".to_string()),
                        field_filters: vec![FieldFilter {
                            name: "span_attr:http.method".to_string(),
                            value: "POST".to_string(),
                        }],
                        limit: 10,
                    })
                    .len()
                    == 1
        });

        let hits = engine.search_traces(&TraceSearchRequest {
            start_unix_nano: 0,
            end_unix_nano: 500,
            service_name: Some("checkout".to_string()),
            operation_name: Some("POST /checkout".to_string()),
            field_filters: vec![FieldFilter {
                name: "span_attr:http.method".to_string(),
                value: "POST".to_string(),
            }],
            limit: 10,
        });
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].trace_id, "trace-deferred-query-1");
    }

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    assert_eq!(reopened.list_services(), vec!["checkout".to_string()]);
    assert_eq!(
        reopened.list_field_values("span_attr:http.method"),
        vec!["POST".to_string()]
    );
    let hits = reopened.search_traces(&TraceSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 500,
        service_name: Some("checkout".to_string()),
        operation_name: Some("POST /checkout".to_string()),
        field_filters: vec![FieldFilter {
            name: "span_attr:http.method".to_string(),
            value: "POST".to_string(),
        }],
        limit: 10,
    });
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].trace_id, "trace-deferred-query-1");

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
    assert!(stats.segment_count >= 1);

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_stats_keep_cached_field_column_counts_after_part_path_changes() {
    let path = temp_test_dir("stats-cached-field-columns");
    let config = DiskStorageConfig::default()
        .with_trace_shards(1)
        .with_target_segment_size_bytes(1);
    let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk engine");

    engine
        .append_rows(vec![TraceSpanRow::new(
            "trace-stats-cached-1",
            "span-1",
            None,
            "GET /checkout",
            100,
            180,
            vec![
                Field::new("resource_attr:service.name", "checkout"),
                Field::new("span_attr:http.method", "GET"),
                Field::new("span_attr:http.status_code", "200"),
            ],
        )
        .expect("row")])
        .expect("append rows");

    wait_until(|| {
        let stats = engine.stats();
        stats.segment_count >= 1
            && stats.trace_seal_queue_depth == 0
            && stats.typed_field_columns == 1
            && stats.string_field_columns == 2
    });

    let stats = engine.stats();
    assert_eq!(stats.typed_field_columns, 1);
    assert_eq!(stats.string_field_columns, 2);

    let data_path = segment_data_files(&path.join("segments"))
        .into_iter()
        .next()
        .expect("persisted segment should exist");
    let moved_data_path = data_path.with_extension("segment.moved");
    fs::rename(&data_path, &moved_data_path).expect("move persisted segment out of the way");

    let cached_stats = engine.stats();
    assert_eq!(cached_stats.typed_field_columns, 1);
    assert_eq!(cached_stats.string_field_columns, 2);

    drop(engine);
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
fn disk_engine_writes_recovery_snapshot_and_reopens_without_recreating_segment_meta() {
    let path = temp_test_dir("recovery-snapshot");
    let config = DiskStorageConfig::default()
        .with_target_segment_size_bytes(1)
        .with_trace_shards(1);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![
                make_row("trace-snapshot-1", "span-1", 10, 20),
                make_row("trace-snapshot-2", "span-1", 30, 40),
            ])
            .expect("append rows");
        wait_until(|| engine.stats().segment_count >= 1);
    }

    let manifest_path = path.join(RECOVERY_MANIFEST_FILENAME);
    assert!(
        manifest_path.exists(),
        "clean shutdown should persist a recovery manifest"
    );
    assert!(
        !root_files_with_prefix(&path, "trace-index-shard-").is_empty(),
        "clean shutdown should persist per-shard recovery files",
    );

    let segments_dir = path.join("segments");
    let meta_json_files = segment_files_with_suffix(&segments_dir, ".meta.json");
    let meta_bin_files = segment_files_with_suffix(&segments_dir, ".meta.bin");
    for meta_path in meta_json_files.iter().chain(meta_bin_files.iter()) {
        fs::remove_file(meta_path).expect("remove segment meta");
    }

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    let hits = reopened.search_traces(&TraceSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 100,
        service_name: Some("checkout".to_string()),
        operation_name: None,
        field_filters: Vec::new(),
        limit: 10,
    });
    assert_eq!(hits.len(), 2);
    assert!(
        segment_files_with_suffix(&segments_dir, ".meta.json").is_empty()
            && segment_files_with_suffix(&segments_dir, ".meta.bin").is_empty(),
        "snapshot-backed reopen should not need to recreate missing segment metadata",
    );

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_replays_only_tail_segments_outside_the_recovery_snapshot() {
    let path = temp_test_dir("recovery-tail");
    let config = DiskStorageConfig::default()
        .with_target_segment_size_bytes(1)
        .with_trace_shards(1);
    let manifest_path = path.join(RECOVERY_MANIFEST_FILENAME);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![make_row("trace-base", "span-1", 10, 20)])
            .expect("append base row");
        wait_until(|| engine.stats().segment_count >= 1);
    }
    let stale_manifest = fs::read(&manifest_path).expect("read initial manifest");
    let stale_shard_files = root_files_with_prefix(&path, "trace-index-shard-")
        .into_iter()
        .map(|path| {
            let file_name = path
                .file_name()
                .expect("snapshot shard file name")
                .to_string_lossy()
                .to_string();
            let bytes = fs::read(&path).expect("read snapshot shard file");
            (file_name, bytes)
        })
        .collect::<Vec<_>>();

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("reopen disk engine");
        engine
            .append_rows(vec![make_row("trace-tail", "span-1", 30, 40)])
            .expect("append tail row");
        wait_until(|| engine.stats().segment_count >= 2);
    }

    fs::write(&manifest_path, stale_manifest).expect("restore stale manifest");
    for shard_path in root_files_with_prefix(&path, "trace-index-shard-") {
        fs::remove_file(shard_path).expect("remove current shard snapshot");
    }
    for (file_name, bytes) in stale_shard_files {
        fs::write(path.join(file_name), bytes).expect("restore stale shard snapshot");
    }

    let segments_dir = path.join("segments");
    let persisted_segment_files = segment_data_files(&segments_dir);
    assert!(
        persisted_segment_files.len() >= 2,
        "fixture should contain at least two persisted segments"
    );
    let mut meta_files = segment_files_with_suffix(&segments_dir, ".meta.bin");
    meta_files.sort();
    if meta_files.is_empty() {
        meta_files = segment_files_with_suffix(&segments_dir, ".meta.json");
        meta_files.sort();
    }
    let covered_meta = meta_files.first().cloned();
    if let Some(meta_path) = covered_meta.as_ref() {
        fs::remove_file(meta_path).expect("remove covered segment metadata");
    }

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    assert!(reopened.trace_window("trace-base").is_some());
    assert!(reopened.trace_window("trace-tail").is_some());
    if let Some(meta_path) = covered_meta {
        assert!(
            !meta_path.exists(),
            "segments already covered by the recovery snapshot should not need metadata recreation",
        );
    } else {
        let covered_meta_json = segments_dir.join("segment-00000000000000000001.meta.json");
        let covered_meta_bin = segments_dir.join("segment-00000000000000000001.meta.bin");
        assert!(
            !covered_meta_json.exists() && !covered_meta_bin.exists(),
            "snapshot-covered wal segments should not need metadata recreation",
        );
    }

    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_ignores_corrupted_recovery_shard_and_rebuilds_from_segments() {
    let path = temp_test_dir("recovery-corrupt-shard");
    let config = DiskStorageConfig::default()
        .with_target_segment_size_bytes(1)
        .with_trace_shards(1);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![
                make_row("trace-corrupt-1", "span-1", 10, 20),
                make_row("trace-corrupt-2", "span-1", 30, 40),
            ])
            .expect("append rows");
        wait_until(|| engine.stats().segment_count >= 1);
    }

    let shard_files = root_files_with_prefix(&path, "trace-index-shard-");
    assert_eq!(
        shard_files.len(),
        1,
        "fixture should persist exactly one shard snapshot"
    );
    fs::write(&shard_files[0], b"corrupted").expect("corrupt shard snapshot");

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    let hits = reopened.search_traces(&TraceSearchRequest {
        start_unix_nano: 0,
        end_unix_nano: 100,
        service_name: Some("checkout".to_string()),
        operation_name: None,
        field_filters: Vec::new(),
        limit: 10,
    });
    assert_eq!(hits.len(), 2, "corrupted shard snapshot should be ignored");

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
            .append_rows(vec![make_row("trace-compact-1", "span-1", 10, 20)])
            .expect("append first row");
        engine
            .append_rows(vec![make_row("trace-compact-1", "span-2", 30, 40)])
            .expect("append second row");
        engine
            .append_rows(vec![make_row("trace-compact-1", "span-3", 50, 60)])
            .expect("append third row");

        wait_until(|| engine.stats().segment_count >= 2);
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
    let config = DiskStorageConfig::default().with_target_segment_size_bytes(1);

    {
        let engine =
            DiskStorageEngine::open_with_config(&path, config.clone()).expect("open disk engine");
        engine
            .append_rows(vec![make_row("trace-part-read-1", "span-1", 100, 150)])
            .expect("append first row");
        engine
            .append_rows(vec![make_row("trace-part-read-1", "span-2", 160, 210)])
            .expect("append second row");
        engine
            .append_rows(vec![make_row("trace-part-read-1", "span-3", 220, 260)])
            .expect("append third row");
        wait_until(|| engine.stats().segment_count >= 2);
    }

    let reopened = DiskStorageEngine::open_with_config(&path, config).expect("reopen disk engine");
    let window = reopened
        .trace_window("trace-part-read-1")
        .expect("trace window exists");
    let rows = reopened.rows_for_trace(
        "trace-part-read-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(rows.len(), 3);
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
fn disk_engine_lists_services_trace_ids_and_field_values_after_reopen() {
    let path = temp_test_dir("list-indexes");

    {
        let engine = DiskStorageEngine::open(&path).expect("open disk engine");
        engine
            .append_rows(vec![
                TraceSpanRow::new(
                    "trace-list-1",
                    "span-1",
                    None,
                    "GET /checkout",
                    100,
                    150,
                    vec![
                        Field::new("resource_attr:service.name", "checkout"),
                        Field::new("span_attr:http.method", "GET"),
                    ],
                )
                .expect("row"),
                TraceSpanRow::new(
                    "trace-list-2",
                    "span-1",
                    None,
                    "POST /orders",
                    160,
                    210,
                    vec![
                        Field::new("resource_attr:service.name", "orders"),
                        Field::new("span_attr:http.method", "POST"),
                    ],
                )
                .expect("row"),
            ])
            .expect("append rows");
    }

    let reopened = DiskStorageEngine::open(&path).expect("reopen disk engine");
    assert_eq!(
        reopened.list_services(),
        vec!["checkout".to_string(), "orders".to_string()]
    );
    assert_eq!(
        reopened.list_trace_ids(),
        vec!["trace-list-1".to_string(), "trace-list-2".to_string()]
    );
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
fn disk_engine_trace_by_id_reads_across_rotated_persisted_segments() {
    let path = temp_test_dir("head-and-sealed-federation");
    let config = DiskStorageConfig::default()
        .with_trace_shards(1)
        .with_target_segment_size_bytes(1);
    let engine = DiskStorageEngine::open_with_config(&path, config).expect("open disk engine");

    engine
        .append_rows(vec![make_row("trace-federated-1", "span-1", 100, 150)])
        .expect("append first head row");
    engine
        .append_rows(vec![make_row("trace-federated-1", "span-2", 160, 210)])
        .expect("append second head row");

    wait_until(|| {
        let stats = engine.stats();
        stats.segment_count >= 2 && stats.trace_seal_queue_depth == 0
    });

    let window = engine
        .trace_window("trace-federated-1")
        .expect("trace window should span rotated persisted data");
    let rows = engine.rows_for_trace(
        "trace-federated-1",
        window.start_unix_nano,
        window.end_unix_nano,
    );

    assert_eq!(window.start_unix_nano, 100);
    assert_eq!(window.end_unix_nano, 210);
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter()
            .map(|row| row.span_id.as_str())
            .collect::<Vec<_>>(),
        vec!["span-1", "span-2"]
    );

    drop(engine);
    fs::remove_dir_all(path).expect("cleanup temp dir");
}

#[test]
fn disk_engine_reports_head_group_commit_and_seal_metrics() {
    let head_path = temp_test_dir("head-metrics");
    let head_engine = DiskStorageEngine::open_with_config(
        &head_path,
        DiskStorageConfig::default().with_trace_shards(1),
    )
    .expect("open disk engine for head metrics");

    head_engine
        .append_rows(vec![make_row("trace-head-only-1", "span-1", 10, 20)])
        .expect("append head-only row");

    let head_stats = head_engine.stats();
    assert!(head_stats.trace_head_segments >= 1);
    assert!(head_stats.trace_head_rows >= 1);
    assert!(head_stats.trace_group_commit_flushes >= 1);
    assert!(head_stats.trace_group_commit_rows >= 1);
    assert!(head_stats.trace_group_commit_bytes > 0);

    drop(head_engine);
    fs::remove_dir_all(head_path).expect("cleanup head metrics temp dir");

    let seal_path = temp_test_dir("seal-metrics");
    let config = DiskStorageConfig::default()
        .with_trace_shards(1)
        .with_target_segment_size_bytes(1);
    let engine = DiskStorageEngine::open_with_config(&seal_path, config).expect("open disk engine");

    engine
        .append_rows(vec![make_row("trace-head-metrics-1", "span-1", 100, 150)])
        .expect("append first metrics row");
    engine
        .append_rows(vec![make_row("trace-head-metrics-2", "span-1", 200, 250)])
        .expect("append second metrics row");

    wait_until(|| {
        let stats = engine.stats();
        stats.segment_count >= 2 && stats.trace_seal_queue_depth == 0
    });

    let stats = engine.stats();
    assert!(stats.trace_group_commit_flushes >= 2);
    assert!(stats.trace_group_commit_rows >= 2);
    assert!(stats.trace_group_commit_bytes > 0);
    assert_eq!(stats.trace_seal_queue_depth, 0);
    assert!(stats.trace_seal_completed >= 2);
    assert!(stats.trace_seal_rows >= 2);
    assert!(stats.trace_seal_bytes > 0);

    drop(engine);
    fs::remove_dir_all(seal_path).expect("cleanup temp dir");
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
