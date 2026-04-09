# Progress Log

## Session: 2026-04-08

### Phase 1: Canonical Reading
- **Status:** complete
- Actions taken:
  - Read `brainstorming` skill instructions to keep the comparison design-first.
  - Read `planning-with-files` skill instructions and initialized task files in this repo.
  - Read the thread capability package template to align future thread-transfer assets with ability transfer instead of simple state transfer.
  - Read the upstream VictoriaTraces source analysis report.
  - Read current Rust architecture and recovery v2 design docs.
- Files created/modified:
  - task_plan.md (created)
  - findings.md (created)
  - progress.md (created)

### Phase 2: Comparison and Delivery
- **Status:** complete
- Actions taken:
  - Compared upstream and current Rust design along ingest, query/index, persistence/recovery, and cluster/HA dimensions.
  - Anchored the comparison to the fixed official benchmark harness and current same-shape evidence.
  - Wrote the main design realignment document.
  - Wrote a thread capability package so future thread transfer can inherit method and benchmark conventions.
- Files created/modified:
  - docs/plans/2026-04-08-victoriatraces-design-realignment.md (created)
  - docs/handoffs/2026-04-08-rust-vt-design-capability-package.md (created)
  - task_plan.md (updated)
  - findings.md (updated)
  - progress.md (updated)

## Error Log
| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|

## Notes
- This session is converging on a design comparison and next-step architecture, not yet implementation.
- The next implementation wave should start from the realignment doc rather than re-opening the storage design question from scratch.

## Session: 2026-04-09

### Phase 5: Recovery-First Implementation
- **Status:** in progress
- Actions taken:
  - Read `brainstorming` and `test-driven-development` skill instructions before continuing storage behavior changes.
  - Implemented recovery manifest v2 support for covered segment kind, so snapshot-covered segments can now round-trip as either `.wal` or `.part`.
  - Updated clean-shutdown snapshot persistence to preserve the actual `segment_paths` instead of rewriting everything to `.part`.
  - Added a regression test proving that wal-backed covered segments survive manifest round-trip without being rewritten as part-backed entries.
  - Moved steady-state trace roll from immediate seal to rotate-only persistence, so rolled segments stay wal-backed persisted segments instead of immediately sealing into parts.
  - Updated reopen/recovery/compaction paths to accept wal-backed persisted segments as first-class recovered state.
  - Re-ran full `vtstorage` test suites after rotate-only landed.
  - Re-ran same-shape native arm64 official benchmark and flipped the result from lagging to leading: 5-round median `612,601 spans/s` for Rust disk vs `601,426 spans/s` for official, with lower p99.
  - Removed wasted `live_updates` construction from the append hot path and confirmed `vtstorage` still passes end-to-end tests.
  - Added an explicit trace WAL switch to disk storage config, defaulting to enabled so the stable runtime keeps the stronger crash semantics.
  - Implemented `trace_wal_enabled=false` so active head rows stay query-visible in memory, but rolled segments materialize directly into `.part` without producing trace `.wal` files.
  - Added recovery coverage proving that a WAL-disabled engine can restart from part-backed rotated segments and recover trace rows correctly.
  - Wired `VT_STORAGE_TRACE_WAL_ENABLED` through `vtapi` env loading and documented it in the storage env examples plus the Chinese README config table.
  - Removed the stale live-update overlay tests and deleted the no-longer-used live-update helper/API surface from `crates/vtstorage/src/disk.rs`.
  - Removed the idle trace seal runtime threads from disk storage open/drop, since rotate-only no longer schedules that path at all.
  - Removed a batch of truly dead helper functions and unused structures so `vtstorage` returns to a clean no-warning state.
  - Re-ran `cargo test -p vtstorage` and `cargo test -p vtapi` after the cleanup batch and got full green again.
  - Re-ran the native arm64 same-shape official benchmark on the current working tree and observed that the current tree is still behind official: Rust disk `522,674 spans/s`, official `595,256 spans/s`, throughput ratio `0.8781`, p99 ratio `1.1799`.
  - Ran a direct throughput-profile probe for WAL on/off and found that the current no-WAL path is functionally correct but much slower under sustained load than WAL-on, because it falls into a heavier rotate-to-part path.
  - Replaced the no-WAL rotate path with a cheap persisted `.rows` row-file path instead of synchronously materializing `.part` files on every roll.
  - Extended recovery manifest covered-segment kind from `wal|part` to `wal|part|rows`, so clean-shutdown snapshot recovery stays valid for the new no-WAL persisted segment form.
  - Added coverage proving both external restart recovery and internal manifest round-trip preserve rows-backed covered segments.
  - Re-ran `cargo test -p vtstorage` and `cargo test -p vtapi` after the rows-path cut and kept full green.
  - Re-ran direct native arm64 WAL on/off throughput probes after the rows-path cut:
    - WAL on: `692,153 spans/s`, `p99 0.677 ms`
    - WAL off: `595,778 spans/s`, `p99 0.752 ms`
    - compared with the old no-WAL direct-to-part path (`252,397 spans/s`), the new no-WAL path recovers the intended throughput profile while remaining slightly behind WAL-on.
  - Re-ran the native arm64 official same-shape benchmark on the new tree and returned Rust disk to a stable lead over official: 3-round median `638,582 spans/s` vs official `607,682 spans/s`, with Rust p99 `0.768 ms` vs official `1.253 ms`.
- Files created/modified:
  - crates/vtstorage/src/disk.rs (updated)
  - crates/vtstorage/tests/disk_engine_tests.rs (updated)
  - crates/vtapi/src/main.rs (updated)
  - README.md (updated)
  - ops/env/storage-a.env.example (updated)
  - ops/env/storage-b.env.example (updated)
  - task_plan.md (updated)
  - progress.md (updated)
  - findings.md (updated)

### Phase 6: Sealed Query-Prune Index
- **Status:** complete
- Actions taken:
  - Continued under `brainstorming` + `test-driven-development` discipline instead of stopping at design docs.
  - Added failing unit tests for persisted segment summaries, shard-summary round-trip, exact trace matching from segment metadata, and the new minimal persisted runtime index contract.
  - Introduced persisted segment summaries as a real sealed-side prune structure and extended them to carry per-trace summaries inside each persisted segment.
  - Changed persisted-segment observation so recovered/rotated sealed segments now keep only minimal runtime state (`trace_ids`, `windows_by_trace`, `row_refs_by_trace`, `rows_ingested`) plus compact persisted summaries, instead of rebuilding the old heavy persisted inverted indexes in memory.
  - Routed reopened sealed trace search through `candidate persisted segments -> exact trace-summary match`, with legacy shard-search fallback only for older recovery snapshots that do not yet carry per-trace summaries.
  - Upgraded recovery shard snapshot format from v2 to compact v3:
    - v3 no longer writes empty legacy persisted inverted indexes into the shard payload
    - v3 persists compact segment summaries plus per-trace summaries
    - versioned readers still accept older shard snapshot payloads
  - Preserved the important recovery semantic that snapshot-backed reopen plus missing `segment-*.meta.{json,bin}` must still answer queries without recreating metadata files.
  - Re-ran the targeted disk-query regression suite and then full `vtstorage` + `vtapi` suites, all green.
  - Rebuilt release arm64 binaries and re-ran the native arm64 same-shape official compare after the Phase 6 query/index cut:
    - official median `605,762.724 spans/s`, p99 `1.252 ms`
    - Rust disk median `623,163.967 spans/s`, p99 `0.774 ms`
    - throughput ratio `1.0287`, p99 ratio `0.6177`
  - Added a dedicated `vtbench official-query-compare` mode instead of reusing the old memory-only `storage-query` benchmark.
  - Made the query harness benchmark the persisted-data path rather than hot in-memory state:
    - clean start
    - ingest a fixed OTLP protobuf fixture
    - `official` executes `/internal/force_flush`
    - stop process
    - restart on the same data dir
    - benchmark public Jaeger trace search on the reopened dataset
  - Switched the query fixture onto a real wall-clock time window (current time minus 5 minutes) so official no longer rejects the queries as out-of-retention.
  - Re-ran a native arm64 3-round official-vs-disk query-heavy benchmark on the common Jaeger trace-search endpoint:
    - official median `28,022.114 qps`, p99 `2.162 ms`
    - Rust disk median `167,437.718 qps`, p99 `0.275 ms`
    - throughput ratio `5.9752`, p99 ratio `0.1271`
    - output dir `/tmp/rust-vt-official-query-bench-20260409`
  - Wrote a dedicated Chinese harness note under `docs/plans/2026-04-09-official-query-benchmark-harness.md`.
- Files created/modified:
  - crates/vtstorage/src/disk.rs (updated)
  - crates/vtbench/src/main.rs (updated)
  - crates/vtbench/src/preflight.rs (updated)
  - crates/vtbench/src/official_query_compare.rs (created)
  - docs/plans/2026-04-09-official-query-benchmark-harness.md (created)
  - task_plan.md (updated)
  - progress.md (updated)
  - findings.md (updated)

## Verification Log
- `cargo test -p vtstorage recovery_manifest_round_trips_covered_wal_segments -- --nocapture`
- `cargo test -p vtstorage`
- `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- `target/aarch64-apple-darwin/release/vtbench official-compare --official-bin=/tmp/victoria-traces-v0.8.0-arm64/victoria-traces-prod --rust-bin=/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtapi --targets=official,disk --rounds=5 --output-dir=/tmp/rust-vt-official-bench-rotate-only-20260409-1039`
- `cargo test -p vtstorage`
- `cargo build --release --target aarch64-apple-darwin -p vtapi`
- `target/aarch64-apple-darwin/release/vtbench official-compare --official-bin=/tmp/victoria-traces-v0.8.0-arm64/victoria-traces-prod --rust-bin=/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtapi --targets=official,disk --rounds=3 --output-dir=/tmp/rust-vt-official-bench-rotate-only-liveupdate-cut-20260409-1047`
- `cargo test -p vtstorage --test disk_engine_tests disk_engine_can_disable_trace_wal_and_recover_from_parts_only`
- `cargo test -p vtstorage`
- `cargo test -p vtapi`
- `cargo build --release -p vtapi -p vtbench`
- `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- `target/aarch64-apple-darwin/release/vtbench official-compare --official-bin=/tmp/victoria-traces-v0.8.0-arm64/victoria-traces-prod --rust-bin=/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtapi --targets=official,disk --rounds=3 --output-dir=/tmp/rust-vt-official-bench-liveupdate-clean-20260409-arm64`
- `target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13181/v1/traces --duration-secs=4 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/rust-vt-wal-ab-wal-on.json`
- `target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13182/v1/traces --duration-secs=4 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/rust-vt-wal-ab-wal-off.json`
- `cargo test -p vtstorage recovery_manifest_round_trips_covered_rows_segments -- --nocapture`
- `cargo test -p vtstorage --test disk_engine_tests disk_engine_can_disable_trace_wal_and_recover_from_rows_only -- --nocapture`
- `cargo test -p vtstorage`
- `cargo test -p vtapi`
- `cargo build --release -p vtapi -p vtbench`
- `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- `target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13181/v1/traces --duration-secs=4 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/rust-vt-wal-ab2-wal-on.json`
- `target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13182/v1/traces --duration-secs=4 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/rust-vt-wal-ab2-wal-off.json`
- `target/aarch64-apple-darwin/release/vtbench official-compare --official-bin=/tmp/victoria-traces-v0.8.0-arm64/victoria-traces-prod --rust-bin=/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtapi --targets=official,disk --rounds=3 --output-dir=/tmp/rust-vt-official-bench-rows-walfix-20260409`
- `cargo test -p vtstorage search_segment_meta_for_request_matches_exact_trace_filters -- --nocapture`
- `cargo test -p vtstorage observe_persisted_segment_keeps_only_minimal_trace_runtime_indexes -- --nocapture`
- `cargo test -p vtstorage recovery_shard_round_trips_persisted_segment_summaries -- --nocapture`
- `cargo test -p vtstorage disk_engine_writes_recovery_snapshot_and_reopens_without_recreating_segment_meta -- --nocapture`
- `cargo test -p vtstorage disk_engine_searches_traces_by_generic_field_filter_after_reopen -- --nocapture`
- `cargo test -p vtstorage disk_engine_searches_traces_by_operation_name_after_reopen -- --nocapture`
- `cargo test -p vtstorage disk_engine_lists_services_trace_ids_and_field_values_after_reopen -- --nocapture`
- `cargo test -p vtstorage`
- `cargo test -p vtapi`
- `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- `target/aarch64-apple-darwin/release/vtbench official-compare --official-bin=/tmp/victoria-traces-v0.8.0-arm64/victoria-traces-prod --rust-bin=/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtapi --targets=official,disk --rounds=3 --output-dir=/tmp/rust-vt-official-bench-phase6-20260409`
- `cargo test -p vtbench`
- `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- `target/aarch64-apple-darwin/release/vtbench official-query-compare --official-bin=/tmp/victoria-traces-v0.8.0-arm64/victoria-traces-prod --rust-bin=/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtapi --targets=official,disk --rounds=3 --official-port=13181 --disk-port=13183 --fixture-requests=2000 --duration-secs=3 --warmup-secs=1 --concurrency=16 --spans-per-request=5 --payload-variants=256 --output-dir=/tmp/rust-vt-official-query-bench-20260409`
