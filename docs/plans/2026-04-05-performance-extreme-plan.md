# Performance Extreme Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Push `rust_victoria_trace` substantially beyond the current ingest/query throughput while reducing memory and wire/disk overhead, without regressing correctness, durability, or high-availability semantics.

**Architecture:** Keep the current product shape and HA semantics intact, but replace the hottest JSON/string-heavy paths with binary batch paths, reduce per-row index churn with batch publication, and shrink the in-memory index footprint through dictionary/intern-based representations. The work should preserve all existing trace/log APIs, read/write quorum semantics, read repair, rebalance, TLS/mTLS, and current test coverage.

**Tech Stack:** Rust workspace (`vtcore`, `vtingest`, `vtstorage`, `vtquery`, `vtapi`, `vtbench`), `axum`, `reqwest`, `prost`, current disk WAL/part engine, current benchmark harness.

---

## Baseline

Current evidence to preserve for apples-to-apples comparison:

- `storage-ingest --release`: about `99,421 spans/s`, `p99=2.047ms`, `p999=11.238ms`
- `http-ingest --release` with `5 spans/request`: about `50,257 spans/s`, about `10,048 req/s`, `p99=5.368ms`, `p999=7.457ms`

Do not change benchmark semantics before capturing fresh before/after numbers with the same command line.

## Non-Negotiable Constraints

- No regression in current public/internal/admin API behavior.
- No regression in write quorum, read quorum, read repair, rebalance, TLS/mTLS, or cert reload behavior.
- No regression in crash recovery semantics already covered by checksum + sync policy tests.
- Keep all existing workspace tests green while adding targeted performance-regression tests where practical.
- Prefer lower resident memory and lower CPU per ingested row over micro-optimizations that only improve one benchmark.

## Success Criteria

- Increase local `storage-ingest --release` throughput materially, with a target of at least `2x` from the current baseline.
- Increase `http-ingest --release` throughput materially, with a target of at least `2x` spans/s from the current baseline.
- Reduce memory retained by the in-memory/disk index structures for large workloads by at least `30%` in controlled measurement.
- Preserve current HA semantics under the existing cluster integration suite.
- Produce fresh before/after benchmark reports and document any remaining bottlenecks honestly.

## Phase 1: Lock The Evidence Path

### Task 1: Freeze comparable benchmark commands

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/findings.md`

**Step 1: Record the exact baseline commands and their current outputs**

Add a short benchmark baseline section with the current `storage-ingest` and `http-ingest` release commands and outputs.

**Step 2: Add a rule that later benchmark changes must preserve a comparable command path**

Document that benchmark payload shape, concurrency, and request size should stay fixed for before/after comparison.

**Step 3: Re-run the baseline once before code changes**

Run:

```bash
cargo run -p vtbench --release -- storage-ingest --rows=1000 --batch-size=100 --duration-secs=2 --warmup-secs=1 --sample-interval-secs=1
cargo run -p vtbench --release -- http-ingest --requests=200 --spans-per-request=5 --concurrency=32 --duration-secs=2 --warmup-secs=1 --sample-interval-secs=1
```

Expected: fresh numbers recorded before touching the hot path.

## Phase 2: Binaryize The Hottest Write Path

### Task 2: Introduce a binary row batch codec

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/src/codec.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/src/lib.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/tests/codec_tests.rs`

**Step 1: Write the failing test**

Cover round-trip for:

- one `TraceSpanRow`
- a batch of rows
- rows with repeated field names/values
- log-backed rows encoded through the same row shape

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p vtcore --test codec_tests
```

Expected: compile or symbol failure because codec does not exist yet.

**Step 3: Write minimal implementation**

Implement a stable binary batch format with:

- version byte
- row count
- string table for repeated field names/values
- row metadata section
- payload/body section

Do not optimize for perfect compression yet; optimize first for lower CPU and fewer allocations than `serde_json`.

**Step 4: Run test to verify it passes**

Run:

```bash
cargo test -p vtcore --test codec_tests
```

Expected: PASS.

### Task 3: Move storage WAL append from JSON rows to binary batches

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/engine.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Write the failing test**

Add a disk-engine test that appends multiple rows and verifies reopen/recovery still works after replacing WAL row payload encoding.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p vtstorage disk_engine_recovers_rows_after_reopen -- --exact
```

Expected: FAIL after switching the test expectation to the new binary path.

**Step 3: Write minimal implementation**

Change active WAL append from per-row `serde_json::to_vec` to encoded binary batches. Keep:

- checksum protection
- sync policy behavior
- corrupted-tail recovery

Prefer batch framing so one `append_rows` call becomes one encoded record group instead of `N` independent records.

**Step 4: Run storage tests**

Run:

```bash
cargo test -p vtstorage
```

Expected: all storage tests pass.

### Task 4: Move internal cluster replication off JSON row payloads

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`

**Step 1: Write the failing test**

Add an integration test that verifies internal row replication still works when `storage` accepts a binary row-batch payload instead of JSON `RowsIngestRequest`.

**Step 2: Run test to verify it fails**

Run:

```bash
cargo test -p vtapi cluster_insert_and_select_round_trip_with_replication -- --exact
```

Expected: FAIL once the test path is moved to binary internal RPC.

**Step 3: Write minimal implementation**

Change `insert -> storage` replication to:

- send `application/x-vt-row-batch`
- avoid JSON serialization of `Vec<TraceSpanRow>`
- decode directly into the storage append path

**Step 4: Run cluster API tests**

Run:

```bash
cargo test -p vtapi cluster_insert_and_select_round_trip_with_replication -- --exact
cargo test -p vtapi cluster_insert_accepts_partial_replica_failure_when_quorum_is_met -- --exact
cargo test -p vtapi cluster_insert_rejects_when_write_quorum_is_not_met -- --exact
```

Expected: PASS.

## Phase 3: Reduce Write-Path Contention

### Task 5: Batch index publication in the disk engine

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Write the failing test**

Add a regression test or instrumentation assertion showing `append_rows` publishes a whole batch into the state layer once, not row-by-row.

**Step 2: Run test to verify it fails**

Run the new targeted storage test and confirm current row-by-row behavior fails the expectation.

**Step 3: Write minimal implementation**

Refactor `append_rows` to:

- encode/write the batch
- collect row locations
- publish one batch into the in-memory index/state

Avoid per-row lock churn where possible.

**Step 4: Re-run storage suite**

Run:

```bash
cargo test -p vtstorage
```

Expected: PASS.

### Task 6: Introduce group-commit friendly flush behavior

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/main.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Write the failing test**

Add a test covering configured flush/grouping behavior without regressing `sync_data` semantics when durability policy requires it.

**Step 2: Run test to verify it fails**

Run the new targeted storage test.

**Step 3: Write minimal implementation**

Introduce a bounded group-commit policy that:

- preserves durability policy semantics
- allows multiple appenders to share one physical flush window

Do not weaken the current acknowledged-write guarantee.

**Step 4: Re-run storage and API suites**

Run:

```bash
cargo test -p vtstorage
cargo test -p vtapi cluster_metrics_report_remote_io_counters -- --exact
```

Expected: PASS.

## Phase 4: Shrink The In-Memory Index Footprint

### Task 7: Intern repeated strings in the index layer

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/state.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/engine.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/memory_engine_tests.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Write the failing test**

Add a stats-oriented test that ingests many repeated services/operations/field names and asserts the index uses a dictionary/intern pool instead of full string duplication everywhere.

**Step 2: Run test to verify it fails**

Run the new targeted storage test.

**Step 3: Write minimal implementation**

Introduce intern pools for:

- service names
- operation names
- field names
- indexed field values where repetition is high

Keep external APIs string-based; only change internal representation.

**Step 4: Re-run storage and query tests**

Run:

```bash
cargo test -p vtstorage
cargo test -p vtquery
```

Expected: PASS.

### Task 8: Replace expensive set intersections with integer-based postings where possible

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/state.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Write the failing test**

Add a targeted search test that checks repeated field-filter intersections under larger synthetic data.

**Step 2: Run test to verify it fails or times out under the current structure**

Run the targeted test.

**Step 3: Write minimal implementation**

Move hot candidate sets from `BTreeSet<String>` to sorted integer postings or similar compact structure.

**Step 4: Re-run storage/query suites**

Run:

```bash
cargo test -p vtstorage
cargo test -p vtquery
```

Expected: PASS.

## Phase 5: Push Query Work Downstream

### Task 9: Avoid reconstructing full rows until needed

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtquery/src/service.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Write the failing test**

Add a test proving selective decode is used not only for trace rows by location, but also for search-result refinement where only a subset of columns is needed.

**Step 2: Run test to verify it fails**

Run the new targeted storage test.

**Step 3: Write minimal implementation**

Extend part readers so simple field tests can operate on compact column projections instead of materializing full `TraceSpanRow` eagerly.

**Step 4: Re-run storage/query/API suites**

Run:

```bash
cargo test -p vtstorage
cargo test -p vtquery
cargo test -p vtapi search_endpoint_filters_by_generic_field_filter -- --exact
```

Expected: PASS.

## Phase 6: Separate Logs From Trace Semantics Without Forking Storage

### Task 10: Keep shared storage lifecycle, but stop paying trace-only overhead for logs

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/src/model.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtquery/src/service.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
- Test: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`

**Step 1: Write the failing test**

Add a logs-specific regression that ensures the log search path does not require trace-only fields or trace-window semantics unnecessarily.

**Step 2: Run test to verify it fails**

Run the targeted logs test.

**Step 3: Write minimal implementation**

Preserve shared WAL/part/merge lifecycle, but move logs to a lighter logical schema on top of the same physical engine.

**Step 4: Re-run ingest/query/API suites**

Run:

```bash
cargo test -p vtcore
cargo test -p vtquery
cargo test -p vtapi
```

Expected: PASS.

## Phase 7: Benchmark, Compare, And Decide The Next Ceiling

### Task 11: Re-run full before/after benchmarks

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/findings.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/var/bench-storage-ingest-after.json`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/var/bench-http-ingest-after.json`

**Step 1: Run release benchmarks with the same command line as baseline**

Run:

```bash
cargo run -p vtbench --release -- storage-ingest --rows=1000 --batch-size=100 --duration-secs=2 --warmup-secs=1 --sample-interval-secs=1 --report-file=./var/bench-storage-ingest-after.json
cargo run -p vtbench --release -- http-ingest --requests=200 --spans-per-request=5 --concurrency=32 --duration-secs=2 --warmup-secs=1 --sample-interval-secs=1 --report-file=./var/bench-http-ingest-after.json
```

**Step 2: Compare before/after**

Record:

- spans/s
- req/s
- p99
- p999
- error count

**Step 3: Document memory/resource findings**

Note measured or inferred changes in:

- CPU cost per row
- wire size per replicated batch
- persisted bytes per ingested workload
- index memory footprint

**Step 4: Re-run full verification**

Run:

```bash
cargo fmt --all
cargo test --workspace
```

Expected: full workspace remains green after all performance work.

## Expected ROI By Stage

| Stage | Expected Throughput Uplift | Expected Resource Benefit | Risk |
|------|----------------------------|---------------------------|------|
| Binary WAL + binary internal RPC | High, likely `1.5x-2.5x` | Lower CPU, lower wire bytes, lower WAL bytes | Medium |
| Batch publish + group commit | Medium to high | Lower lock contention, better p99 | Medium |
| String intern + integer postings | Medium | Much lower memory and faster intersections | Medium |
| Query pushdown / less row materialization | Medium | Lower read IO and lower CPU on search | Medium |
| Native logs schema on shared storage lifecycle | Low to medium on trace, medium on logs | Lower index and field overhead for logs | Medium |

## What Not To Spend Time On First

- Swapping the web framework
- Tiny hash-function tweaks
- Tokio tuning without removing hot-path serialization
- Hand-written unsafe micro-optimizations

Those are downstream optimizations. They should wait until the heavy JSON/string/lock costs are gone.

## Verification Matrix

The following must remain green after each phase:

- `cargo test --workspace`
- trace HTTP ingest/query regressions
- logs HTTP/gRPC ingest/query regressions
- quorum success/failure regressions
- read repair and rebalance regressions
- TLS/mTLS and cert-reload regressions

## Recommended Execution Order

1. Phase 1 baseline freeze
2. Phase 2 binaryize write path
3. Phase 3 reduce write-path contention
4. Phase 4 shrink memory/index overhead
5. Phase 5 push query work downstream
6. Phase 6 separate logs from trace-only overhead
7. Phase 7 benchmark and document

This order maximizes ROI while minimizing the chance of breaking HA semantics early.
