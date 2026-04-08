# Disk Next Hotspots Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Re-establish a clearly visible throughput and p99 lead over official VictoriaTraces after dropping the failed packet-path experiment, without changing the benchmark shape or reintroducing rejected async/outer-batching designs.

**Architecture:** Keep the current `WAL-first + per-shard consuming head + background seal + trace-by-id head+sealed federation` envelope, but remove more synchronous CPU from the ack path. The recommended path is to split the head index into a slim trace-by-id index on the write path and defer service/operation/field indexing to seal time. This attacks the current hot cost in `ActiveSegment::append_block_rows` and `SegmentAccumulator::ingest_prepared_block_rows` without reopening the failed packet handoff path.

**Tech Stack:** Rust, `vtstorage`, `vtapi`, `vtbench`, OTLP protobuf ingest, Apple Silicon native-arm64 benchmark runs.

---

## Current Read Of The Hot Path

The accepted mainline still does the following on every acknowledged trace append:

1. route/coalesce `TraceBlock`s by shard in [`disk.rs`](../../crates/vtstorage/src/disk.rs)
2. build a `PreparedTraceBlockAppend`
3. append a WAL batch into `ActiveSegment`
4. build row refs plus trace/service/operation/field metadata inside `SegmentAccumulator`
5. flush the active segment as part of group commit

The packet-path experiment failed because it changed the handoff shape but did not improve the real physical or indexing kernel under load. The current remaining structural wall is now the synchronous head indexing work performed in:

- [`ActiveSegment::append_block_rows`](../../crates/vtstorage/src/disk.rs)
- [`SegmentAccumulator::ingest_prepared_block_rows`](../../crates/vtstorage/src/disk.rs)
- [`SegmentAccumulator::ingest_prepared_block_row_run`](../../crates/vtstorage/src/disk.rs)

That code currently interns and deduplicates:

- trace ids
- service names
- operation names
- indexed field name/value pairs

for every acked batch, even though the only head-side query path that is guaranteed today is `trace-by-id`.

## Alternative Schemes Reviewed

### Option 1: Slim Head Index, Defer Search Metadata To Seal

Recommendation: **do this next**

What changes:

- Head append keeps only what `trace-by-id` needs immediately:
  - trace window
  - row refs
  - row payload cache
- service / operation / field indexes are no longer maintained on the ack path
- final searchable metadata is built when sealing the head into a part

Why this is attractive:

- removes the highest synchronous CPU that still remains in the current accepted path
- preserves current benchmark shape and current `trace-by-id` semantics
- does not depend on a new cross-layer handoff object
- keeps write-path complexity lower than a direct decode sink rewrite

Risk:

- `search/services/field-values` stay sealed-only until a later federation project
- seal cost rises, so seal metrics must be watched carefully

Expected upside:

- this is the only near-term change with a realistic chance to produce a visible step-up under `sync_policy=None` rather than only a `sync_policy=Data` win

### Option 2: Direct Decode Sink Into Head Pages

Recommendation: **hold for later**

What changes:

- OTLP protobuf decode would write directly into a storage-owned append sink
- this removes `TraceBlock` materialization and the block-level prepare pass entirely

Why it is not next:

- overlaps conceptually with the failed packet-path direction
- requires new API/storage ownership boundaries
- easy to get wrong on sharding, backpressure, and benchmark reproducibility

Risk:

- very high implementation and debugging cost
- easy to regress semantics and silently drift benchmark shape

Expected upside:

- potentially large, but only worth attempting if Option 1 stalls

### Option 3: Stronger Commit Epoch / Adaptive Linger / More Seal Tuning

Recommendation: **keep as supporting work, not the headline**

What changes:

- continue tuning group commit epoch size
- adapt linger only under load
- raise or reshape head seal thresholds

Why it is not enough alone:

- most of the upside shows up under `sync_policy=Data`
- current headline benchmark is still dominated by `sync_policy=None`
- this is more likely to harvest mid-single-digit gain than produce a decisive leap

Risk:

- low

Expected upside:

- useful once the ack-path CPU wall is reduced

## Recommended Execution Order

1. Implement Option 1 on a feature branch.
2. Benchmark it with the exact same ARM64 benchmark shape and `vtbench compare` gate.
3. Only if Option 1 fails to create a visible margin, revisit Option 2.
4. Keep Option 3 as a follow-up tuning lane after Option 1, not before it.

### Task 1: Lock The Benchmark Gate Before The Next Rewrite

**Files:**
- Modify: `crates/vtbench/src/main.rs`
- Modify: `docs/benchmarks/baselines/disk-otlp-protobuf-load-arm64-fresh-single.json`
- Modify: `docs/benchmarks/baselines/disk-otlp-protobuf-load-arm64-fresh-5round-median.json`
- Test: `crates/vtbench/tests/compare_tests.rs`

**Step 1: Recreate a clean accepted baseline artifact**

Run:

```bash
cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench
```

Expected: native ARM64 `vtapi` and `vtbench` binaries.

**Step 2: Run one clean single and one clean 5-round median on the accepted mainline**

Run:

```bash
target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13083/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/disk-next-hotspots-single.json
```

Expected: `errors=0`, native ARM64 report saved.

**Step 3: Refresh or confirm the baseline artifacts only if they still match the accepted path**

Expected: the compare gate is ready before changing storage logic again.

### Task 2: Add Red Tests For Slim Head Semantics

**Files:**
- Modify: `crates/vtstorage/tests/disk_engine_tests.rs`
- Modify: `crates/vtapi/tests/http_api_tests.rs`

**Step 1: Add a test that `trace-by-id` sees head rows without search metadata**

Test intent:

- append rows into disk head
- assert `trace_window` and `rows_for_trace` work immediately
- assert head rows do not need service/operation/field search indexes on the ack path

**Step 2: Add a test that sealed parts still expose service / field metadata after seal**

Test intent:

- append rows
- force seal
- assert `list_services`, `list_field_names`, `list_field_values`, and `search_traces` still work after sealing

**Step 3: Run the targeted tests and verify the old synchronous index assumption now fails**

Run:

```bash
cargo test -p vtstorage disk_engine_trace_by_id_reads_head_and_sealed_parts_together -- --exact
cargo test -p vtstorage disk_engine_reports_head_group_commit_and_seal_metrics -- --exact
```

Expected: at least one new red test before the implementation lands.

### Task 3: Split The Head Accumulator Into Slim And Full Paths

**Files:**
- Modify: `crates/vtstorage/src/disk.rs`
- Test: `crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Introduce a slim head accumulator data model**

Required behavior:

- track `trace_id -> trace window`
- track `trace_id -> row refs`
- keep row payload cache and cached payload ranges as-is
- do not maintain service / operation / field indexes during head append

**Step 2: Route `ActiveSegment::append_block_rows` through the slim path**

Required behavior:

- keep WAL bytes and row refs identical
- stop calling the metadata-heavy indexing path on ack

**Step 3: Keep head `trace-by-id` federation unchanged**

Required behavior:

- `trace_window`
- `rows_for_trace`
- `list_trace_ids`

must keep working for head + sealing + sealed parts together.

**Step 4: Run focused tests**

Run:

```bash
cargo test -p vtstorage disk_engine_trace_by_id_reads_head_and_sealed_parts_together -- --exact
cargo test -p vtstorage disk_engine_recovers_rows_after_reopen -- --exact
```

Expected: `trace-by-id` behavior is unchanged.

### Task 4: Move Search Metadata Build To Seal

**Files:**
- Modify: `crates/vtstorage/src/disk.rs`
- Test: `crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Build final service / operation / field metadata during seal**

Required behavior:

- sealing scans the head rows once
- generated `SegmentMeta` remains functionally equivalent to current sealed metadata

**Step 2: Publish sealed metadata into shard state exactly once**

Required behavior:

- no duplicate search metadata
- reopen / recovery still reconstructs the same shard-visible search state

**Step 3: Run seal and reopen tests**

Run:

```bash
cargo test -p vtstorage disk_engine_compacts_small_parts_after_reopen -- --exact
cargo test -p vtstorage disk_engine_lists_services_trace_ids_and_field_values_after_reopen -- --exact
cargo test -p vtstorage disk_engine_searches_traces_by_operation_name_after_reopen -- --exact
```

Expected: sealed behavior remains intact.

### Task 5: Expose New Metrics To Prove The CPU Shift

**Files:**
- Modify: `crates/vtstorage/src/engine.rs`
- Modify: `crates/vtstorage/src/disk.rs`
- Modify: `crates/vtapi/src/app.rs`
- Test: `crates/vtapi/tests/http_api_tests.rs`

**Step 1: Add metrics that separate head-path indexing from seal-path indexing**

Recommended counters:

- `trace_head_rows_indexed_total`
- `trace_head_metadata_rows_deferred_total`
- `trace_seal_metadata_rows_built_total`
- `trace_seal_metadata_build_micros_total`

**Step 2: Expose them in `/metrics`**

Expected: benchmark runs can prove that metadata work moved off the ack path instead of disappearing from visibility.

### Task 6: Run The Full Benchmark Gate

**Files:**
- Modify: `docs/2026-04-06-otlp-ingest-performance-report.md` if and only if the candidate wins
- Modify: `docs/handoffs/HANDOFF-2026-04-06-rust-vt-head-segment-leap.md` with outcome notes

**Step 1: Run clean native ARM single**

Run:

```bash
target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13083/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/disk-next-hotspots-candidate-single.json
target/aarch64-apple-darwin/release/vtbench compare --baseline-file=docs/benchmarks/baselines/disk-otlp-protobuf-load-arm64-fresh-single.json --candidate-file=/tmp/disk-next-hotspots-candidate-single.json
```

Expected: pass, `errors=0`.

**Step 2: Run clean native ARM 5-round median**

Run the same benchmark shape five times from clean server starts, save the median report, then compare:

```bash
target/aarch64-apple-darwin/release/vtbench compare --baseline-file=docs/benchmarks/baselines/disk-otlp-protobuf-load-arm64-fresh-5round-median.json --candidate-file=/tmp/disk-next-hotspots-candidate-median.json
```

Expected: pass, with a visible margin over the accepted baseline and still ahead of official.

**Step 3: Stop if the candidate only wins single-run noise**

Reject the candidate if any of the following happen:

- single-run up, median flat
- throughput gain comes with material `p99` regression
- non-zero errors appear in the report
- metrics show seal backlog exploding

### Task 7: Only If Option 1 Wins, Start Option 3

**Files:**
- Modify: `crates/vtstorage/src/disk.rs`
- Test: `crates/vtstorage/tests/disk_engine_tests.rs`

**Step 1: Tune commit epoch and seal thresholds against the slimmer head**

Only do this after the slim head path wins. The point is to let the lower-CPU head make better use of existing group commit and larger heads before trying any riskier decode-path rewrite.

## Stop Conditions

- If slim-head indexing does not produce a visible benchmark step-up, do not immediately retry packet-like or direct-decode designs in the same branch.
- If benchmark reports show non-zero errors, fix the harness or runtime stability issue before drawing throughput conclusions.
- If seal-time metadata reconstruction makes `trace-by-id` or recovery semantics worse, revert and reassess before tuning.

## Why This Is The Best Next Bet

This plan attacks the hottest remaining synchronous work that still obviously exists in the current accepted mainline, while preserving:

- benchmark shape
- native ARM benchmark discipline
- `trace-by-id` near-realtime visibility
- current WAL-first durability model
- the already accepted head+sealed federation for direct trace reads

It also avoids the two failure modes that have already cost time:

- cross-layer ingest handoff redesign before the kernel proves out
- queueing/control-plane changes that do not reduce the actual per-row write kernel
