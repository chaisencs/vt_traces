# Progress Log

## Session: 2026-04-04

### Phase 1: Requirements & Discovery
- **Status:** complete
- **Started:** 2026-04-04 15:30
- Actions taken:
  - Inspected current workspace and confirmed `rust_victoria_trace` is empty.
  - Verified Rust toolchain availability.
  - Checked upstream architecture notes and prior VictoriaTraces/VictoriaLogs research context.
- Files created/modified:
  - `rust_victoria_trace/task_plan.md` (created)
  - `rust_victoria_trace/findings.md` (created)
  - `rust_victoria_trace/progress.md` (created)

### Phase 2: Planning & Structure
- **Status:** complete
- Actions taken:
  - Defined a staged rewrite strategy centered on a first single-node vertical slice.
  - Prepared a dedicated implementation plan document under `docs/plans`.
  - Created a Cargo workspace and five Rust crates.
- Files created/modified:
  - `docs/plans/2026-04-04-rust-victoria-trace-rewrite.md`
  - `rust_victoria_trace/Cargo.toml`
  - `rust_victoria_trace/crates/*`

### Phase 3: Implementation
- **Status:** complete
- Actions taken:
  - Implemented shared trace row and trace window types in `vtcore`.
  - Implemented OTLP-like JSON flattening in `vtingest`.
  - Implemented `StorageEngine` trait and `MemoryStorageEngine` in `vtstorage`.
  - Implemented `QueryService` in `vtquery`.
  - Implemented `axum` router and handlers in `vtapi`.
- Files created/modified:
  - `rust_victoria_trace/crates/vtcore/src/*`
  - `rust_victoria_trace/crates/vtingest/src/*`
  - `rust_victoria_trace/crates/vtstorage/src/*`
  - `rust_victoria_trace/crates/vtquery/src/*`
  - `rust_victoria_trace/crates/vtapi/src/*`

### Phase 4: Testing & Verification
- **Status:** complete
- Actions taken:
  - Wrote test-first suites for `vtcore`, `vtingest`, `vtstorage`, `vtquery`, and `vtapi`.
  - Verified `healthz`.
  - Verified ingest -> query HTTP round-trip for one trace.
  - Verified invalid ingest is returned as HTTP 400 instead of panicking.
  - Added architecture document and parity roadmap.
- Files created/modified:
  - `rust_victoria_trace/crates/vtcore/tests/model_tests.rs`
  - `rust_victoria_trace/crates/vtingest/tests/flatten_tests.rs`
  - `rust_victoria_trace/crates/vtstorage/tests/memory_engine_tests.rs`
  - `rust_victoria_trace/crates/vtquery/tests/query_service_tests.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/docs/architecture.md`

### Phase 5: Cluster Foundation
- **Status:** complete
- Actions taken:
  - Added a cluster config and stable trace-aware replica placement.
  - Split HTTP roles into single-node, `storage`, `insert`, and `select` routers.
  - Added internal storage RPC routes for replicated row append and remote query.
  - Implemented replica fan-out on ingest and replica fallback on trace reads.
  - Added cluster-specific Prometheus-style metrics and integration tests.
  - Extended the `vtapi` binary to select role and bind address from environment.
- Files created/modified:
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/lib.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/docs/architecture.md`
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/findings.md`

### Phase 6: Storage Core Hardening
- **Status:** complete
- Actions taken:
  - Replaced the old single-file JSONL disk engine with a segment-based row store.
  - Added per-segment sidecar metadata files and restart recovery from segment metadata.
  - Changed the disk engine to keep a thin in-memory index and read persisted rows on demand.
  - Added `segment_count` storage stats and exposed them through `/metrics`.
  - Added configurable cluster write quorum and degraded-success ingest behavior.
  - Extended cluster tests to cover quorum success and quorum failure cases.
  - Split disk persistence into active WAL files and sealed `.part` files with a magic header.
  - Batched disk row reads per segment instead of reopening files for each row.
- Files created/modified:
  - `rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `rust_victoria_trace/crates/vtstorage/src/engine.rs`
  - `rust_victoria_trace/crates/vtstorage/src/lib.rs`
  - `rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/docs/architecture.md`
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/findings.md`

### Phase 7: Jaeger Query Compatibility
- **Status:** complete
- Actions taken:
  - Added Jaeger-compatible query routes on single-node and `select` roles.
  - Implemented Jaeger services listing, service operations listing, trace lookup, and trace search envelopes.
  - Reused existing trace search and replica-read paths for cluster-compatible Jaeger responses.
  - Added regression tests for Jaeger trace lookup, service operations, and operation-filtered trace search.
  - Updated README capability and endpoint documentation to reflect the new query surface.
- Files created/modified:
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/progress.md`

### Phase 8: Compaction & Node Governance
- **Status:** complete
- Actions taken:
  - Added small-part compaction for sealed disk segments on reopen and after segment rotation.
  - Kept compaction conservative by touching only sealed `.part` files and rebuilding index state from disk after merges.
  - Added cluster node failure backoff and temporary quarantine to avoid repeatedly probing failed nodes.
  - Exposed new cluster metrics for skipped remote probes, quarantined nodes, and node quarantine events.
  - Added regression tests for automatic part compaction and failed-node quarantine between reads.
- Files created/modified:
  - `rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/progress.md`

### Phase 9: Selective Decode, Pruning & HA Control Loops
- **Status:** complete
- Actions taken:
  - Changed sealed `.part` trace reads from whole-part materialization to row-targeted selective decode.
  - Added a storage counter for part selective decodes and exposed it through `/metrics`.
  - Added per-trace operation sets and bloom hints so trace search can prune by time range, service name, and operation name before row fetch.
  - Threaded operation-name filtering through native search and Jaeger trace search query paths.
  - Added configurable cluster read quorum semantics for trace reads.
  - Added read repair after successful quorum reads when a replica is missing or stale.
  - Added internal trace ownership listing and an admin rebalance endpoint to repair missing desired replicas across the cluster.
  - Added regression tests for selective part decode, operation-name search after reopen, read quorum enforcement, read repair, and rebalance.
- Files created/modified:
  - `rust_victoria_trace/crates/vtcore/src/model.rs`
  - `rust_victoria_trace/crates/vtstorage/src/bloom.rs`
  - `rust_victoria_trace/crates/vtstorage/src/lib.rs`
  - `rust_victoria_trace/crates/vtstorage/src/engine.rs`
  - `rust_victoria_trace/crates/vtstorage/src/memory.rs`
  - `rust_victoria_trace/crates/vtstorage/src/state.rs`
  - `rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
  - `rust_victoria_trace/crates/vtquery/tests/query_service_tests.rs`
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/task_plan.md`

### Phase 10: Production P0 Hardening
- **Status:** complete
- Actions taken:
  - Re-audited the current implementation against the user's production bar: 200k-QPS ambition, durability, non-disruptive node loss, and real cluster replication.
  - Confirmed that sharding, replication, read quorum, read repair, and admin rebalance already exist, but durability, overload control, topology awareness, and benchmark evidence remain the P0 gaps.
  - Added disk sync policy support, `fsync` counters, checksum-aware WAL records, and corrupted WAL tail recovery.
  - Added API body-size limits and fail-fast concurrency rejection on routers.
  - Added topology-aware replica placement and a background rebalance poller for `select`.
  - Added `vtbench` with `storage-ingest`, `storage-query`, and `http-ingest` modes and smoke-ran all three.
  - Re-ran fresh crate-specific and workspace-wide verification after the hardening changes.
- Files created/modified:
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/findings.md`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/Cargo.toml`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/docs/architecture.md`
  - `rust_victoria_trace/crates/vtstorage/Cargo.toml`
  - `rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `rust_victoria_trace/crates/vtstorage/src/engine.rs`
  - `rust_victoria_trace/crates/vtstorage/src/lib.rs`
  - `rust_victoria_trace/crates/vtstorage/src/memory.rs`
  - `rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
  - `rust_victoria_trace/crates/vtapi/Cargo.toml`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/lib.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/crates/vtbench/Cargo.toml`
  - `rust_victoria_trace/crates/vtbench/src/main.rs`

### Phase 11: Replica Governance, Auth Boundaries & Better Performance Evidence
- **Status:** complete
- Actions taken:
  - Replaced the old placement path with weighted rendezvous hashing and optional per-node weights.
  - Preserved topology-aware spread, but made ownership movement smaller when nodes are added.
  - Made cluster replica writes fan out concurrently and allowed trace reads to return from fast replicas without waiting for slower ones.
  - Added bearer-token boundaries for public, internal, and admin routes, and threaded internal auth through cluster RPCs.
  - Added standard OTLP/HTTP JSON ingest on `POST /v1/traces`.
  - Extended `vtbench` with duration-driven runs and latency percentile reporting.
  - Hardened the concurrency-limit regression so the full workspace suite stays stable.
  - Re-ran fresh workspace verification and refreshed benchmark smoke outputs.
- Files created/modified:
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/docs/architecture.md`
  - `rust_victoria_trace/crates/vtapi/Cargo.toml`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/lib.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/crates/vtbench/src/main.rs`

### Phase 12: OTLP Protobuf, TLS & Membership Control Plane
- **Status:** complete
- Actions taken:
  - Added OTLP/HTTP protobuf decoding and encoding helpers in `vtingest`.
  - Switched `/v1/traces` and the legacy ingest handler to content-type aware OTLP/HTTP request decoding.
  - Added optional HTTPS / mTLS server wiring with a shared `serve_app` path used by both tests and the runtime binary.
  - Added HTTPS / mTLS cluster client wiring for `insert`, `select`, and background admin pollers.
  - Added `GET /admin/v1/cluster/members`, active storage health probing, new membership metrics, and configurable background membership refresh polling.
  - Added end-to-end tests for OTLP protobuf ingest, HTTPS, mTLS, and cluster members control-plane visibility.
  - Re-ran fresh crate-specific and workspace-wide verification after the protocol and control-plane changes.
- Files created/modified:
  - `rust_victoria_trace/Cargo.toml`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/docs/architecture.md`
  - `rust_victoria_trace/crates/vtingest/Cargo.toml`
  - `rust_victoria_trace/crates/vtingest/src/lib.rs`
  - `rust_victoria_trace/crates/vtingest/src/proto.rs`
  - `rust_victoria_trace/crates/vtingest/tests/protobuf_tests.rs`
  - `rust_victoria_trace/crates/vtapi/Cargo.toml`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/lib.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/src/server.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`

### Phase 13: Distributed Governance & Benchmark Evidence
- **Status:** complete
- Actions taken:
  - Added explicit control-plane node configuration and local control-node identity to cluster config.
  - Added deterministic leader election for `select` control nodes and leader-gated rebalance enforcement.
  - Added `GET /admin/v1/cluster/leader`, background control refresh support, and control-plane probe / leader-change metrics.
  - Added integration tests for local leader election and follower rebalance rejection.
  - Added a reloading cluster HTTP client wrapper so `insert`, `select`, and admin pollers can pick up rotated CA bundles and mTLS identities without restart.
  - Added an end-to-end regression test proving `select` recovers remote reads after cluster client cert/key files are rotated in place.
  - Extended the control plane with `GET /admin/v1/cluster/state`, epoch-bearing peer snapshots, and peer-state anti-entropy so a `select` node can absorb fresher leader views from control peers.
  - Tightened disk `.part` encoding for dynamic fields by promoting homogeneous `bool` and `i64` columns into typed column layouts instead of always using string dictionaries.
  - Extended `vtbench` with `--warmup-secs` and `--report-file`, and verified benchmark reports can be written to disk.
  - Refreshed README capability and deployment guidance so the old `当前限制` section is replaced by production deployment advice.

### Phase 17: Trace Microbatch Leap
- **Status:** in_progress
- **Started:** 2026-04-06 02:30
- Actions taken:
  - Wrote the trace microbatch design under `docs/plans/2026-04-06-trace-microbatch-design.md` before implementation.
  - Added Step 0 metrics for retained trace blocks, trace batch queue depth, flush counts, input/output block counts, and batch wait totals/max.
  - Replaced the old trace pass-through batching path with a shard-local combiner and added tests for staggered-request coalescing and passthrough engines.
  - Verified that wait-based batch formation (`50us` / `100us`) increases batch ratio but hurts throughput, so the current mainline stays on `VT_STORAGE_BATCH_MAX_WAIT_MICROS=0`.
  - Added engine-specific trace batch payload modes so memory still coalesces trace blocks while disk now receives passthrough multi-block appends.
  - Removed `PreparedTraceBlockAppend`'s retained source `TraceBlock` from the disk prepare path and kept only prepared metadata plus encoded row payloads.
  - Tried a deeper disk prepared-row-batch fusion experiment, benchmarked it, and reverted it after it regressed ingest materially.
  - Re-ran same-host `vtbench otlp-protobuf-load` after each valid optimization round and kept only the best-performing branch state.
- Latest same-host fresh single run:
  - official: `399255.205 spans/s`, `p99=0.675ms`
  - memory: `275513.872 spans/s`, `p99=0.890ms`
  - disk: `361882.872 spans/s`, `p99=0.673ms`
- Step 0 metric snapshots on the current best branch:
  - memory: `input_blocks=220448`, `output_blocks=102986`, `retained_trace_blocks=102986`
  - disk: `input_blocks=289504`, `output_blocks=289504`, `flushes=183921`
- Current interpretation:
  - The shared batching-layer route is real: disk recovered from the failed async-worker detour and gained another step through combiner + disk passthrough.
  - The remaining gap to official is no longer primarily batch formation; it is inside the disk append kernel itself, especially WAL append / segment maintenance / live-index update cost.
- Files created/modified:
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/findings.md`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/crates/vtapi/src/cluster.rs`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/http_client.rs`
  - `rust_victoria_trace/crates/vtapi/src/lib.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/crates/vtstorage/src/engine.rs`
  - `rust_victoria_trace/crates/vtstorage/src/memory.rs`
  - `rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
  - `rust_victoria_trace/crates/vtbench/src/main.rs`
  - `rust_victoria_trace/var/bench-storage-ingest.json`

### Phase 14: Bench Soak/Fault Evidence
- **Status:** in_progress
- Actions taken:
  - Extended `vtbench` with `--sample-interval-secs`, `--fault-after-secs`, and `--fault-duration-secs`.
  - Added `p999` latency output and machine-readable timeline buckets to benchmark reports.
  - Smoke-ran an `http-ingest` benchmark with an injected outage window and wrote the report to `var/bench-http-fault.json`.
- Files created/modified:
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/findings.md`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/crates/vtbench/src/main.rs`
  - `rust_victoria_trace/var/bench-http-fault.json`

### Phase 15: Control Journal & OTLP Logs
- **Status:** complete
- Actions taken:
  - Added `LogRow` / `LogSearchRequest` shared models and encoded OTLP logs onto the existing shared storage core by translating log records into storage-compatible structured rows.
  - Added OTLP logs JSON/protobuf/gRPC codecs and flattening in `vtingest`.
  - Added local and clustered logs ingest/search API paths on `vtapi`.
  - Added control-plane journal state, `GET /admin/v1/cluster/journal`, `POST /admin/v1/cluster/journal/append`, leader-event journal replication, and peer journal catch-up from fresher snapshots.
  - Re-ran fresh formatting and full workspace verification after the control-plane and logs changes.
- Files created/modified:
  - `rust_victoria_trace/README.md`
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/findings.md`
  - `rust_victoria_trace/crates/vtcore/src/lib.rs`
  - `rust_victoria_trace/crates/vtcore/src/model.rs`
  - `rust_victoria_trace/crates/vtcore/tests/model_tests.rs`
  - `rust_victoria_trace/crates/vtingest/src/lib.rs`
  - `rust_victoria_trace/crates/vtingest/src/flatten.rs`
  - `rust_victoria_trace/crates/vtingest/src/logs.rs`
  - `rust_victoria_trace/crates/vtingest/src/logs_proto.rs`
  - `rust_victoria_trace/crates/vtingest/tests/flatten_tests.rs`
  - `rust_victoria_trace/crates/vtingest/tests/protobuf_tests.rs`
  - `rust_victoria_trace/crates/vtquery/src/service.rs`
  - `rust_victoria_trace/crates/vtquery/tests/query_service_tests.rs`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`

### Phase 16: Disk Ingest Hot-Path Reuse
- **Status:** complete
- Actions taken:
  - Reconfirmed that `vtbench otlp-protobuf-load` is a pure ingest benchmark and does not pay `drain_pending_trace_updates()` on the write path.
  - Changed the disk append path to reuse the `SegmentAccumulator::observe_prepared_block_rows` scan for live-update construction instead of rescanning `prepared_rows` after append.
  - Kept the request path synchronous and avoided reviving the rejected async shard-worker path.
  - Added a regression test proving `SegmentAccumulator` now returns the same live updates the old rescan helper built.
  - Re-ran fresh release builds plus same-host same-shape fresh single-run and 5-round stability benchmarks against official, memory, and disk.
- Files created/modified:
  - `rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/findings.md`

### Phase 17: Trace Microbatch Leap
- **Status:** in_progress
- Actions taken:
  - Wrote a bounded design for the batching-layer trace microbatch route under `docs/plans/2026-04-06-trace-microbatch-design.md`.
  - Started Step 0 by extending storage stats with retained-trace-block and trace-batch formation fields.
  - Started Step 1 by replacing the old trace pass-through in `BatchingStorageEngine` with per-shard trace batch workers and worker-side block coalescing.
- Files created/modified:
  - `rust_victoria_trace/docs/plans/2026-04-06-trace-microbatch-design.md`
  - `rust_victoria_trace/crates/vtstorage/src/engine.rs`
  - `rust_victoria_trace/crates/vtstorage/src/batching.rs`
  - `rust_victoria_trace/crates/vtstorage/src/memory.rs`
  - `rust_victoria_trace/crates/vtstorage/src/state.rs`
  - `rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `rust_victoria_trace/crates/vtapi/src/app.rs`
  - `rust_victoria_trace/crates/vtapi/src/main.rs`
  - `rust_victoria_trace/crates/vtstorage/tests/batching_engine_tests.rs`
  - `rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
  - `rust_victoria_trace/progress.md`
  - `rust_victoria_trace/task_plan.md`
  - `rust_victoria_trace/findings.md`

## Test Results
| Test | Input | Expected | Actual | Status |
|------|-------|----------|--------|--------|
| Rust toolchain availability | `rustc --version` | Installed toolchain | `rustc 1.73.0` | PASS |
| Cargo availability | `cargo --version` | Installed cargo | `cargo 1.73.0` | PASS |
| `vtcore` tests | `cargo test -p vtcore` | Shared model passes | 4 tests passed | PASS |
| `vtingest` tests | `cargo test -p vtingest` | Flattening + protobuf codec pass | 7 tests passed | PASS |
| `vtstorage` tests | `cargo test -p vtstorage` | Storage engines pass | 14 tests passed | PASS |
| `vtquery` tests | `cargo test -p vtquery` | Query service passes | 8 tests passed | PASS |
| `vtapi` tests | `cargo test -p vtapi` | API passes | 53 tests passed | PASS |
| Workspace tests | `cargo test --workspace` | All crates and tests pass | all workspace tests passed | PASS |
| Cluster round-trip | `cargo test -p vtapi cluster_insert_and_select_round_trip_with_replication -- --exact` | insert/select/storage replicate and query | 1 test passed | PASS |
| Cluster failover | `cargo test -p vtapi cluster_select_reads_from_replica_when_primary_is_down -- --exact` | select falls back to surviving replica | 1 test passed | PASS |
| Cluster metrics | `cargo test -p vtapi cluster_metrics_report_remote_io_counters -- --exact` | role metrics expose remote IO counters | 1 test passed | PASS |
| Segment rotation | `cargo test -p vtstorage disk_engine_rotates_segments_and_recovers_indexes -- --exact` | disk engine rotates segments and recovers metadata | 1 test passed | PASS |
| Storage suite | `cargo test -p vtstorage` | storage engines pass full suite | 6 tests passed | PASS |
| Storage metrics | `cargo test -p vtapi metrics_endpoint_exposes_storage_counters -- --exact` | API metrics expose new storage segment counters | 1 test passed | PASS |
| Quorum success | `cargo test -p vtapi cluster_insert_accepts_partial_replica_failure_when_quorum_is_met -- --exact` | insert succeeds when quorum is met | 1 test passed | PASS |
| Quorum failure | `cargo test -p vtapi cluster_insert_rejects_when_write_quorum_is_not_met -- --exact` | insert rejects when quorum is missed | 1 test passed | PASS |
| Sealed part header | `cargo test -p vtstorage disk_engine_rotates_segments_and_recovers_indexes -- --exact` | sealed part files carry custom part magic and recover correctly | 1 test passed | PASS |
| Read batching | `cargo test -p vtstorage disk_engine_batches_row_reads_per_segment -- --exact` | trace row loads batch per segment | 1 test passed | PASS |
| Jaeger compatibility | `cargo test -p vtapi jaeger_` | Jaeger query compatibility passes | 3 tests passed | PASS |
| Part compaction | `cargo test -p vtstorage disk_engine_compacts_small_parts_after_reopen -- --exact` | small sealed parts compact after reopen without losing trace data | 1 test passed | PASS |
| Node quarantine | `cargo test -p vtapi cluster_select_quarantines_failed_node_between_reads -- --exact` | failed node is quarantined and skipped during backoff window | 1 test passed | PASS |
| Selective decode | `cargo test -p vtstorage disk_engine_uses_selective_decode_for_part_reads -- --exact` | sealed part reads use row-targeted decode instead of whole-part materialization | 1 test passed | PASS |
| Operation search | `cargo test -p vtstorage disk_engine_searches_traces_by_operation_name_after_reopen -- --exact` | disk-backed trace search prunes and finds traces by operation name after reopen | 1 test passed | PASS |
| Read quorum | `cargo test -p vtapi cluster_select_enforces_read_quorum_for_trace_reads -- --exact` | trace reads fail when non-empty quorum is not satisfied | 1 test passed | PASS |
| Read repair | `cargo test -p vtapi cluster_select_repairs_missing_replica_after_successful_read -- --exact` | successful quorum read repairs missing replica copies | 1 test passed | PASS |
| Rebalance | `cargo test -p vtapi cluster_rebalance_repairs_missing_desired_replica -- --exact` | admin rebalance repairs missing desired replica placement | 1 test passed | PASS |
| Native operation search | `cargo test -p vtapi search_endpoint_filters_by_operation_name -- --exact` | native HTTP trace search filters by operation name | 1 test passed | PASS |
| WAL tail recovery | `cargo test -p vtstorage disk_engine_recovers_from_corrupted_wal_tail -- --exact` | corrupted WAL tail is truncated and earlier rows survive reopen | 1 test passed | PASS |
| Fsync policy | `cargo test -p vtstorage disk_engine_records_fsync_operations_when_sync_policy_requires_it -- --exact` | sync policy records actual fsync operations | 1 test passed | PASS |
| Body limit | `cargo test -p vtapi ingest_rejects_payloads_over_body_limit -- --exact` | oversized request body is rejected with HTTP 413 | 1 test passed | PASS |
| Concurrency rejection | `cargo test -p vtapi storage_router_rejects_requests_when_concurrency_limit_is_reached -- --exact` | overloaded router rejects immediately instead of queueing forever | 1 test passed | PASS |
| Background rebalance | `cargo test -p vtapi background_rebalance_task_polls_select_admin_endpoint -- --exact` | select background task periodically hits rebalance endpoint | 1 test passed | PASS |
| Bench harness | `cargo run -p vtbench -- storage-ingest --rows=1000 --batch-size=100` | benchmark tool runs and prints throughput summary | `ops_per_sec=87038.830` | PASS |
| Bench query mode | `cargo run -p vtbench -- storage-query --traces=100 --spans-per-trace=3 --queries=500` | query benchmark mode runs and prints throughput summary | `ops_per_sec=5616.247` | PASS |
| Bench HTTP mode | `cargo run -p vtbench -- http-ingest --requests=20 --spans-per-request=2 --concurrency=4` | HTTP benchmark mode runs end-to-end | `ops_per_sec=1399.858` | PASS |
| Weighted placement | `cargo test -p vtapi cluster::tests:: --lib` | weighted rendezvous stays stable and limits key movement | 9 tests passed | PASS |
| Standard OTLP path | `cargo test -p vtapi otlp_http_json_ingest_works_on_standard_v1_traces_path -- --exact` | `/v1/traces` ingests JSON OTLP payloads | 1 test passed | PASS |
| OTLP protobuf path | `cargo test -p vtapi otlp_http_protobuf_ingest_works_on_standard_v1_traces_path -- --exact` | `/v1/traces` ingests protobuf OTLP payloads | 1 test passed | PASS |
| HTTPS serve path | `cargo test -p vtapi tls_server_accepts_https_requests -- --exact` | runtime serve path accepts HTTPS traffic | 1 test passed | PASS |
| mTLS serve path | `cargo test -p vtapi mtls_server_requires_client_identity -- --exact` | mTLS rejects unauthenticated clients and accepts valid identities | 1 test passed | PASS |
| Cluster client TLS reload | `cargo test -p vtapi cluster_client_reloads_rotated_mtls_identity_without_restart -- --exact` | select recovers remote reads after cluster client cert/key rotation without restart | 1 test passed | PASS |
| Control-state anti-entropy | `cargo test -p vtapi cluster_control_state_absorbs_fresher_peer_snapshot -- --exact` | local select adopts fresher peer leader/epoch snapshot | 1 test passed | PASS |
| Control journal replication | `cargo test -p vtapi cluster_leader_replication_exposes_control_journal -- --exact` | local leader journals and replicates leader election state to peer control nodes | 1 test passed | PASS |
| Logs HTTP ingest/search | `cargo test -p vtapi ingest_then_search_logs_round_trip -- --exact` | `/v1/logs` ingests JSON OTLP logs and `/api/v1/logs/search` returns structured results | 1 test passed | PASS |
| Logs gRPC ingest/search | `cargo test -p vtapi ingest_logs_over_grpc_then_search -- --exact` | OTLP logs gRPC unary ingest stores rows that logs search can read back | 1 test passed | PASS |
| Typed dynamic field columns | `cargo test -p vtstorage disk_engine_persists_typed_dynamic_field_columns_after_reopen -- --exact` | homogeneous `i64`/`bool` fields reopen from typed part columns without losing values | 1 test passed | PASS |
| Cluster members control plane | `cargo test -p vtapi cluster_members_endpoint_reports_node_health_and_topology -- --exact` | members endpoint probes storage health and exposes topology/weight metadata | 1 test passed | PASS |
| Public auth | `cargo test -p vtapi public_routes_require_bearer_token_when_configured -- --exact` | public API rejects missing bearer token | 1 test passed | PASS |
| Internal auth | `cargo test -p vtapi storage_internal_routes_require_internal_bearer_token_when_configured -- --exact` | internal RPC rejects missing bearer token | 1 test passed | PASS |
| Admin auth | `cargo test -p vtapi select_admin_routes_require_admin_bearer_token_when_configured -- --exact` | admin API rejects missing admin token | 1 test passed | PASS |
| Parallel write fan-out | `cargo test -p vtapi cluster_insert_fanout_runs_remote_writes_in_parallel -- --exact` | insert fan-out completes in parallel | 1 test passed | PASS |
| Fast replica read | `cargo test -p vtapi cluster_select_returns_from_fast_replica_without_waiting_for_slow_one -- --exact` | select returns from fast replica without waiting for slow one | 1 test passed | PASS |
| Duration bench ingest | `cargo run -p vtbench -- storage-ingest --rows=1000 --batch-size=100 --duration-secs=1` | throughput run prints latency percentiles | `ops_per_sec=77674.646`, `latency_p99_ms=0.954` | PASS |
| Duration bench query | `cargo run -p vtbench -- storage-query --traces=100 --spans-per-trace=3 --queries=500 --duration-secs=1` | query run prints latency percentiles | `ops_per_sec=4870.861`, `latency_p99_ms=0.630` | PASS |
| Duration bench HTTP | `cargo run -p vtbench -- http-ingest --requests=20 --spans-per-request=2 --concurrency=4 --duration-secs=1` | HTTP run prints latency percentiles and actual completed request count | `ops_per_sec=9363.466`, `latency_p99_ms=0.756` | PASS |
| Fault-injected bench HTTP | `cargo run -p vtbench -- http-ingest --requests=40 --spans-per-request=2 --concurrency=4 --duration-secs=2 --warmup-secs=1 --sample-interval-secs=1 --fault-after-secs=1 --fault-duration-secs=1 --report-file=./var/bench-http-fault.json` | benchmark emits p999, error count, and time-series report under an injected outage window | `ops_per_sec=16621.942`, `errors=12024`, `latency_p999_ms=1.516` | PASS |
| Reused append-time live updates | `cargo test -p vtstorage segment_accumulator_returns_live_updates_without_rescanning_prepared_rows -- --exact --nocapture` | append-time accumulator emits the same live updates as the old rescan helper | 1 test passed | PASS |
| Storage suite after hot-path reuse | `cargo test -p vtstorage -- --nocapture` | storage tests still pass after removing the append-time live-update rescan | all `vtstorage` tests passed | PASS |
| Release build after hot-path reuse | `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench` | optimized binaries build cleanly after hot-path change | build passed | PASS |
| OTLP protobuf load benchmark (fresh single run) | `vtbench otlp-protobuf-load --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024` against official/memory/disk | compare latest same-host ingest throughput and p99 after removing the append-time live-update rescan | official `309768.282 spans/s p99=1.132ms`, memory `323530.539 spans/s p99=0.842ms`, disk `340055.678 spans/s p99=0.793ms` | PASS |
| OTLP protobuf load benchmark (5-round stability median) | same command, serial official -> memory -> disk for 5 rounds | capture stability after the hot-path change | official `321503.677 spans/s p99=1.061ms`, memory `153990.072 spans/s p99=1.819ms`, disk `346439.448 spans/s p99=0.748ms` | PASS |
| Same-host A/B review (`2fd0eca` vs `816e523`) | fresh single-run official, baseline memory/disk, current memory/disk on the same host and same benchmark generator | decide whether the retained trace microbatch branch is a real win or a regression | official `359069.163 spans/s p99=0.832ms`, baseline disk `397705.956 spans/s p99=0.570ms`, current disk `342820.041 spans/s p99=0.742ms` | PASS |
| Same-host disk isolation (`2fd0eca` vs `f326503` vs `816e523`) | fresh single-run disk-only comparison on the same host and same benchmark generator | isolate whether the regression comes from append-time live-update reuse or from the retained outer batching path | baseline `420416.742 spans/s p99=0.407ms`, mid `412299.565 spans/s p99=0.419ms`, current `366477.541 spans/s p99=0.637ms` | PASS |
| Current retained batch metrics review | fresh single-run `/metrics` scrape after current disk and memory ingest runs | determine whether the retained outer batching path is materially changing physical append units | disk avg input blocks per flush `278944 / 167892 = 1.66`, disk output blocks `278944`, memory input/output blocks `185952 -> 104532` | PASS |
| Passthrough bypass recovery (`f326503` vs current head) | same-host disk-only fresh single-run after bypassing outer trace batching for passthrough engines | confirm whether recovery line returns to the pre-microbatch direct-write band | `f326503 359159.514 spans/s p99=0.672ms`, current head `355886.613 spans/s p99=0.603ms` | PASS |
| Direct disk kernel slope (`disk-trace-block-append`) | `vtbench disk-trace-block-append --duration-secs=5 --warmup-secs=1 --spans-per-block=5 --trace-shards=1 --payload-variants=1024` with `blocks-per-append=1/4/8/16` | measure whether current disk append kernel actually benefits from larger same-shard append packets | `1 -> 1.29M`, `4 -> 1.65M`, `8 -> 1.60M`, `16 -> 1.74M spans/s` | PASS |
| OTLP protobuf load benchmark after recovery + shard-batch kernel | `vtbench otlp-protobuf-load --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024` against official/memory/disk | check whether the recovery plus current kernel work already lifts end-to-end ingest above official | official `333655.337 spans/s p99=0.863ms`, memory `250914.462 spans/s p99=1.045ms`, disk `302830.051 spans/s p99=1.006ms` | PASS |
| Cheap same-shard combiner correctness | `cargo test -p vtstorage disk_engine_combines_concurrent_same_shard_block_appends -- --exact --nocapture` | verify the disk-local combiner actually merges concurrent same-shard appends before wider rollout | 1 test passed after wiring the combiner path | PASS |
| Post-combiner storage verification | `cargo test -p vtstorage -- --nocapture`, `cargo test -p vtbench -- --nocapture`, `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench` | ensure the new disk combiner, direct append harness, and release binaries all stay green together | all touched crates/tests passed; release build passed | PASS |
| OTLP protobuf load benchmark after cheap same-shard combiner (fresh single run) | fresh clean start, same `vtbench otlp-protobuf-load` shape against official/memory/disk | measure whether exposing the shard-batch kernel through the disk-local combiner recovers the end-to-end lead over official | official `398170.042 spans/s p99=0.666ms`, memory `344016.675 spans/s p99=0.633ms`, disk `417535.483 spans/s p99=0.422ms` | PASS |
| OTLP protobuf load benchmark after cheap same-shard combiner (fresh 5-round median) | fresh clean start, 5 serial rounds with the same official/memory/disk URLs | verify the recovered lead is stable rather than a one-shot warm run artifact | official `342053.911 spans/s p99=0.870ms`, memory `166741.431 spans/s p99=1.713ms`, disk `354507.035 spans/s p99=0.728ms` | PASS |
| Post-run disk combiner metrics | `/metrics` scrape after the fresh 5-round disk run | quantify whether the cheap combiner formed real multi-block appends and whether read-side drain is still a debt | input/output blocks `1414336 -> 1409140`, max queue depth `6`, wait max `18217us`, metrics scrape still took about `9.7s` | PASS |
| Disk trace-shard sweep (`4/8/16`) | fresh disk-only single runs with `VT_STORAGE_TRACE_SHARDS=4/8/16` and the same OTLP protobuf load shape | test whether current combiner gains are being diluted by too many trace shards | `4 -> 336939.068 spans/s`, `8 -> 378300.053`, `16 -> 388392.046`; fewer shards regressed | PASS |
| Outer passthrough batching re-check | re-enable batching envelope for passthrough `append_trace_blocks(...)` only, then rerun fresh single official/memory/disk OTLP protobuf load | test whether the next gain should come from reusing the outer batching worker instead of staying disk-local | official `398139.658 spans/s p99=0.683ms`, memory `355753.478 spans/s p99=0.609ms`, disk `375492.296 spans/s p99=0.534ms`; disk regressed badly, so this route was reverted | PASS |
| Raw same-shard queue pickup verification | `cargo fmt --all`, `cargo test -p vtstorage -- --nocapture`, `cargo test -p vtbench -- --nocapture`, `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench` after moving the combiner queue back to raw `TraceBlock`s and draining one more pending wave after prepare | verify the cheaper pre-prepare same-shard handoff stays correct and buildable before trusting benchmark wins | formatting passed, touched tests passed, release build passed | PASS |
| OTLP protobuf load benchmark after raw same-shard queue pickup (fresh single run) | fresh clean start, same `vtbench otlp-protobuf-load` shape against official/memory/disk | measure whether queuing raw blocks before prepare actually widens the end-to-end lead over official | official `391557.761 spans/s p99=0.691ms`, memory `355908.426 spans/s p99=0.615ms`, disk `430473.274 spans/s p99=0.423ms` | PASS |
| OTLP protobuf load benchmark after raw same-shard queue pickup (fresh 5-round median) | fresh clean start, 5 serial rounds with the same official/memory/disk URLs | verify the widened lead survives a full stability run instead of one lucky burst | official `390899.877 spans/s p99=0.674ms`, memory `172859.201 spans/s p99=1.365ms`, disk `423781.852 spans/s p99=0.431ms` | PASS |
| Post-run disk combiner metrics after raw same-shard queue pickup | `/metrics` scrape after the fresh 5-round disk run | quantify whether the cheaper handoff materially improved physical append formation and whether read-side drain remains the next debt | input/output blocks `1681824 -> 1648770` across the 5-round disk run, max queue depth `6`, wait max `16749us`, metrics scrape took about `30.7s` | PASS |

## Error Log
| Timestamp | Error | Attempt | Resolution |
|-----------|-------|---------|------------|
| 2026-04-04 15:31 | `git status` failed because parent is not a git repo | 1 | Continue without git assumptions |
| 2026-04-04 15:40 | `vtingest` failed to compile because `thiserror` was missing | 1 | Added workspace dependency to crate manifest |
| 2026-04-04 15:46 | `vtapi` failed because `AppState` unnecessarily derived `Clone` | 1 | Removed the derive and relied on `Arc<AppState>` |
| 2026-04-04 15:54 | invalid ingest path panicked inside handler | 1 | Added test and mapped flatten errors to HTTP 400 |
| 2026-04-04 21:18 | `reqwest 0.12` transitively required newer Rust than 1.73 | 1 | Switched to `reqwest 0.11` and pinned compatible lockfile versions for `url`, `idna`, and `indexmap` |
| 2026-04-04 22:35 | single-file disk engine became the main performance and recovery bottleneck | 1 | Replaced it with segment files plus sidecar metadata and thin in-memory indexes |
| 2026-04-04 22:58 | sealed part files were only renamed WAL payloads without a distinct format boundary | 1 | Added a part magic header and explicit WAL -> part sealing path |
| 2026-04-04 20:31 | Jaeger query compatibility tests failed with 404 and empty responses | 1 | Added Jaeger routes plus trace/operation envelope builders on top of existing query paths |
| 2026-04-04 21:03 | sealed parts accumulated without any merge path, keeping segment fan-out high after restart | 1 | Added conservative sealed-part compaction and rebuilt in-memory indexes after merge |
| 2026-04-04 21:05 | select kept probing the same failed replica on every read | 1 | Added failure backoff, temporary quarantine, and skip-aware cluster metrics |
| 2026-04-04 21:32 | new rebalance path failed to compile because of `owners.contains(*node)` type mismatch and handler future shape | 1 | consumed ownership by value in `rebalance_cluster`, fixed node comparison, and re-ran targeted plus full-suite verification |
| 2026-04-04 22:21 | `vtbench storage-query` failed because generated trace ids did not match the preloaded data shape | 1 | split row generation into trace-aware helpers and re-ran the benchmark mode |
| 2026-04-04 22:28 | full workspace test hung intermittently on the concurrency-limit regression | 1 | moved the wait side to `spawn_blocking`, added a timeout, and de-flaked the background rebalance assertion |
| 2026-04-04 23:29 | the concurrency-limit regression still flaked in the full workspace suite because the HTTP-level overlap was timing-sensitive | 2 | rewrote the test to use router-level concurrent `oneshot` requests and re-verified the full workspace |
| 2026-04-05 00:12 | `rcgen` transitively pulled a `time-core` release that required newer Cargo edition support than 1.73 could parse | 1 | pinned `time` to a Rust 1.73-compatible release and re-ran fresh verification |

## 5-Question Reboot Check
| Question | Answer |
|----------|--------|
| Where am I? | Phase 15 completed; the rewrite now has weighted replica placement, auth boundaries, OTLP trace/log ingest over HTTP + gRPC, a replicated control journal, and a shared-core logs query path |
| Where am I going? | The next step is proving materially higher throughput with stronger soak/capacity evidence and deciding whether metrics should become a first-class series engine here |
| What's the goal? | A production-oriented Rust VictoriaTraces rewrite that can genuinely evolve toward prod-grade scale |
| What have I learned? | Production hardening is not just durability and quorum; once the data plane is credible, control metadata and protocol surface become the next real constraints, and they should be pushed without forking a second storage kernel if the product boundary still fits shared storage semantics |
| What have I done? | Implemented and tested WAL/part storage, checksum-aware WAL recovery, fsync policy, selective part decode, operation-aware pruning, quorum-aware cluster ingest and reads, read repair, rebalance, weighted rendezvous placement, auth boundaries, OTLP trace/log ingest over HTTP + gRPC, HTTPS/mTLS serve paths, an admin-led but replicated control journal surface, fail-fast overload control, Jaeger/Tempo query compatibility, small-part compaction, node quarantine, and a latency-aware benchmark harness |
