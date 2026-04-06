# Task Plan: Rust VictoriaTraces Rewrite

## Goal
Build a production-oriented Rust rewrite of the VictoriaTraces core inside `rust_victoria_trace`, starting with a single-node ingest/storage/query vertical slice and an architecture that can expand to cluster mode later.

## Current Phase
Phase 17

## Phases

### Phase 1: Requirements & Discovery
- [x] Understand user intent
- [x] Identify constraints and requirements
- [x] Document findings in findings.md
- **Status:** complete

### Phase 2: Planning & Structure
- [x] Define technical approach
- [x] Create Rust workspace structure
- [x] Document decisions with rationale
- **Status:** complete

### Phase 3: Implementation
- [x] Create workspace and core crates
- [x] Implement OTLP ingest model and flattening pipeline
- [x] Implement in-memory trace index and row store
- [x] Implement trace lookup API path
- [x] Test incrementally
- **Status:** complete

### Phase 4: Testing & Verification
- [x] Run Rust unit/integration tests
- [x] Verify ingest -> query vertical slice
- [x] Log results in progress.md
- **Status:** complete

### Phase 5: Cluster Foundation
- [x] Split `insert` / `select` / `storage` API roles
- [x] Add trace-aware replica routing
- [x] Add replica fallback on reads
- [x] Add cluster metrics and tests
- **Status:** complete

### Phase 6: Delivery
- [x] Summarize implemented scope and gaps to full VictoriaTraces parity
- [x] Leave next-phase roadmap
- [ ] Deliver current slice to user
- **Status:** in_progress

### Phase 7: Storage Core Hardening
- [x] Replace single append file with segment-based disk storage
- [x] Add sidecar metadata recovery for segments
- [x] Add write quorum semantics for cluster ingest
- [x] Move row-oriented segments to columnar parts
- **Status:** complete

### Phase 8: Segment Compaction & Node Governance
- [x] Add automatic small-part compaction for sealed segments
- [x] Add cluster node failure backoff and quarantine
- [x] Expose quarantine behavior through metrics and tests
- **Status:** complete

### Phase 9: Selective Decode, Pruning & Repair Loops
- [x] Add part-level selective decode for sealed `.part` trace reads
- [x] Add time/service/operation pruning with per-trace operation bloom hints
- [x] Add configurable read quorum for trace reads
- [x] Add read repair after successful quorum reads
- [x] Add admin-triggered rebalance for missing desired replicas
- [x] Verify targeted and full-suite regressions
- **Status:** complete

### Phase 10: Production P0 Hardening
- [x] Add durability hardening: record checksums, fsync policy, and crash-tail recovery
- [x] Add overload controls: API concurrency limits, request body bounds, and test coverage
- [x] Add stronger cluster governance: automatic repair loop and topology-aware placement
- [x] Add benchmark/load-test harness for ingest/query hot paths
- [x] Re-verify touched crates and workspace with fresh commands
- **Status:** complete

### Phase 11: Replica Governance, Auth & Performance Evidence
- [x] Upgrade placement to weighted rendezvous with optional node weights
- [x] Add concurrent replica fan-out and fast-replica trace reads
- [x] Add public/internal/admin bearer-token boundaries
- [x] Add standard OTLP/HTTP JSON ingest on `/v1/traces`
- [x] Extend `vtbench` with duration mode and latency percentile output
- [x] Re-verify touched crates and workspace with fresh commands
- **Status:** complete

### Phase 12: OTLP Protobuf, TLS & Membership Control Plane
- [x] Add OTLP/HTTP protobuf ingest on `/v1/traces`
- [x] Add optional HTTPS / mTLS server mode
- [x] Add HTTPS / mTLS cluster client wiring for `insert` and `select`
- [x] Add admin cluster members endpoint and background membership refresh loop
- [x] Re-verify touched crates and workspace with fresh commands
- **Status:** complete

### Phase 13: Distributed Governance, TLS Lifecycle & Columnar Refinement
- [x] Add select control-plane peer config and peer membership sync
- [x] Add deterministic leader election and leader-only governance loops
- [x] Add replicated control journal endpoints and peer journal sync
- [x] Add rotating/reloading cluster HTTP client certificates without process restart
- [x] Tighten columnar part internals with typed field encoding where it materially reduces read/write cost
- [x] Re-verify touched crates and workspace with fresh commands
- **Status:** complete

### Phase 14: Benchmark Evidence & Protocol Expansion
- [x] Extend `vtbench` with time-series reporting, `p999`, and injectable failure windows
- [ ] Add higher-pressure soak/capacity workflows and publish repeatable result baselines
- [x] Expand OTLP beyond traces with shared-core logs ingest/search support
- [ ] Decide whether metrics belongs in this repository as a first-class series engine or as a separate bounded product slice
- **Status:** in_progress

### Phase 17: Trace Microbatch Leap
- [x] Write a bounded design for the batching-layer microbatch route before touching code
- [x] Add Step 0 metrics for retained blocks and trace batch formation
- [x] Add Step 1 shard-local trace microbatch combiner on the shared batching layer
- [x] Re-run same-host `vtbench otlp-protobuf-load` gates before deciding whether to keep pushing the route
- **Status:** in_progress

## Key Questions
1. Which crash-consistency guarantees can be delivered now without overcomplicating the current part lifecycle?
2. Which overload controls protect the process fastest: per-role concurrency bounds, body-size limits, or both?
3. How far can admin-led membership refresh and repair go before needing a fully distributed control plane?

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Start with single-node vertical slice | Full parity with VictoriaTraces cluster is too large for one pass; a vertical slice proves architecture and behavior |
| Build a Cargo workspace with multiple crates | Keeps ingest, storage, query, and API boundaries explicit and easier to evolve |
| Model spans as flattened rows plus trace index | Preserves the same core design principle used by VictoriaTraces while staying implementable now |
| Implement tests before behavior code where practical | Needed to keep the rewrite honest and avoid speculative architecture |
| Use an OTLP-like JSON ingest surface for v0 | It preserved the conceptual boundary without pulling full protobuf and wire compatibility into the first slice |
| Add cluster split before columnar parts | It locks the product topology early so the storage rewrite can focus on local efficiency instead of moving network boundaries later |
| Add segment storage before columnar storage | It establishes the persistent part lifecycle and thin-index model before encoding optimization work |
| Add compaction before deeper pruning | It reduces fan-out and query amplification early while preserving the current storage API surface |
| Add in-process node quarantine before broader HA work | It removes repeated failed probes immediately without committing to a full membership protocol yet |
| Add operation-aware pruning before broader per-field indexes | It targets a common tracing filter with small metadata and avoids paying the cost of generic indexing too early |
| Add read repair before full automation | It restores replica health opportunistically on read paths while keeping the cluster loop simple and testable |
| Prioritize durability before deeper hot-path rewrites | A fast trace system that can lose acknowledged writes is not production-ready |
| Add bounded overload controls before claiming high-QPS readiness | Backpressure is required before any throughput number is trustworthy |
| Add OTLP protobuf on `/v1/traces` before broader protocol work | It closes the most visible compatibility gap without forcing OTLP gRPC into the same pass |
| Add TLS/mTLS and a select-side membership control surface before distributed consensus | It materially improves deployability and observability while keeping the control loop simple enough to verify |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|
| `git status` failed because repo root is not a git repository | 1 | Continue without git assumptions; work directly in filesystem |
| `cargo init` created nested `.git` directories for each crate | 1 | Removed generated nested repos and kept a single workspace layout |

## Notes
- This session targets a serious foundation, not fake completeness.
- Initial implementation favors correctness and architecture clarity over premature micro-optimization.
- Part payloads, selective decode, operation-aware pruning, and repair/rebalance control loops are now in place.
- The hardening pass through Phase 13 is now complete; the remaining risks are proof-level benchmark evidence and whether metrics should become a first-class series engine here.
- Phase 14 is now focused on turning `vtbench` into a stronger soak/fault evidence path and deciding how far OTLP metrics should be brought into a trace/log-first product boundary.
- The current disk ingest focus inside Phase 14 is hot-path reuse and tail-stability work on the synchronous mainline, not a return to the failed async shard-worker experiment.
- The trace-microbatch route was worth testing because it matched the request shape, but it is no longer the primary disk direction after the same-host review.
- The remaining high-value performance path is now inside the disk append kernel itself, not in another outer request-batching rewrite.
- Same-host review now shows the retained outer trace microbatch layer regresses disk versus both `2fd0eca` and `f326503`; the next execution plan is to recover the faster direct disk path first, then pursue a disk-internal prepared shard batch writer instead of extending the batching envelope.
- Recovery line is now in place: passthrough engines bypass outer trace batching again, and disk-only same-host A/B puts current head back near the `f326503` direct-write band.
- The new `vtbench disk-trace-block-append` harness now proves the disk kernel scales materially with larger same-shard append packets; the next unresolved task is exposing that gain to end-to-end HTTP ingest with a cheaper handoff than the rejected outer microbatch layer.
- Reusing the outer batching envelope for passthrough trace appends has now been re-tested and rejected; it regressed disk to about `375492 spans/s`, so the next gain must stay disk-local.
- The disk-local same-shard combiner is now in its stronger form: it queues raw `TraceBlock`s before prepare, picks up one more pending same-shard wave after the first prepare pass, and still passes its dedicated concurrency regression test.
- Fresh clean-start benchmarking now has disk clearly above official on both fresh single-run (`430473 spans/s` vs `391557`) and fresh 5-round median (`423781` vs `390899`) with better p99 in both cases.
- The lead is materially better than the earlier combiner checkpoint, but the combiner still only reduced disk output blocks from `1681824` input blocks to `1648770` output blocks across the 5-round run, so the next real bottleneck remains forming materially larger same-shard append packets cheaply and paying down read-side drain debt.
- A fresh disk-only `VT_STORAGE_TRACE_SHARDS=4/8/16` sweep shows `16` shards still wins on this host, so shard-count reduction is not the next high-value move.
