# Task Plan: Rust VictoriaTraces Rewrite

## Goal
Build a production-oriented Rust rewrite of the VictoriaTraces core inside `rust_victoria_trace`, starting with a single-node ingest/storage/query vertical slice and an architecture that can expand to cluster mode later.

## Current Phase
Phase 13

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
