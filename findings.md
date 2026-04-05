# Findings & Decisions

## Requirements
- User wants a Rust rewrite of the VictoriaTraces component under `rust_victoria_trace`.
- User explicitly wants the work pushed hard on performance and quality.
- The target directory is currently empty, so the rewrite can establish its own project structure.
- A literal full reimplementation of VictoriaTraces + VictoriaLogs parity is too large for one pass; we need a staged rewrite with a real working core.

## Research Findings
- Official VictoriaTraces is built on top of VictoriaLogs rather than a standalone bespoke trace store.
- The critical behaviors to preserve are:
  - OTLP span ingestion
  - flattening spans into structured fields
  - `_time` based on span end time
  - per-trace start/end indexing for fast trace lookup
  - stream-oriented grouping and query narrowing
- Current local workspace is not inside a git repository, so git-based branch/commit workflows are unavailable here.
- Rust toolchain is available:
  - `rustc 1.73.0`
  - `cargo 1.73.0`
- The first vertical slice is now implemented with five crates and end-to-end HTTP round-trip tests.
- The project now also has a first cluster split:
  - `insert` role for ingest and replica fan-out
  - `storage` role for local persistence and internal RPC
  - `select` role for replica-aware reads and fan-out metadata queries
- The disk engine has now been moved from a single append file to segment-based persistence with sidecar metadata.
- The disk engine already persists sealed parts as a real columnar structure with dictionary columns for required/optional strings and native `i64` vectors for core timestamps; the remaining storage gap is mostly typed encoding for dynamic fields and richer execution/pruning, not absence of columnar layout.
- The disk engine now distinguishes active WAL files from sealed `.part` files with an explicit part header boundary.
- The disk engine now persists dynamic fields with mixed encodings: dictionary strings for general text plus typed `i64` / `bool` columns when the field domain allows it.
- Cluster ingest now supports explicit write quorum instead of all-replicas-or-fail semantics.
- Current storage durability is weaker than production requirements because WAL flushes do not call `sync_data` / `sync_all`.
- Current cluster placement is stable and replicated, but still uses simple modulo-style trace hashing rather than topology-aware placement.
- Current cluster search and services listing still fan out to all storage nodes, which is correct but not a proven 200k-QPS shape.
- There is no benchmark or load-test harness in the workspace yet, so performance claims have no hard evidence today.
- The workspace now has `vtbench`, but it is still a smoke/engineering benchmark harness rather than proof of the user's 200k-QPS target.
- The current control plane now has explicit control peers, peer health probing, deterministic leader election, peer state anti-entropy with an epoch-bearing control snapshot, and a replicated control journal, but it is still admin-led rather than a consensus-backed metadata plane.
- Server-side TLS and cluster HTTP clients now both support file-based hot reload, so rotated CA bundles and client identities can be picked up without process restart.
- OTLP logs now fit the current product boundary cleanly because they can reuse the shared structured-row storage kernel without forcing a separate persistence stack; metrics still would not fit that way without a true series engine.
- `vtbench otlp-protobuf-load` uses one trace per request with `spans-per-request` spans under that single trace id, so the single-trace prepared-row path is the dominant ingest hot path for the current benchmark shape.
- The pure ingest benchmark does not call `drain_pending_trace_updates()` on the write path; the relevant disk hot spot is append-time live-update construction, not the later read-side drain itself.
- Reusing `SegmentAccumulator::observe_prepared_block_rows` to emit `LiveTraceUpdate` values removed a second append-time scan over `prepared_rows` / `prepared_shared_groups` and restored the disk benchmark lead over official VictoriaTraces on the current machine.
- Fresh same-host single run after the change: official `309768.282 spans/s` at `p99=1.132ms`, memory `323530.539 spans/s` at `p99=0.842ms`, disk `340055.678 spans/s` at `p99=0.793ms`.
- Fresh same-host 5-round serial median after the change: official `321503.677 spans/s` at `p99=1.061ms`, memory `153990.072 spans/s` at `p99=1.819ms`, disk `346439.448 spans/s` at `p99=0.748ms`.
- One disk round in the new 5-round run still collapsed (`159796.984 spans/s`, `p99=3.192ms`), and a post-run `/metrics` read took seconds because read-side drain still has to absorb pending live updates; the remaining performance risk is now mostly tail instability and read-path merge debt rather than average ingest throughput.

## Technical Decisions
| Decision | Rationale |
|----------|-----------|
| Use a Cargo workspace rooted at `rust_victoria_trace` | Makes it possible to separate API, ingest, storage, query, and shared model crates |
| Start with in-memory storage and explicit traits | Lets us prove the end-to-end path now while keeping room for disk-backed engines later |
| Separate trace index from row store | Mirrors VictoriaTraces architecture and keeps lookup/query responsibilities clear |
| Keep OTLP flattening in a dedicated crate/module | This is a stable translation boundary regardless of storage backend |
| Return trace rows from HTTP as a map-shaped `fields` object | Makes API tests and future UI/compat adapters simpler than exposing a raw vector of name/value pairs |
| Add cluster roles before rewriting the local storage format | It prevents the future storage rewrite from reopening the network API and routing design |
| Add row-oriented segments before columnar parts | It locks in recovery, rotation, and thin index behavior before optimizing the physical encoding |
| Batch on-disk reads per segment before optimizing encodings | Avoids wasting I/O on per-row file open patterns that would distort later performance work |
| Treat durability and backpressure as P0 before claiming production readiness | They define whether acknowledged writes are trustworthy and whether overload is survivable |
| Reuse the append-time `SegmentAccumulator` scan to build live updates instead of rescanning prepared rows after append | It reduces duplicate work on the write hot path without changing ingest semantics or reintroducing the failed async worker experiment |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| `rust_victoria_trace` is empty | Treat it as a greenfield Rust workspace |
| Parent directory is not a git repository | Skip git actions and focus on filesystem changes/tests |
| `cargo init` created nested repos | Remove child `.git` directories and use workspace root only |
| `reqwest 0.12` pulled in transitive crates requiring newer Rust than 1.73 | Downgrade to `reqwest 0.11` and pin `url` / `idna` / `indexmap` in `Cargo.lock` via `cargo update --precise` |
| The async shard-worker experiment regressed disk ingest badly compared with checkpoint `2fd0eca` | Restored the `2fd0eca` mainline first, then resumed optimization from the known-good synchronous path |
| Earlier performance reasoning over-weighted `observe_live_updates` even though the benchmark is pure ingest | Rechecked the write path and targeted append-time live-update construction instead of the later read-side drain |

## Resources
- Local clone from prior research:
  - `/tmp/VictoriaTraces`
  - `/tmp/VictoriaLogs`
- Relevant upstream files:
  - `/tmp/VictoriaTraces/app/vtinsert/opentelemetry/opentelemetry.go`
  - `/tmp/VictoriaTraces/app/vtinsert/insertutil/index_helper.go`
  - `/tmp/VictoriaTraces/app/vtselect/traces/query/query.go`
  - `/tmp/VictoriaLogs/lib/logstorage/block.go`
  - `/tmp/VictoriaLogs/lib/logstorage/datadb.go`
  - `rust_victoria_trace/docs/architecture.md`

## Visual/Browser Findings
- Prior browser/source review established that VictoriaTraces performance comes from VictoriaLogs columnar block layout, per-day partitions, bloom filters, typed encodings, stream locality, and a thin trace-specific time-window index.
