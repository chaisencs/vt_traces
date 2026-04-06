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
- The higher-success "big leap" path is to add trace microbatching at the shared `BatchingStorageEngine` layer, not to retry a disk-internal async shard-worker design; this route directly attacks the tiny-request ingest shape shared by both memory and disk.
- The first gating metrics for that route are retained trace blocks, trace batch queue depth, trace batch flush counts, input-vs-output block counts, and batch wait totals/max.
- The batching-layer trace combiner is only worthwhile at `max_batch_wait=0` on this machine; tiny positive waits (`50us`, `100us`) improved batch ratios but regressed real throughput.
- Disk benefits from engine-specific passthrough batching: allowing disk to receive multi-block `append_trace_blocks(...)` batches without rebuilding one synthetic `TraceBlock` lifted fresh disk ingest into the low-`360k spans/s` range on the current host.
- The lighter disk prepare path also benefits from dropping the retained source `TraceBlock` after `PreparedTraceBlockAppend::new`; keeping only prepared row metadata plus encoded row payloads preserved semantics while trimming work.
- A deeper disk prepared-row-batch fusion attempt regressed hard enough to discard; the remaining gap to official is now inside the disk append kernel itself rather than in the shared batching envelope.
- The cheap disk-local same-shard combiner works as intended: the request thread that wins the combiner lock can drain concurrent same-shard work and feed the shard-batch append kernel without reintroducing the failed async worker design.
- Fresh clean-start single run after wiring the combiner: official `398170.042 spans/s` at `p99=0.666ms`, memory `344016.675 spans/s` at `p99=0.633ms`, disk `417535.483 spans/s` at `p99=0.422ms`.
- Fresh clean-start 5-round serial median after wiring the combiner: official `342053.911 spans/s` at `p99=0.870ms`, memory `166741.431 spans/s` at `p99=1.713ms`, disk `354507.035 spans/s` at `p99=0.728ms`.
- Post-run combiner metrics show only light but real coalescing on the HTTP path: input blocks `1414336`, output blocks `1409140`, and max queue depth `6`. That means the recovered lead is not coming from huge cross-request batches yet; it is coming from the cheaper handoff plus the existing shard-batch append kernel.
- The first `/metrics` scrape after the 5-round disk run still took about `9.7s`, which is more evidence that read-side `drain_pending_trace_updates()` remains the next visible debt even though pure ingest throughput is back above official.
- A disk-only `VT_STORAGE_TRACE_SHARDS` sweep (`4`, `8`, `16`) landed at about `336939`, `378300`, and `388392 spans/s` respectively, so reducing shard count is not the next win on this machine; the default high-shard setup is still best among the tested points.

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
| Treat batching-layer per-shard trace microbatches as a bounded experiment, not a committed architecture | It was worth testing because it matched the benchmark shape, but the same-host A/B now shows it should not remain the primary disk direction |
| Keep engine-specific trace batch payload modes | Memory needs coalesced blocks to reduce retained heap block count; disk performs better when it receives passthrough multi-block appends |
| Reject positive microbatch waits on the current mainline | Measured `50us` / `100us` waits increased coalescing ratios but reduced actual ingest throughput |
| Reject the outer trace microbatch layer as the main disk leap vehicle | Same-host A/B against `2fd0eca` and `f326503` shows the retained `816e523` path regresses disk because it adds batching overhead without changing the disk engine's physical append unit |
| Put the next "big leap" on a disk-internal prepared shard batch writer, not another envelope rewrite | The current disk engine still appends block by block even when it receives multiple blocks, so the only high-value next step is to amortize WAL and live-update work inside the append kernel itself |
| Recovering the direct disk path plus adding a shard-batch append kernel restores the `f326503` direct-write band, but not the end-to-end HTTP ingest lead over official | Same-host disk-only A/B put current head back near `f326503`, yet fresh HTTP ingest still landed around `302830 spans/s`, so the remaining gap is exposing bigger same-shard batches cheaply on the write path, not more inner append cleanup alone |
| The disk kernel now shows meaningful direct multi-block scaling | The new `vtbench disk-trace-block-append` harness improved from about `1.29M spans/s` at `1 block/append` to about `1.74M spans/s` at `16 blocks/append`, which is enough evidence that larger same-shard append packets are worth feeding into disk |
| Keep the cheap same-shard combiner on the disk mainline | It recovers disk above official on both fresh single-run and fresh 5-round median without reintroducing the async worker / positive-wait regressions |
| Do not spend the next round lowering `trace_shards` | A fresh disk-only sweep shows `16` shards outperform `8` and `4`, so lower shard counts would reduce throughput on this host rather than unlock bigger combiner wins |

## Issues Encountered
| Issue | Resolution |
|-------|------------|
| `rust_victoria_trace` is empty | Treat it as a greenfield Rust workspace |
| Parent directory is not a git repository | Skip git actions and focus on filesystem changes/tests |
| `cargo init` created nested repos | Remove child `.git` directories and use workspace root only |
| `reqwest 0.12` pulled in transitive crates requiring newer Rust than 1.73 | Downgrade to `reqwest 0.11` and pin `url` / `idna` / `indexmap` in `Cargo.lock` via `cargo update --precise` |
| The async shard-worker experiment regressed disk ingest badly compared with checkpoint `2fd0eca` | Restored the `2fd0eca` mainline first, then resumed optimization from the known-good synchronous path |
| Earlier performance reasoning over-weighted `observe_live_updates` even though the benchmark is pure ingest | Rechecked the write path and targeted append-time live-update construction instead of the later read-side drain |
| A deeper disk prepared-row-batch fusion experiment regressed far below the current best branch | Reverted the experiment and kept the simpler disk passthrough / lighter prepare path on the mainline |

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
