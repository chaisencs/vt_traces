# Findings & Decisions

## Requirements
- User wants a Rust rewrite of the VictoriaTraces component under `rust_victoria_trace`.
- User explicitly wants the work pushed hard on performance and quality.
- The target directory is currently empty, so the rewrite can establish its own project structure.
- A literal full reimplementation of VictoriaTraces + VictoriaLogs parity is too large for one pass; we need a staged rewrite with a real working core.
- This session must continue from `docs/handoffs/HANDOFF-2026-04-07-rust-vt-stable-throughput-query-gate.md`.
- This session must first read `README.md`, `docs/2026-04-06-otlp-ingest-performance-report.md`, `/tmp/vt_throughput_profile_autotuned_20260407_181147/median.json`, and `/tmp/vt_throughput_autotuned_probe_20260407_181552/metrics.txt`.
- This session may only do three things: lock the `stable` vs `throughput` release definition, run/evaluate the query gate, and produce production deployment guidance.
- This session must not continue ingest hot-path changes, must not change the benchmark shape, and must not merge the default and high-throughput releases into one semantic tier.

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
- The current handoff explicitly defines the candidate release split as:
  - `stable`: `VT_TRACE_INGEST_PROFILE=default` + `VT_STORAGE_SYNC_POLICY=data`
  - `throughput`: `VT_TRACE_INGEST_PROFILE=throughput` + `VT_STORAGE_SYNC_POLICY=none` + `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=268435456`
- The current strongest throughput benchmark artifact is `/tmp/vt_throughput_profile_autotuned_20260407_181147/median.json`, which reports `671273.875 spans/s` median with `p99=0.840916ms` and zero errors.
- The associated throughput probe at `/tmp/vt_throughput_autotuned_probe_20260407_181552/metrics.txt` shows `vt_storage_trace_seal_queue_depth=0`, `vt_storage_trace_head_segments=10`, `vt_storage_trace_head_rows=2555440`, and zero `fsync` operations because the run is on `sync_policy=none`.
- The current README already presents the project as a single-node `storage/select/insert` compatible trace backend and already documents a production-biased disk launch profile (`VT_STORAGE_MODE=disk`, `VT_STORAGE_PATH`, `VT_STORAGE_SYNC_POLICY=data`, `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608`, request/body and concurrency limits).
- The ingest performance report is intentionally public-facing and still describes the mainline published benchmark as the disk engine beating official under a production-oriented path, not the new throughput-only release semantics.
- The main unresolved production blocker is no longer ingest throughput; it is query performance plus near-realtime visibility for `trace-by-id`, `search`, `services`, `field-values`, and mixed read/write.
- Local query gate root: `/tmp/vt_query_gate_20260407_200521`
- `stable` local gate on this Apple Silicon host landed at roughly:
  - preload write `3430.94 spans/s`, write `p99=254.20ms`
  - static `trace-by-id` about `210.27 rps` with `p99=30ms`
  - static `search` about `28563.76 rps`
  - static `services` about `63489.40 rps`
  - static `field-values` about `73174.41 rps`
  - mixed write `3472.74 spans/s`, mixed `trace-by-id` about `244.82 rps` with `p99=13ms`
- `throughput` local gate on this Apple Silicon host landed at roughly:
  - preload write `418055.51 spans/s`, write `p99=1.18ms`
  - static `trace-by-id` about `6.26 rps` with `p99=905ms`
  - static `search` about `8662.82 rps` with a `4040ms` max outlier
  - static `services` about `67410.34 rps`
  - static `field-values` about `72645.28 rps`
  - mixed write `409894.84 spans/s`, mixed `trace-by-id` about `6.92 rps` with `69` failed requests
  - mixed `search` about `5716.42 rps` with a `5048ms` max outlier
- Near-realtime visibility is strong for both tiers on the same host:
  - `stable` ack-to-visible medians: `trace-by-id 0.50ms`, `search 0.52ms`, `services 0.48ms`, `field-values 0.48ms`
  - `throughput` ack-to-visible medians: `trace-by-id 0.30ms`, `search 0.31ms`, `services 0.30ms`, `field-values 0.31ms`
- The dominant query-plane risk on `throughput` is not ack-to-visible; it is backlog-era query / metrics instability:
  - after the preload burst, `/metrics` timed out once at `10s` while the seal backlog was draining
  - after the mixed read/write run, `/metrics` timed out again at `60s`
- Root-cause split from code inspection:
  - `list_services`, `list_field_names`, `list_field_values`, and `search_traces` still call `drain_pending_trace_updates()` directly on the request path in `crates/vtstorage/src/disk.rs`
  - `stats()` no longer drains unbounded work, but it still applies up to `STATS_TRACE_UPDATE_DRAIN_BATCH_LIMIT=8192` update batches on the scrape path, also by taking shard write locks and calling `observe_live_updates(...)`
  - `trace-by-id` does not drain pending live updates, but it still federates head reads by touching the active segment plus every sealing snapshot via `head_rows_for_trace(...)`; under `throughput + large segment + seal backlog`, this becomes the dominant read-path stall
- Recommended fix split:
  - Fix A: remove request-path live-update drain from `search/services/field-values/metrics`, and move it to an asynchronous background applier or a read overlay
  - Fix B: keep a dedicated fast head trace-by-id index / cache so `rows_for_trace` does not pay for large sealing-snapshot federation during backlog
- Low-value workaround to avoid: shrinking the throughput segment or reintroducing syncs just to hide the symptom would blur the release semantics and give back the ingest win
- The dominant production risk on `stable` is not the query plane; it is that `data` sync on this host keeps ingest in a durability-first band that is orders of magnitude below the throughput tier.
- Fresh query-plane decoupling artifacts now live under `/tmp/vt_query_gate_fix2_20260407_220316`.
- The decoupling pass fixed the timeout part of the problem:
  - `search/services/field-values` no longer block on request-path drain
  - `/metrics` no longer times out after preload or mixed load
  - post-preload `/metrics` now returns while reporting `trace_live_update_queue_depth=2677458`
  - post-mixed `/metrics` now returns while reporting `trace_live_update_queue_depth=3921112`
- The same pass made the remaining semantics explicit:
  - static `throughput` after the fix:
    - preload write `475276.74 spans/s`, `p99=0.978ms`
    - `trace-by-id` about `75.29 rps`, `p99=756ms`, `errors=0`
    - `search` about `28542.59 rps`, `p99=1ms`
    - `services` about `34742.44 rps`
    - `field-values` about `31334.15 rps`
  - mixed `throughput` after the fix:
    - write `335001.68 spans/s`, `p99=1.448ms`
    - `search` about `3260.32 rps`
    - `services` about `4590.41 rps`
    - `field-values` about `4561.93 rps`
    - `trace-by-id` still shows AB length-mismatch failures during concurrent writes because the probed trace keeps growing under the unchanged benchmark shape
- Near-realtime visibility under heavy `throughput` backlog is now split by read model instead of hidden inside timeouts:
  - `trace-by-id` stays near-realtime, with fresh visibility median about `0.48ms`
  - `search` stayed invisible for `15s` in 5/5 backlog probes
  - `services` and `field-values` stayed invisible for `5s` in 3/3 backlog probes, then appeared later once the background applier advanced
- The next product decision is now clean:
  - either ship `throughput` as an ingest-first / eventual-search SKU under backlog
  - or add a read overlay / materially faster live-update apply path before calling `throughput` production-ready for near-realtime search/tag use

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
| Reject reusing the outer batching envelope for passthrough `append_trace_blocks(...)` | Re-enabling batching only for passthrough trace appends still dropped disk to about `375492 spans/s`, so the extra control plane is expensive even before it forms meaningfully larger append packets |
| Put the next "big leap" on a disk-internal prepared shard batch writer, not another envelope rewrite | The current disk engine still appends block by block even when it receives multiple blocks, so the only high-value next step is to amortize WAL and live-update work inside the append kernel itself |
| Recovering the direct disk path plus adding a shard-batch append kernel restores the `f326503` direct-write band, but not the end-to-end HTTP ingest lead over official | Same-host disk-only A/B put current head back near `f326503`, yet fresh HTTP ingest still landed around `302830 spans/s`, so the remaining gap is exposing bigger same-shard batches cheaply on the write path, not more inner append cleanup alone |
| The disk kernel now shows meaningful direct multi-block scaling | The new `vtbench disk-trace-block-append` harness improved from about `1.29M spans/s` at `1 block/append` to about `1.74M spans/s` at `16 blocks/append`, which is enough evidence that larger same-shard append packets are worth feeding into disk |
| Reject pre-prepare raw `TraceBlock` coalescing on the mainline | Merging same-shard raw blocks into one bigger block before `PreparedTraceBlockAppend::new` lowered disk ingest to about `420970` fresh single and `407471` fresh 5-round median on the same host, so the extra row-copy/build cost outweighs the saved prepare calls here |
| Keep the disk-local combiner in its raw-queue / late-pickup form, but do not add extra pre-prepare block fusion on top | The late-pickup combiner still keeps disk above official, while the additional raw-block fusion attempt regressed throughput |
| Do not spend the next round lowering `trace_shards` | A fresh disk-only sweep shows `16` shards outperform `8` and `4`, so lower shard counts would reduce throughput on this host rather than unlock bigger combiner wins |
| The current disk mainline still stays above official after keeping only the bounded stats-side drain fix | Fresh single-run is now official `396475.630 spans/s` vs disk `430192.512`, and fresh 5-round median is official `343086.506` vs disk `359315.329`; machine drift is high, but disk still keeps a real lead with better p99 |
| Bounding `stats()`-side live-update drain pays down the largest visible read debt without hurting ingest | The first post-run `/metrics` scrape fell from about `30.7s` to about `14ms`, while fresh single and 5-round ingest both stayed above official |
| `vtbench` now has a built-in baseline comparator | Optimization rounds can now be turned into explicit pass/fail gates with saved baseline/candidate reports instead of ad-hoc manual comparison |
| Saved benchmark reports now carry stable comparison metadata | Schema version, git SHA, and benchmark-binary target arch / OS are now recorded so same-host comparisons can reject benchmark-shape drift and surface the exact revision being compared |
| Production guidance should map onto VictoriaTraces' `insert` / `select` / `storage` split as directly as possible | The user wants a smooth migration path, including familiar storage path and logging controls rather than a brand-new operational model |
| `stable` and `throughput` should stay as two separate release SKUs in docs and ops playbooks | Their write, durability, and post-burst query behavior diverge enough that one recommendation would be misleading |
| The first production recommendation should be `stable`, not `throughput` | `throughput` keeps near-realtime visibility excellent, but its post-burst query plane and metrics endpoint still show backlog-driven stalls severe enough to require canary/soak before wider rollout |

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
| Throughput tier `/metrics` timed out during seal backlog drain and again after mixed load | Record it as a production risk; do not paper over it by measuring only after the backlog fully settles |

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
