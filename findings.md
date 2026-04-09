# Findings & Decisions

## Comparison Dimensions
- Ingest hot path
- Query and index model
- Persistence and recovery semantics
- Cluster and HA behavior

## Stable Inputs
- Upstream design source: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/victoriatraces-source-analysis-report.md`
- Current Rust design baseline: `docs/architecture.md`
- Recent recovery design baselines:
  - `docs/plans/2026-04-08-persisted-recovery-index-design.md`
  - `docs/plans/2026-04-08-recovery-index-v2-design.md`

## Early Findings
- Upstream VictoriaTraces is not a trace-native storage engine. It is a trace data model layered onto VictoriaLogs storage plus an additional trace-window acceleration stream.
- Upstream performance comes from a coordinated design set: trace-local routing, asynchronous internal aggregation, prefix-seek index layout, compressed sorted blocks, and background merge hierarchy.
- Current Rust implementation already preserves some strong upstream-aligned ideas:
  - trace-aware routing
  - trace-window-first lookup
  - immutable sealed parts
- Current Rust implementation also carries stronger semantics than upstream in some areas:
  - explicit readiness model
  - recovery acceleration artifacts
  - richer cluster-side quorum and repair behavior
- Current Rust implementation is still structurally behind upstream on the storage/index side:
  - row-oriented steady-state persistence
  - no prefix-seek tag index
  - no mergeset-like compressed sorted block hierarchy
  - no background merge pipeline at upstream maturity

## Open Hypotheses
- The biggest performance gap is likely structural, not a single hot function: current ingest path is doing more synchronous trace-native bookkeeping than upstream.
- Recovery v2 should be preserved, but the data-plane write path probably needs to move closer to an upstream-style async flush/merge model.
- The best next architecture is not "copy upstream exactly"; it is "adopt upstream's low-overhead data path while preserving the Rust rewrite's stronger readiness and cluster semantics."

## Comparative Conclusions
- Preserve:
  - strict `livez` / `readyz`
  - recovery v2
  - trace-aware routing
  - stronger cluster quorum/repair/auth semantics
- Adopt from upstream:
  - cheaper steady-state write path
  - stronger separation between ingest acceptance and layout optimization
  - persistent immutable query acceleration structures
  - compressed sorted block-oriented storage/index layout
- Stop doing:
  - growing synchronous head search/list work on the ack path
  - treating the current row-oriented path as the end-state storage design
  - forcing recovery improvements to slow down steady-state ingest

## Canonical Outputs
- Design realignment doc:
  - `docs/plans/2026-04-08-victoriatraces-design-realignment.md`
- Capability package for future thread transfer:
  - `docs/handoffs/2026-04-08-rust-vt-design-capability-package.md`

## Implementation Findings
- `recovery manifest` had a real semantic gap: it only persisted covered `segment_id`s and silently rewrote all covered paths back to `.part` on reload.
- `persist_recovery_snapshot()` had the same blind spot at clean shutdown: it discarded the runtime `segment_paths` truth and reconstructed every covered path as `segment-*.part`.
- This gap would block the planned rotate-only write path, because a wal-backed persisted segment could never survive a clean snapshot/restart cycle as wal-backed state.
- The first code implementation wave therefore landed the recovery prerequisite before touching steady-state ingest: manifest/snapshot now preserve `wal|part` kind for covered segments.
- The actual throughput regression was not caused by the recovery-v2 file formats themselves. The decisive fix was restoring a cheaper steady-state write path: rolled trace segments now persist as wal-backed immutable segments instead of immediately snapshotting into the seal pipeline.
- After rotate-only landed, the same native arm64 harness that previously showed Rust behind official flipped to Rust ahead of official: 5-round median `612,601 spans/s` vs `601,426 spans/s`, with Rust p99 at `0.847 ms` vs official `1.267 ms`.
- Append was still doing wasted work after the rotate-only win: it constructed `LiveTraceUpdate` overlays even though the hot path no longer consumed them. That construction has now been removed from `append_block_rows`, and the remaining live-update/seal scaffolding is now a cleanup target rather than an ingest prerequisite.
- The cleanup target was real, not theoretical: `crates/vtstorage/src/disk.rs` still carried dead live-update tests, idle seal runtime threads, and stale helper code even after the write path had moved to head federation plus rotate-only persistence.
- That stale scaffolding has now been removed from the active data path. `vtstorage` is back to a clean no-warning build, and `vtapi` full tests pass again on top of the cleaned storage layer.
- VictoriaTraces upstream still lacks a trace WAL, which means its default tradeoff is closer to “throughput first, accept active-head crash loss”. The Rust rewrite should not hard-code that tradeoff globally; it now needs an explicit runtime switch.
- The correct product shape is “WAL on by default, WAL off only by deliberate opt-in”. That preserves the stronger durability contract for `stable`, while still allowing a higher-throughput no-WAL mode when the operator explicitly accepts active-head crash loss.
- The new `trace_wal_enabled` implementation keeps a WAL-disabled head fully query-visible in memory and writes rotated segments as persisted `.rows` row files, so the no-WAL mode is not a fake flag; it really removes trace `.wal` files from the steady-state data path.
- Recovery semantics remain coherent in no-WAL mode because rotate-only persistence plus `wal|part|rows` recovery manifest support allow covered rows-backed segments to round-trip cleanly after restart.
- The first no-WAL implementation exposed the real problem clearly: direct-to-part materialization on roll was far too heavy for a throughput mode.
- Replacing that path with persisted batched-binary `.rows` row files fixed the intended tradeoff:
  - old no-WAL probe on the direct-to-part path: `252,397 spans/s`
  - new no-WAL probe on the `.rows` path: `595,778 spans/s`, `p99 0.752 ms`
  - current WAL-on probe remains stronger on absolute throughput: `692,153 spans/s`, `p99 0.677 ms`
- Recovery semantics now stay coherent for the new no-WAL mode because recovery manifest covered-segment kind has been extended again, from `wal|part` to `wal|part|rows`.
- Latest benchmark evidence says the current tree is back to leading official on native arm64. Latest 3-round median is:
  - official: `607,682 spans/s`, `p99 1.253 ms`
  - Rust disk: `638,582 spans/s`, `p99 0.768 ms`
  - throughput ratio: `1.0508`
  - p99 ratio: `0.6133`
  - output dir: `/tmp/rust-vt-official-bench-rows-walfix-20260409`
- This changes the next hot-path decision:
  - do not turn WAL off by default; WAL-on still wins on absolute throughput and durability
  - keep `trace_wal_enabled=false` as an explicit operator tradeoff
  - move the next implementation wave to sealed-side query/index efficiency, because ingest has returned to a good competitive position
- The first sealed-side query/index implementation wave has now landed:
  - persisted segment summaries are no longer just coarse prune metadata; they now also persist per-trace summaries inside each sealed segment
  - reopened sealed trace search no longer needs to rebuild segment metadata just to answer service / operation / generic-field filtered trace search
  - the exact reopened path is now `candidate segments from persisted summaries -> exact trace-summary match -> merge hits`
- The key runtime simplification is that sealed persisted segments no longer repopulate the old heavy in-memory persisted inverted indexes on observe/recovery:
  - kept: `trace_ids`, `windows_by_trace`, `row_refs_by_trace`, `rows_ingested`, compact persisted summaries
  - removed from the steady-state persisted rebuild path: `services_by_trace`, `trace_refs_by_service`, `trace_refs_by_operation`, `trace_refs_by_field_name_value`, `field_values_by_name`
- Recovery shard snapshot format has now been compacted again:
  - version bumped to v3
  - v3 keeps backward read compatibility for older shard snapshots
  - v3 no longer serializes empty legacy persisted inverted-index sections for newly written snapshots
- Query-side correctness and recovery semantics both held after the compact summary cut:
  - full `vtstorage` test suite passes
  - full `vtapi` test suite passes
  - snapshot-backed reopen still serves search after metadata deletion without recreating `segment-*.meta.{json,bin}`
- Latest native arm64 same-shape benchmark after the Phase 6 cut still keeps Rust ahead of official:
  - official median: `605,762.724 spans/s`, `p99 1.252 ms`
  - Rust disk median: `623,163.967 spans/s`, `p99 0.774 ms`
  - throughput ratio: `1.0287`
  - p99 ratio: `0.6177`
  - output dir: `/tmp/rust-vt-official-bench-phase6-20260409`
- The missing Phase 6 evidence is now closed with a real query-heavy public-path benchmark instead of the old memory-only `storage-query` micro-benchmark.
- The new `official-query-compare` harness benchmarks the persisted-data query path, not hot mutable state:
  - ingest OTLP protobuf fixture
  - `official` force-flushes
  - both targets restart on the same data directory
  - benchmark runs against `GET /select/jaeger/api/traces`
- The first harness attempt exposed two real benchmarking pitfalls that are now fixed:
  - official rejects synthetic 1970-era timestamps with `request time out of retention`
  - stale benchmark processes can poison later rounds via fixed ports
- After moving fixture timestamps to “current time minus 5 minutes” and rerunning on clean ports, the 3-round native arm64 query-heavy result is strongly in Rust’s favor:
  - official median: `28,022.114 qps`, `p99 2.162 ms`
  - Rust disk median: `167,437.718 qps`, `p99 0.275 ms`
  - throughput ratio: `5.9752`
  - p99 ratio: `0.1271`
  - output dir: `/tmp/rust-vt-official-query-bench-20260409`
- This matters because the measured path is the public Jaeger trace-search endpoint, so the win is not just an internal `search_traces()` micro-benchmark; it includes:
  - reopened persisted-data serving
  - sealed-side prune
  - trace materialization
  - HTTP JSON response generation
