# Task Plan: VictoriaTraces Design Gap Closure

## Goal
对照 `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/victoriatraces-source-analysis-report.md`，系统比较 upstream VictoriaTraces 与当前 Rust 实现的写入、查询、持久化与恢复设计，明确哪些优点要保留、哪些缺点要补齐，并形成可执行的改造路线。

## Current Phase
Phase 6

## Phases

### Phase 1: Canonical Reading
- [x] Read the VictoriaTraces source analysis report for core architectural choices
- [x] Read current Rust architecture and recent recovery/perf design docs
- [x] Capture stable comparison dimensions in findings.md
- **Status:** complete

### Phase 2: Design Comparison
- [x] Compare ingest hot path
- [x] Compare query/index model
- [x] Compare persistence/recovery semantics
- [x] Compare cluster/HA tradeoffs
- **Status:** complete

### Phase 3: Direction Setting
- [x] Decide what to preserve from current Rust design
- [x] Decide what to adopt from VictoriaTraces design
- [x] Reject low-value or incompatible directions explicitly
- **Status:** complete

### Phase 4: Delivery Assets
- [x] Write a comparison and next-step design doc under docs/plans
- [x] Summarize the concrete next implementation bets
- [x] Note whether a thread capability package is needed next
- **Status:** complete

### Phase 5: Recovery-First Implementation
- [x] Preserve covered segment kind in recovery manifest/snapshot (`wal|part`)
- [x] Make clean-shutdown recovery snapshot preserve actual persisted segment paths
- [x] Add regression coverage for wal-backed covered segments
- [x] Move steady-state trace roll from immediate seal to rotate-only persistence
- [x] Re-benchmark Rust disk vs official after rotate-only lands
- [x] Add a storage config switch to disable trace WAL explicitly
- [x] Make rotate-only persistence recover correctly when WAL is disabled and segments are part-backed from birth
- [x] Plumb `VT_STORAGE_TRACE_WAL_ENABLED` through `vtapi` runtime config and deployment examples
- [x] Remove dead live-update tests/APIs plus the idle seal runtime from the steady-state data path
- [x] Re-run full `vtstorage` / `vtapi` suites after the cleanup batch
- [x] Re-benchmark the current arm64 working tree against official after the cleanup batch
- [x] Probe the current WAL on/off throughput tradeoff directly on the throughput profile
- [x] Use the new benchmark evidence to choose the next hot-path cut
- **Status:** in progress

### Phase 6: Sealed Query-Prune Index
- [x] Land a sealed persistent filter index for service / operation / generic field pruning
- [x] Route sealed trace search through prune-first execution instead of broad row scans
- [x] Re-benchmark query-heavy same-shape workloads after sealed prune index lands
- [x] Verify the new sealed index does not regress ingest hot path or recovery semantics
- **Status:** complete

## Key Questions
1. Which VictoriaTraces design choices materially explain its performance profile?
2. Which of those choices are compatible with the Rust rewrite's current goals and strengths?
3. Which current Rust features are worth preserving even if upstream does not have them?
4. What should the next implementation wave target first to improve throughput without discarding recovery v2?

## Decisions Made
| Decision | Rationale |
|----------|-----------|
| Stay on the current thread for now | The immediate need is comparison and design convergence, not thread transfer |
| Use VictoriaTraces report as canonical upstream design summary | Prevents re-deriving the upstream architecture from scratch |
| Evaluate along four dimensions: ingest, query/index, persistence/recovery, cluster/HA | Keeps comparison concrete and implementation-oriented |

## Errors Encountered
| Error | Attempt | Resolution |
|-------|---------|------------|

## Notes
- This task is comparative and architectural first; implementation sequencing comes after design convergence.
- If a new thread is opened later, it should inherit method, benchmark, and output conventions via a capability package rather than a chat-summary handoff.
- Main delivery artifacts:
  - `docs/plans/2026-04-08-victoriatraces-design-realignment.md`
  - `docs/handoffs/2026-04-08-rust-vt-design-capability-package.md`
- Current implementation focus has moved from design-only to code: recovery manifest now carries covered segment kind so rotate-only persisted WAL segments can become a first-class recovery state.
- Current benchmark status now needs to be read in time order, not cherry-picked:
  - there was a temporary regression window where the cleaned current tree lagged official on native arm64
  - after replacing the no-WAL direct-to-part rotate path with a cheap `.rows` row-file path, the latest native arm64 3-round median is back to Rust disk `638,582 spans/s` vs official `607,682 spans/s`, throughput ratio `1.0508`, p99 ratio `0.6133`
  - current output dir: `/tmp/rust-vt-official-bench-rows-walfix-20260409`
- New durability/performance runtime switch:
  - `VT_STORAGE_TRACE_WAL_ENABLED` defaults to `true`
  - setting it to `false` disables trace active-head WAL, keeps head visibility in memory, and materializes rolled segments as persisted `.rows` row files
  - this is intentionally an opt-in tradeoff: higher write efficiency in exchange for losing active-head rows on process crash before roll
- Latest direct probe says the no-WAL path is now a real mode rather than a pathological slow path:
  - previous single native arm64 run with WAL off on the direct-to-part path: `252,397 spans/s`
  - latest single native arm64 run with WAL on: `692,153 spans/s`, `p99 0.677 ms`
  - latest single native arm64 run with WAL off on the new `.rows` path: `595,778 spans/s`, `p99 0.752 ms`
  - conclusion: `trace_wal_enabled=false` is now a viable throughput-oriented option, but WAL-on still wins on absolute throughput and durability
- Phase 6 has now landed a first real sealed-side prune path:
  - recovery shard snapshot v3 stores compact persisted segment summaries plus per-trace summaries, instead of depending on heavy persisted inverted indexes
  - reopened disk search now uses `persisted_segments -> candidate segment pruning -> exact trace-summary match`, so searches no longer need to rebuild segment metadata just to answer sealed queries
  - old shard snapshot versions remain readable through versioned recovery decoding
- Latest native arm64 3-round same-shape ingest compare after the Phase 6 cut:
  - official median: `605,762.724 spans/s`, `p99 1.252 ms`
  - Rust disk median: `623,163.967 spans/s`, `p99 0.774 ms`
  - throughput ratio: `1.0287`
  - p99 ratio: `0.6177`
  - output dir: `/tmp/rust-vt-official-bench-phase6-20260409`
- Phase 6 query-heavy same-shape benchmark is now also complete on the public Jaeger trace search path after persisted-data restart:
  - official median: `28,022.114 qps`, `p99 2.162 ms`
  - Rust disk median: `167,437.718 qps`, `p99 0.275 ms`
  - throughput ratio: `5.9752`
  - p99 ratio: `0.1271`
  - output dir: `/tmp/rust-vt-official-query-bench-20260409`
  - harness doc: `docs/plans/2026-04-09-official-query-benchmark-harness.md`
