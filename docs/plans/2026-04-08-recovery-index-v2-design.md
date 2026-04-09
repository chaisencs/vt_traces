# Persisted Recovery Index v2 Design

> This document records the v2 replacement for the earlier `binary meta + monolithic snapshot` design in [2026-04-08-persisted-recovery-index-design.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-persisted-recovery-index-design.md).

## Why v1 Was Still Not Good Enough

The first replacement design fixed the worst restart pathology, but it still left two structural inefficiencies:

1. Recovery snapshot state was written as one large `trace-index.snapshot` blob.
2. `segment-*.meta.bin` still repeated raw strings and row metadata with limited deduplication.

That design was better than JSON, but it still looked like an internal stopgap rather than a mature storage checkpoint format. In particular:

- a single corrupted snapshot file could invalidate the whole checkpoint
- shard-level recovery state was not independently addressable
- snapshot rewrite cost scaled as one monolith
- binary segment metadata still paid repeated storage for service names, operation names, field names, and field values

## v2 Goals

v2 keeps strict readiness semantics unchanged and upgrades only the persistence format:

1. replace the monolithic snapshot with `manifest + per-shard snapshot files`
2. rewrite `meta.bin` as dictionary-coded, compressed binary metadata
3. treat corrupted recovery snapshot files as soft failures and fall back to segment recovery

`readyz=200` still means the full persisted query-correct state is ready. No degraded-query mode is introduced.

## v2 Layout

### Recovery Checkpoint

Root-level recovery files become:

- `trace-index.manifest`
- `trace-index-shard-0000.bin`
- `trace-index-shard-0001.bin`
- `...`

The manifest stores:

- `next_segment_id`
- `persisted_bytes`
- `typed_field_columns`
- `string_field_columns`
- sealed covered `segment_id` set
- one entry per shard with:
  - `shard_index`
  - compressed payload size
  - uncompressed payload size
  - payload checksum

Each shard file stores only one shard’s serialized `DiskTraceShardState`, compressed independently.

### Segment Metadata

Each sealed segment still writes `segment-*.meta.bin`, but v2 changes its payload:

- top-level repeated strings are removed or derived when reading
- strings are dictionary-coded per segment
- per-trace string references use dictionary ids instead of raw UTF-8 repeats
- per-row segment id is omitted because the segment is already known from the enclosing file
- the encoded payload is compressed before writing

Legacy `.meta.json` remains readable as a rebuild fallback.

## Recovery Behavior

Startup now does:

1. scan `segments/`
2. load `trace-index.manifest`
3. validate covered sealed segment ids
4. load each `trace-index-shard-*.bin`
5. if any shard file is missing, corrupted, mismatched, or has invalid checksum:
   - ignore the whole checkpoint
   - recover from persisted segments instead
6. if valid:
   - reuse covered shard state directly
   - materialize only uncovered tail segments

This makes the checkpoint format more like a proper manifest-based storage checkpoint rather than a single opaque cache file.

## Failure Model

v2 intentionally treats snapshot corruption as non-fatal:

- corrupted manifest: ignore checkpoint, rebuild from segments
- corrupted shard file: ignore checkpoint, rebuild from segments
- missing covered segment: ignore checkpoint, rebuild from segments

The recovery checkpoint is an acceleration structure, not an availability dependency.

## Expected Benefits

Compared with v1:

- smaller segment metadata files
- less CPU spent decoding segment metadata
- less write amplification from a single monolithic snapshot file
- better fault isolation and easier inspection of persisted shard checkpoints
- safer production restart behavior because corrupted snapshot files no longer hard-fail startup

## Tradeoffs

v2 still does not aim for the final possible format.

Remaining future improvements:

- delta/varint encoding for row refs
- optional mmap-friendly shard snapshot layout
- periodic background checkpointing instead of only startup-clean-shutdown points
- segment-level manifest compaction or incremental checkpoint delta files

Those are follow-up optimizations. v2 is the point where the design becomes credible for production review.
