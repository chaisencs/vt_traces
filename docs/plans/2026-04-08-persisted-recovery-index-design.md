# Persisted Recovery Index Design

> For later review, this document intentionally records both the old design and the replacement design.

## Context

On April 8, 2026, production restart behavior exposed two separate problems in the disk trace storage path:

1. Startup semantics were weak. The process could appear alive while the storage port was still not bound.
2. Recovery cost was dominated by rehydrating persisted trace indexes from large per-segment JSON metadata files.

The first problem has already been addressed by separating liveness from readiness. The second problem remains the dominant restart cost.

## Old Design

The previous persisted recovery path was:

1. Scan `segments/`
2. Detect `.wal`, `.part`, `.rows`, `.meta.json`
3. Seal unfinished row files into `.part`
4. Load each `segment-*.meta.json`
5. Deserialize `SegmentMeta`
6. Re-apply every `SegmentTraceMeta` into `DiskTraceShardState`

This design had three structural issues:

- Recovery cost scaled with the full persisted corpus on every startup.
- `SegmentMeta` was stored as large JSON, which amplified CPU, allocation, and string processing cost.
- There was no dedicated persisted representation of the query-ready shard index, so startup had to rebuild it from segment metadata every time.

## Replacement Design

The replacement design has three parts:

1. `binary segment meta`
2. `persisted recovery snapshot`
3. `strict ready semantics`

### 1. Binary Segment Meta

Each sealed segment writes a compact binary metadata file alongside the existing deterministic segment naming scheme.

Goals:

- eliminate JSON parse overhead for new segments
- preserve enough per-segment metadata to recover segments not covered by a snapshot
- keep segment-local rebuild possible if metadata is missing

Binary segment metadata remains a recovery input, not the primary fast path.

### 2. Persisted Recovery Snapshot

Storage will persist a binary recovery snapshot containing the query-ready persisted trace index state:

- `next_segment_id`
- `persisted_bytes`
- `typed_field_columns`
- `string_field_columns`
- sealed persisted segment id set
- serialized `DiskTraceShardState` per shard

Startup will:

1. scan on-disk segment files
2. try to load the recovery snapshot
3. validate that the snapshot matches the sealed on-disk segment base
4. skip full rebuild for all covered segments
5. recover only the uncovered tail:
   - newly sealed segments not present in the snapshot
   - unfinished WAL or legacy row files

This keeps readiness strict while making restart cost proportional to the delta since the last snapshot instead of the full retained corpus.

### 3. Strict Ready Semantics

This design does not weaken the meaning of readiness.

- `livez=200` means the process is alive and transport is bound.
- `readyz=200` means the full query-correct persisted storage state is available.
- No partial-query or degraded-query mode is introduced for readiness.

## Why This Design

Compared with only staging readiness:

- it keeps operational semantics clean
- it avoids query capability ambiguity during rollout

Compared with only replacing JSON by binary metadata:

- it removes the need to rebuild the entire persisted query index on every startup
- it gives restart cost a checkpoint-like upper bound

## Recovery Rules

The snapshot is valid only if all of the following are true:

- shard count matches current config
- every snapshot segment id exists on disk as a sealed `.part`
- no snapshot segment id is missing on disk

If validation fails, recovery falls back to full segment materialization.

If validation succeeds, recovery applies only the uncovered tail.

## Write Points

The new recovery snapshot is written after:

- successful startup recovery completes
- clean engine shutdown completes

This is sufficient to make controlled restart fast, which is the production problem exposed in this incident.

Future optimization can add periodic snapshot checkpoints during long-running service lifetime if crash-recovery tail cost becomes important.

## Review Checklist

When production is fully ready and stable, review should confirm:

- `livez` and `readyz` semantics still match the documented contract
- clean restart avoids full per-segment rehydrate
- stale snapshot recovery still produces correct query results
- old on-disk `.meta.json` data remains readable
- new sealed segments prefer binary metadata
- no query regression in:
  - trace window lookup
  - rows-for-trace
  - service listing
  - field value listing
  - trace search

## Non-Goals

- No attempt to preserve old metadata as the long-term primary format.
- No degraded readiness mode.
- No extra deployment knobs for recovery behavior in this iteration.
