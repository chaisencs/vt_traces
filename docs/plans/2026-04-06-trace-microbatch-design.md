# Trace Microbatch Design

## Goal

Deliver a higher-success "big leap" for `vtbench otlp-protobuf-load` by changing the physical ingest unit from `one request -> one tiny TraceBlock` to `one shard -> one microbatch`, while preserving synchronous request semantics and without reviving the failed disk-internal async shard worker path.

## Why This Path

- The benchmark shape is dominated by tiny trace requests: `vtbench otlp-protobuf-load` builds one trace per request with `spans-per-request` spans under that trace id.
- Both memory and disk already share the same outer batching wrapper, but the current wrapper only batches logs; traces still pass straight through.
- Memory's 5-round median collapse is consistent with retaining too many tiny `Arc<TraceBlock>` values and synchronously updating indexes for each request-sized block.
- The expensive `drain_pending_trace_updates()` debt is real, but it sits on read/stats paths and is not the first-order limiter for the pure ingest benchmark.

This makes the batching layer the highest-success intervention point: it is shared by memory and disk, it changes the request shape before engine-specific hot paths, and it avoids the coordination/ack regression of the rejected disk-only async design.

## Chosen Architecture

### Step 0: Instrumentation

Expose the following storage metrics through `StorageStatsSnapshot` and `/metrics`:

- `vt_storage_retained_trace_blocks`
- `vt_storage_trace_batch_queue_depth`
- `vt_storage_trace_batch_max_queue_depth`
- `vt_storage_trace_batch_flushes_total`
- `vt_storage_trace_batch_rows_total`
- `vt_storage_trace_batch_input_blocks_total`
- `vt_storage_trace_batch_output_blocks_total`
- `vt_storage_trace_batch_wait_micros_total`
- `vt_storage_trace_batch_wait_micros_max`
- `vt_storage_trace_batch_flush_due_to_rows_total`
- `vt_storage_trace_batch_flush_due_to_blocks_total`
- `vt_storage_trace_batch_flush_due_to_wait_total`

These are the decision metrics for the route. If they do not show real microbatch formation and block-count reduction, the route is not working regardless of code shape.

### Step 1: Per-Shard Trace Batch Worker

Add trace batching to `BatchingStorageEngine` using one worker queue per ingest shard:

- request thread: decode -> build `TraceBlock` -> partition/group by shard -> enqueue -> wait for shard acks
- worker thread: collect shard-local requests up to `max_batch_rows`, `max_trace_batch_blocks`, or `max_batch_wait`
- worker thread: flush through `inner.append_trace_blocks(...)`

Important boundaries:

- do not add a new async worker system inside `DiskStorageEngine`
- do not change ingest correctness or post-ack visibility semantics
- do not depend on repeated trace ids arriving together inside the same batch

### Step 2: Worker-Side Coalescing

If Step 1 proves that batches form, the worker should coalesce the shard-local batch into fewer output `TraceBlock` values before calling the inner engine. This is the actual block-count reduction lever:

- disk benefits from fewer prepare/append calls and fewer tiny block appends
- memory benefits from retaining fewer `Arc<TraceBlock>` instances across long runs

This coalescing happens in the batching layer, not in a disk-only async path.

## Benchmark Gates

### Gate A: Structural Viability

Run the same-host same-shape `vtbench otlp-protobuf-load` benchmark and inspect `/metrics` after each run.

Continue only if all are true:

- `trace_batch_flushes_total > 0`
- `trace_batch_input_blocks_total > trace_batch_output_blocks_total`
- average input blocks per flush is at least `2`
- average rows per flush is at least `10`
- `p99` does not regress by more than `15%` from the pre-change checkpoint

If these fail, stop. The workload is not forming useful batches and this route should not absorb more time.

### Gate B: Block-Count Reduction

After enabling worker-side coalescing, continue only if:

- memory `retained_trace_blocks` drops materially for the same ingest volume
- disk throughput improves or holds while `trace_batch_output_blocks_total` stays well below `trace_batch_input_blocks_total`

### Gate C: Finish Criteria

Declare the route successful only after re-running the same benchmark against:

- official VictoriaTraces
- `VT_STORAGE_MODE=memory`
- `VT_STORAGE_MODE=disk`

using the unchanged `vtbench otlp-protobuf-load` command line.

## Explicitly Rejected

- Reintroducing the failed per-request disk async shard-worker + live-merge path
- Chasing read-side `drain_pending_trace_updates()` first for this ingest-only scoreboard
- Spending the round on isolated Rust micro-optimizations that do not change the ingest unit
