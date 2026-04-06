# 2026-04-06 Disk vs Official Recovery Plan

## Goal

Recover a disk ingest mainline that is again clearly above official VictoriaTraces on the same host and same benchmark shape, then pursue one structural leap with a materially higher success rate than the failed async-worker and outer-batching experiments.

## Hard Gate

A candidate mainline is only acceptable if all of the following hold under the same benchmark shape (`vtbench otlp-protobuf-load --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`):

1. Disk fresh single-run throughput is above official by a visible margin, not just noise.
2. Disk 5-round stability median is also above official.
3. Disk p99 does not regress materially while chasing throughput.
4. The result is reproduced from a clean server start, not a warmed process artifact.

Because official itself is drifting between runs on this machine, "visible margin" should be treated as at least high-single-digit percent, not a near-tie.

## Same-Host Review Evidence

### Apples-to-apples A/B (`2fd0eca` vs `816e523`)

Fresh single-run, same host, same benchmark generator:

| target | spans/s | p99 |
| --- | ---: | ---: |
| official | 359069.163 | 0.832 ms |
| baseline memory (`2fd0eca`) | 359650.033 | 0.697 ms |
| baseline disk (`2fd0eca`) | 397705.956 | 0.570 ms |
| current memory (`816e523`) | 278935.826 | 0.948 ms |
| current disk (`816e523`) | 342820.041 | 0.742 ms |

Derived result:

- Current disk is down about 13.8% vs `2fd0eca` on throughput.
- Current disk p99 is worse by about 30% vs `2fd0eca`.
- This is a real regression in the retained mainline, not just a reporting mismatch.

### Isolation run (`2fd0eca` vs `f326503` vs `816e523`)

Fresh single-run, same host, same benchmark generator:

| target | spans/s | p99 |
| --- | ---: | ---: |
| baseline disk (`2fd0eca`) | 420416.742 | 0.407 ms |
| mid disk (`f326503`) | 412299.565 | 0.419 ms |
| current disk (`816e523`) | 366477.541 | 0.637 ms |

Derived result:

- `f326503` only gives back roughly 1.9% vs `2fd0eca`, so append-time live-update reuse is not the main regression.
- The large regression lands between `f326503` and `816e523`.
- That isolates the primary problem to the outer trace microbatch layer and its current disk integration, not to the lighter prepared metadata path.

### Current retained disk batching metrics (`816e523`)

Same host, current disk mainline, fresh run:

| metric | value |
| --- | ---: |
| trace batch flushes | 167892 |
| trace batch input blocks | 278944 |
| trace batch output blocks | 278944 |
| trace batch max queue depth | 9 |
| trace batch wait max | 10836 us |
| rows ingested | 1394720 |

Derived result:

- Average input blocks per flush is only about `1.66`.
- Output blocks equal input blocks, so disk receives no physical block-count reduction from this layer.
- The current outer batching path adds queueing/control-plane overhead, but it does not change the disk engine's actual append unit.

### Current retained memory batching metrics (`816e523`)

Same host, current memory mainline, fresh run:

| metric | value |
| --- | ---: |
| trace batch flushes | 104532 |
| trace batch input blocks | 185952 |
| trace batch output blocks | 104532 |
| retained trace blocks | 104532 |
| max queue depth | 9 |

Derived result:

- Memory does get real coalescing: input blocks drop from `185952` to `104532`.
- Even so, memory throughput is still poor.
- That means memory's wall is not only "too many small blocks"; it is also synchronous in-memory index maintenance and heap retention shape.

## Review Conclusion

The retained `816e523` branch changed the envelope around ingest, but it did not materially change the disk engine's physical append kernel. The benchmark shape is many `5 spans/request` trace blocks. Current outer batching can occasionally hand disk more than one block at a time, but disk still prepares and appends each block independently inside `DiskStorageEngine::append_trace_blocks` and `append_prepared_trace_blocks`. That is why the branch pays added coordination cost without getting a real kernel-level amortization win.

So the next leap should not be:

- another generic outer batching experiment,
- another async shard worker,
- positive batch waits,
- or a resurrection of batch-local `SegmentAccumulator` map merge.

The next leap should be a disk-internal kernel redesign that actually changes the append unit.

## Proposed Two-Lane Plan

### Lane A: Recover the fast disk mainline first

Objective: get back to the `f326503` / `2fd0eca` performance band before attempting a bigger leap.

Actions:

1. Remove or bypass the outer trace microbatch path for disk ingest.
2. Keep the append-time live-update reuse and lighter prepared metadata path unless a new same-host A/B disproves them.
3. Re-run same-host fresh official vs disk and 5-round medians.

Expected outcome:

- Recover most of the current regression quickly.
- Re-establish a trustworthy baseline for the next experiment.

### Lane B: Build the real leap inside the disk append kernel

Objective: reduce the fixed per-block cost that still dominates the `5 spans/request` benchmark shape.

Core design:

1. Keep request-thread decode and shard partitioning.
2. Replace "many prepared blocks appended one by one" with a shard-local prepared append packet.
3. Let the disk engine append many small source blocks as one physical WAL batch when they already arrive together.
4. Generate live updates during the same append pass.
5. Do not introduce a batch-local `trace_id -> update` map merge.

### Why this design is the right target

Current disk code already accepts `Vec<TraceBlock>`, but it still does this:

1. prepare each block independently,
2. loop `prepared_block` by `prepared_block`,
3. write each block as its own append unit,
4. merge live updates block by block.

That means current cross-request batching never reaches the true hot cost. The real leap is to change the disk engine so multiple blocks can share:

- one WAL batch header/checksum path,
- one segment-size decision loop,
- one row-location construction pass,
- and one live-update observation pass over a larger contiguous prepared row slice.

## Concrete Design Sketch

### Step 1: `PreparedTraceShardBatch`

Add a shard-local batch object in disk storage that represents many prepared source blocks for a single shard.

Suggested contents:

- `prepared_rows: Vec<PreparedBlockRowMetadata>`
- `encoded_row_bytes: Vec<u8>`
- `encoded_row_ranges: Vec<Range<usize>>`
- `block_boundaries: Vec<Range<usize>>`
- block-local shared-group references or a packet-level remap

Important constraint:

- Do not globally merge by trace id.
- If the same trace appears in multiple discontinuous source blocks, it is acceptable for v1 to emit multiple `LiveTraceUpdate`s and let the shard state merge them later.

### Step 2: `append_prepared_trace_shard_batch`

Replace the current block-by-block loop with a batch-aware append path that:

1. computes segment cut points over the full shard batch,
2. writes one physical WAL batch per segment slice,
3. allocates row locations for the whole slice,
4. updates the `SegmentAccumulator` over the full slice.

This is the first place where the design can win big, because it removes repeated header/checksum/length-prefix/write-bookkeeping across many tiny blocks.

### Step 3: Prove kernel gain before adding handoff

Before any new combiner or queueing, first make the kernel batch path measurable with direct multi-block input:

1. call disk append with 1, 4, 8, 16 same-shard blocks,
2. verify a real cost-per-row reduction,
3. verify trace visibility and query correctness,
4. only then consider how to feed it larger batches on the HTTP path.

This keeps the critical experiment narrow and falsifiable.

### Step 4: Only if Step 3 wins, add a cheap combiner

If the kernel batch path proves out, then add a very cheap shard-local combiner at the disk boundary:

- no dedicated worker thread,
- no per-request async ack pipeline,
- no positive wait budget by default,
- request thread that wins the combiner lock drains ready work and flushes.

This is deliberately narrower than the failed async-worker route. The handoff only exists to expose the new kernel append path to more than one request at a time; it is not the feature itself.

## Benchmark Gates For Each Step

### Gate R: recovery gate

- Disk must return to roughly the `f326503` / `2fd0eca` band on the same host.
- If recovery does not happen, stop and re-check assumptions before touching the leap design.

### Gate K: kernel batch gate

- Direct multi-block disk append must beat the recovered baseline under targeted input.
- If the kernel batch writer is not clearly faster when fed 4 to 16 small same-shard blocks, stop. Do not add any new combiner.

### Gate H: HTTP-path gate

- After exposing the kernel batch path to real HTTP ingest, disk fresh single and 5-round median must both beat official with margin.
- If the gain disappears on the HTTP path, the control-plane cost is still too high and the handoff must be simplified or dropped.

### Current Execution Outcome

The disk-local same-shard combiner clears Gate H on this machine, but one follow-on write-path idea has already been rejected and one read-path debt has now been paid down:

| run | official | disk |
| --- | ---: | ---: |
| fresh single | `396475.630 spans/s`, `p99 0.673ms` | `430192.512 spans/s`, `p99 0.409ms` |
| fresh 5-round median | `343086.506 spans/s`, `p99 0.902ms` | `359315.329 spans/s`, `p99 0.713ms` |

Two follow-on conclusions are now locked in:

1. Reusing the outer batching envelope for passthrough trace appends is not the next win; a fresh re-test dropped disk to about `375492 spans/s`, so that route stays rejected.
2. Extra pre-prepare raw-block fusion is also not the next win; a fresh re-test dropped disk to about `420970 spans/s` fresh single and `407471` fresh 5-round median, so that route stays rejected too.
3. Bounded `stats()`-side drain is worth keeping; it cut the first post-run `/metrics` scrape from about `30.7s` to about `14ms` without giving back the ingest lead over official.

The margin is still real but not a large blowout. Post-run metrics still show only light cross-request coalescing on the HTTP path (`1442528` input blocks vs `1416916` output blocks on the latest clean 5-round run), so the remaining work is not "recover the disk lead" anymore; it is "turn the current lead into a visibly larger one on the write path without reintroducing queue overhead, while query/read paths beyond `stats()` still need separate live-update drain cleanup."

## Explicitly Rejected Routes

- Re-adding the failed async shard-worker path.
- Continuing to tune positive outer batch waits.
- Treating outer batching metrics as success if disk append units do not actually get larger.
- Re-introducing the previously rejected `SegmentAccumulator` batch-local map merge design.
- Spending the next round on scattered Rust micro-optimizations before changing the kernel append unit.

## Immediate Execution Order

1. Recover disk to the `f326503`-class direct path and re-benchmark.
2. Add a targeted disk-only benchmark or test harness for multi-block same-shard append.
3. Implement `PreparedTraceShardBatch` and the batch append kernel.
4. Re-benchmark direct multi-block append first.
5. Only if that wins, expose it to real HTTP ingest with the cheapest possible combiner.

## Code Areas That Matter Next

- `crates/vtstorage/src/disk.rs`
  - `DiskStorageEngine::append_trace_blocks`
  - `DiskStorageEngine::append_prepared_trace_blocks`
  - `ActiveSegment::append_block_rows`
  - `SegmentAccumulator::observe_prepared_block_rows`
- `crates/vtstorage/src/batching.rs`
  - likely to be bypassed for disk recovery, not extended further as the main leap vehicle
