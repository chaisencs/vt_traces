# Rust VictoriaTraces Architecture

## Current Scope

This workspace now implements two credible slices of a Rust rewrite of VictoriaTraces:

- Single-node slice
  - OTLP/HTTP JSON + protobuf ingest model
  - span flattening into structured fields
  - per-trace start/end time index
  - in-memory row store
  - disk-backed active WAL plus sealed part store with sidecar metadata recovery
  - query service that resolves trace window before scanning rows
  - HTTP API for ingest, trace lookup, service listing, and trace search
- Cluster-foundation slice
  - `insert` role for ingest and replica fan-out
  - `storage` role for local persistence and internal read/write RPC
  - `select` role for trace-aware reads, cross-node service listing, and merged search
  - replica-aware trace lookup fallback
  - configurable write quorum
  - configurable read quorum
  - read repair and admin rebalance
  - topology-aware weighted rendezvous placement
  - admin cluster members endpoint with active health probing
  - optional background membership refresh polling
  - optional background rebalance polling
  - public/internal/admin bearer-token boundaries
  - standard OTLP/HTTP JSON + protobuf ingest on `/v1/traces`
  - optional HTTPS / mTLS server mode
  - optional HTTPS / mTLS cluster client
  - fail-fast router concurrency limits and body-size bounds
  - role-specific Prometheus-style metrics

This is still intentionally narrower than upstream VictoriaTraces. It now proves both the local core and the first cluster split without pretending to have parity with upstream columnar parts, merge workers, or Jaeger/Tempo compatibility layers yet.

## Workspace Layout

- `crates/vtcore`
  - shared data model
  - `Field`
  - `TraceSpanRow`
  - `TraceWindow`
- `crates/vtingest`
  - OTLP-like request types
  - OTLP/HTTP protobuf codec
  - flattening logic from request -> `TraceSpanRow`
- `crates/vtstorage`
  - storage engine trait
  - `MemoryStorageEngine`
  - `DiskStorageEngine`
  - row buckets by `trace_id`
  - trace window index
- `crates/vtquery`
  - trace lookup service
  - enforces query path: trace window first, row scan second
- `crates/vtapi`
  - `axum` HTTP surface
  - single-node router
  - storage router
  - insert router
  - select router
  - `/healthz`
  - `/metrics`
  - `/api/v1/traces/ingest`
  - `/v1/traces`
  - `/api/v1/traces/:trace_id`
  - `/api/v1/services`
  - `/api/v1/traces/search`
  - `/internal/v1/rows`
  - `/internal/v1/traces/:trace_id`
  - `/internal/v1/services`
  - `/internal/v1/traces/search`
  - `/admin/v1/cluster/members`
  - HTTPS / mTLS serving helper

## Why This Shape

The upstream VictoriaTraces design works because it separates concerns cleanly:

1. Ingest translates OTLP spans into flat structured records.
2. Storage keeps rows and a thin trace-specific index.
3. Query resolves a trace time window before loading rows.

This Rust rewrite preserves those same boundaries. The current implementation now has both memory and disk local engines, and a cluster boundary that keeps trace-aware routing above the storage engine trait. The disk engine now uses an active WAL that seals into immutable part files with a thin in-memory index and grouped per-segment reads, which is materially closer to the upstream storage shape than the earlier monolithic append file.
The durability semantics are now stronger too: WAL appends can run under an explicit sync policy, WAL records carry checksums, and restart recovery truncates corrupted tails instead of failing the whole engine.

## Immediate Next Phases

### Phase 2: Columnar Parts

- replace row-oriented segment payloads with columnar segment/part payloads
- write block metadata separately from column values
- persist a thin trace index beside parts
- background flush and recovery remain local concerns

### Phase 3: Compaction & Query Acceleration

- group rows by stream and time
- add typed encodings for integer/string/const columns
- introduce bloom filters and time pruning
- merge smaller parts into larger parts
- add backpressure and ingestion batching controls

### Phase 4: Query/API Compatibility

- Jaeger-compatible lookup endpoints
- Tempo-compatible search surface where needed
- OTLP gRPC and richer OTLP/HTTP payload handling
- richer search filters and pagination

### Phase 5: Throughput Push

- typed encodings beyond the current dictionary/row model
- broader per-field pruning instead of only time/service/operation shortcuts
- lower-overhead ingest encoding than JSON-shaped row serialization
- more realistic load generation and profiling around the 200k-QPS target

## Current Tradeoffs

- Good:
  - clear crate boundaries
  - tested vertical slice
  - realistic core trace index behavior
  - easy to replace storage backend later
  - first cluster role split is already exercised in tests
  - local disk engine is now WAL + sealed part instead of a single append file
  - insert role already supports degraded-success writes via quorum
  - insert fan-out now runs concurrently across replicas
  - trace reads can now return from fast replicas without waiting for slower copies
  - trace reads batch row loads per segment instead of repeatedly opening files per row
  - WAL records now carry checksums and recover by truncating invalid tails
  - overload is now bounded by explicit request body and concurrency controls
  - weighted rendezvous placement can spread replicas across failure domains and honor per-node capacity weights
  - external, internal, and admin surfaces can now be protected independently with bearer tokens
  - a runnable benchmark harness exists for storage and HTTP ingest/query smoke runs, including duration-driven runs with latency percentiles
- Not done yet:
  - current disk persistence is still row-oriented, not a columnar part format
  - compaction exists, but there is still no VictoriaLogs-grade merge pipeline
  - no stream index
  - no general per-field bloom/index layer
  - membership control now has leader election, epoch views, peer anti-entropy, and a replicated journal, but it is still not a consensus-backed metadata plane
  - OTLP trace/log ingest now exists over HTTP JSON/protobuf and unary gRPC, but metrics parity is still missing
