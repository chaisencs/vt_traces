# 2026-04-08 Official Benchmark Harness

## Goal

Provide one reproducible `vtbench` entrypoint for the "same host, same load shape" OTLP ingest benchmark against official VictoriaTraces and the Rust `vtapi` targets.

## Scope

This harness is intentionally narrow:

- workload: `vtbench otlp-protobuf-load`
- target kinds: `official`, `disk`, `memory`
- benchmark shape defaults:
  - `duration-secs=5`
  - `warmup-secs=1`
  - `concurrency=32`
  - `spans-per-request=5`
  - `payload-variants=1024`
- startup model: clean start per round and per target

It does not try to benchmark query, restart latency, or cluster mode yet.

## CLI

```bash
cargo run -p vtbench -- official-compare \
  --official-bin=/path/to/victoria-traces-prod \
  --rust-bin=/path/to/vtapi \
  --rounds=5
```

Important knobs:

- `--targets=official,disk,memory`
- `--output-dir=/tmp/rust-vt-official-bench`
- `--official-port=13081`
- `--memory-port=13082`
- `--disk-port=13083`
- `--startup-timeout-secs=30`

All normal OTLP load-shape flags still work, except `--url` and `--report-file`, which the harness owns.

## Output

The harness writes:

- `manifest.json`
- `round-XX/<target>.json`
- `round-XX/<target>.stdout.log`
- `round-XX/<target>.stderr.log`
- `summaries/<target>-median.json`
- `summary.json`

The median summaries are the intended review artifact for "fresh N-round median" comparisons.

## Readiness rules

- `official` waits for `/metrics`
- Rust targets wait for `/readyz`

That keeps the benchmark from starting before a target is actually ready to serve ingest.
