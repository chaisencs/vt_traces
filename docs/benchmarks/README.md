# Benchmark QA Catalog

This directory defines the benchmark scenarios that are allowed to become blocking QA gates or public performance claims.

Each blocking scenario must have:

- a stable command shape
- a reference solution
- an explicit noise budget
- disqualifying deviations
- a replayable artifact bundle

Current starter catalog:

- `tiny-memory-ingest`
- `public-otlp-load`

The benchmark harness is not allowed to publish or gate on an unnamed scenario.
