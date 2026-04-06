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

Directory layout:

- `reference-solutions/`: benchmark scenarios with exact command shapes and noise expectations
- `baselines/`: tracked baseline reports or placeholders for approved comparison anchors
- `incidents/`: benchmark incidents that must be reviewed and linked to replay IDs
- `regressions/`: permanent regression cases promoted from incidents
- `transcript-reviews/`: human review assets for release-facing or suspicious runs

Current operator commands:

- create canonical bundle:

```bash
target/debug/vtbench storage-ingest --rows=20 --batch-size=5 --artifacts-root=var/harness-runs
```

- compare two reports:

```bash
target/debug/vtbench compare-report \
  --baseline-file=path/to/baseline.json \
  --candidate-file=path/to/candidate.json \
  --throughput-regression-pct=5 \
  --p99-regression-pct=10
```

Operating rule:

- any public claim must link to a replay bundle
- any suspicious run must produce a transcript review
- any confirmed regression must become a tracked regression case
