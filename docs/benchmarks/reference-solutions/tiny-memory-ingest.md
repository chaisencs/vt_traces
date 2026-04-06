# tiny-memory-ingest

Purpose:

- prove the harness can run a deterministic in-process ingest scenario and produce a canonical bundle

Command shape:

```bash
vtbench storage-ingest --rows=20 --batch-size=5 --artifacts-root=<root>
```

Expected outputs:

- `operations=20`
- `errors=0`
- canonical bundle with `manifest.json`, `report.json`, `env.json`, `context.json`, `commands.json`, `decision.json`, `rerun.sh`

Noise budget:

- throughput and latency are not gated yet
- metadata and artifact presence are exact-match requirements

Disqualifying deviations:

- missing replay ID
- missing git SHA
- missing canonical files
- mismatched operation count
