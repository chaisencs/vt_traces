# public-otlp-load

Purpose:

- define the minimum metadata required before an OTLP public performance claim can be published

Command shape:

```bash
vtbench otlp-protobuf-load --url=<target> --requests=<n> --spans-per-request=<n> --warmup-secs=<n> --public-report=true --comparison-arms=official,memory,disk --artifacts-root=<root>
```

Expected outputs:

- canonical run report with git SHA, raw args, timestamps, duration, and replay ID
- declared control arms include `official`, `memory`, and `disk`
- bundle is replayable

Noise budget:

- functional preflight checks are exact-match
- performance tolerance policy will be filled in by the comparator layer

Disqualifying deviations:

- missing warmup declaration
- missing official control arm
- missing replay bundle
- missing metadata required for publication
