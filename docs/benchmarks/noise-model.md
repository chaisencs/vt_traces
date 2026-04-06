# Benchmark Noise Model

The harness treats environment noise as part of the test result, not as an afterthought.

Every run bundle must capture:

- OS and architecture
- CPU count
- current git SHA and dirty-tree state
- benchmark concurrency
- warmup and duration settings
- target storage path assumptions
- environment variables relevant to `VT_*`, `CARGO_*`, `PATH`, and `RUST_LOG`

Initial policy:

- functional harness tests are binary pass/fail
- replay comparisons should use exact metadata match plus tolerance checks
- public benchmark claims must disclose clean-start vs warm-start and declared control arms

This document is the source of truth for future comparator tolerances.
