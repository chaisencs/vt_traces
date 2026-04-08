# Harness QA & Quality Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a repo-native QA harness for `rust_victoria_trace` that can prove correctness, benchmark integrity, recoverability, and release-readiness instead of only proving that crate tests happen to pass.

**Architecture:** Keep the current crate/unit/integration/release suite as the deterministic base layer, then add a `vtbench`-centered harness stack on top of it: fixture-driven end-to-end runs, replay bundles, benchmark guardrails, regression comparators, fault/recovery drills, and CI gates. The harness must validate not only the storage engines, but also the benchmark process itself, so performance claims cannot silently drift due to stale data paths, warm processes, mismatched flags, or partial evidence.

**Tech Stack:** Rust workspace (`vtcore`, `vtingest`, `vtstorage`, `vtquery`, `vtapi`, `vtbench`), `axum`, `reqwest`, GitHub Actions, local temp dirs under `var/`, existing `StorageStatsSnapshot` metrics, existing disk WAL/part engine, existing `vtbench` load modes.

---

## Why this plan exists

This repository already has a solid engineering baseline:

- crate-level unit and integration tests across all crates
- disk recovery, quorum, TLS, Jaeger/Tempo compatibility, and overload tests
- `vtbench` for ingest/query/load benchmarking
- Linux release workflow with Docker smoke and release artifacts

But this is still not a full harness-quality QA system.

Today, the repo is strongest at:

- deterministic software correctness
- API/storage integration coverage
- release build verification

Today, the repo is weakest at:

- proving benchmark runs are fair and reproducible
- replaying any suspicious run exactly
- converting incidents / regressions into permanent eval cases
- guarding against invalid comparisons
- validating the harness itself as rigorously as the storage engine

Because `vtbench` is now a public benchmark and release-evidence source, the harness itself must be treated as production code.

## Guiding principles from the agent-era QA guidelines

This plan treats QA as an execution control system, not as a loose set of tests.

The build-out must follow these principles:

- QA is part of the daily development loop. Code changes, benchmark-shape changes, workflow changes, release-note evidence changes, and guardrail/policy changes must all trigger evals.
- Behavioral coverage matters more than line coverage. For this repo, the highest-value coverage is “does the harness do exactly what it claims”, not “did this helper function execute”.
- Environment noise is first-class. CPU shape, timeout values, concurrency, warmup, temp-dir freshness, dependency caching, and network assumptions must be captured and reported.
- Critical scenarios need reference solutions. If a harness scenario cannot be solved or judged deterministically, it does not belong in the blocking suite.
- Transcript review is a formal QA asset. `stdout`, `stderr`, command logs, per-target reports, and release summaries are reviewable evidence, not disposable debug output.
- Every public claim must be backed by replayable evidence. A fast run without a reproducible bundle is not a valid benchmark result.

## Current baseline inventory

### Already in place

- Deterministic tests:
  - [http_api_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs)
  - [disk_engine_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs)
  - [batching_engine_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/batching_engine_tests.rs)
  - [memory_engine_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/memory_engine_tests.rs)
  - [protobuf_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/tests/protobuf_tests.rs)
  - [flatten_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/tests/flatten_tests.rs)
  - [codec_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/tests/codec_tests.rs)
  - [model_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/tests/model_tests.rs)
  - [query_service_tests.rs](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtquery/tests/query_service_tests.rs)
- Basic harness functionality:
  - `vtbench storage-ingest`
  - `vtbench storage-query`
  - `vtbench http-ingest`
  - `vtbench otlp-protobuf-load`
  - `vtbench disk-trace-block-append`
- Release/CI:
  - [linux-release.yml](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/.github/workflows/linux-release.yml)

### Missing harness-grade capabilities

- Canonical run artifact schema
- Replay bundle and exact rerun command output
- Guardrails against unfair / mismatched benchmark comparisons
- Dedicated `vtbench` integration tests outside option parsing
- Baseline compare tooling with explicit pass/fail tolerances
- Nightly soak/fault matrix
- Incident-to-eval loop
- Public benchmark publication gate that requires complete evidence

## Hard quality gates

Any future harness work is only acceptable if all of the following are true:

1. A benchmark run can be replayed from captured artifacts with the same mode, flags, storage config, and URLs.
2. A benchmark report is rejected if it mixes warm and cold starts, mismatched sync policies, mismatched shard counts, or mismatched binary revisions.
3. A benchmark summary cannot be published unless it includes:
   - git SHA
   - exact command
   - target URLs
   - storage config
   - clean-start / warm-start declaration
   - run timestamp
4. A failed or suspicious run can be promoted into a permanent regression case with minimal manual work.
5. Release evidence stays split into:
   - correctness
   - benchmark integrity
   - release packaging
   rather than hiding all quality behind one green CI job.

## Control-plane responsibility boundaries

The external QA guidelines require six platform responsibilities to be explicit. In this repository they map as follows:

### Policy engine

Responsible for:

- deciding what a valid benchmark claim is
- validating run-shape invariants before execution
- rejecting incomplete or unfair comparisons

Likely implementation surface:

- `vtbench` preflight validation
- comparator rules
- release evidence checks

### Tool runtime

Responsible for:

- launching local targets
- issuing HTTP or gRPC load
- collecting metrics and responses
- capturing command, exit-code, and timing traces

Likely implementation surface:

- `crates/vtbench/src/main.rs`
- extracted runner modules under `crates/vtbench/src/`

### Guardrail layer

Responsible for:

- blocking stale or unsafe storage paths
- blocking mismatched comparison arms
- blocking publication from incomplete metadata
- enforcing local-only cleanup boundaries for temp dirs and spawned processes

### Checkpoint / replay layer

Responsible for:

- persisting canonical artifacts
- producing exact rerun commands
- tracking replay IDs and incident lineage
- supporting code-only and full-run reruns from captured bundles

### Eval / grader layer

Responsible for:

- rule grading
- execution grading
- human transcript review and public-claim review
- baseline and regression comparisons

### Telemetry / experiment layer

Responsible for:

- nightly sweeps
- noise-budget tracking
- benchmark history and baseline drift
- release evidence lineage
- incident-to-regression conversion

## The project-specific QA stack

This adapts the agent-era QA framework to this repository’s actual shape.

### Layer 1: Deterministic correctness

Purpose:

- prove parsers, codecs, storage engines, APIs, and metrics counters behave correctly

Keep as hard gates:

- `cargo test --workspace -- --nocapture`
- targeted crate/package tests for touched areas
- schema, parser, and state-machine tests for new harness logic

Gaps to add:

- `vtbench` report schema tests
- `vtbench` replay bundle tests
- benchmark option incompatibility tests
- benchmark artifact serialization / deserialization tests

### Layer 2: Task harness / sandbox validation

Purpose:

- prove `vtbench` can run full end-to-end scenarios in controlled, isolated environments and that the measured outcomes match the intended task shape

Examples:

- a run against local memory storage ingests exactly the expected number of rows
- a fault-injection run records the expected error count and interval windows
- a disk run really uses a fresh temp dir unless explicitly told otherwise
- a benchmark fails fast if target URLs or storage config are inconsistent with the declared run mode
- a cluster scenario uses the declared roles and never silently falls back to a different path

This is the single most important missing layer today.

This layer also owns the semantic system matrix:

Purpose:

- prove the same benchmark shape means the same thing across:
  - memory
  - disk
  - cluster roles
  - official VictoriaTraces comparison runs

Matrix dimensions:

- ingest protocol:
  - OTLP/HTTP JSON
  - OTLP/HTTP protobuf
  - OTLP/gRPC traces
  - OTLP logs where relevant
- storage mode:
  - memory
  - disk
- sync policy:
  - `none`
  - `data`
- start state:
  - clean start
  - warm restart
- benchmark mode:
  - throughput
  - latency
  - fault-injected
  - direct kernel append

### Layer 3: Behavior evals

Purpose:

- prove the harness does what the operator asked, does not do more than requested, and does not self-certify bad evidence

Behavioral coverage should dominate line coverage in this repository. The priority cases are:

- claimed clean-start runs must actually start from clean storage
- “same-host same-shape” comparisons must refuse mismatched flags or env
- a public benchmark mode must refuse missing control arms or missing metadata
- fault runs must surface the failure window instead of silently collapsing it into one aggregate number
- reruns must disclose variance instead of silently treating drift as success
- release-note summaries must reference replayable evidence rather than ad hoc terminal output

This layer uses:

- rule graders for hard requirements
- execution graders for measured outcomes
- human review rubrics for publication honesty and caveat visibility

### Layer 4: Runtime guardrails

Purpose:

- reject invalid evidence before it becomes a dashboard or release note

Guardrails must block:

- stale non-empty data dirs during claimed clean-start runs
- mixed binary SHAs in the same comparison batch
- mixed `VT_STORAGE_SYNC_POLICY`
- mixed `VT_STORAGE_TRACE_SHARDS`
- mixed target segment size or body limits when a run is claimed “same-host same-shape”
- missing warmup declaration
- missing official control arm when a run is claimed “beats official”
- cleanup or file writes outside the run-owned temp directories
- publication attempts from bundles with missing replay IDs

Guardrail QA must be evaluated like a detection system, not just asserted in prose. The blocking plan must define:

- a negative corpus of invalid benchmark specs
- a positive corpus of valid benchmark specs
- false positive rate for valid runs
- false negative rate for invalid runs
- high-severity recall for “unfair public comparison” and “unsafe filesystem cleanup”
- explicit deny-and-continue paths such as:
  - missing official arm -> demote run to non-public local comparison
  - stale data dir -> allocate a new temp dir or stop
  - missing metadata -> persist run locally but block publication

### Layer 5: Recoverability and replay

Purpose:

- make every benchmark run inspectable and reproducible

A replay bundle must contain:

- exact command
- git SHA
- built binary path or version fingerprint
- env snapshot
- port allocation
- storage paths
- start timestamp
- end timestamp
- stdout/stderr
- report JSON
- `/metrics` snapshot when applicable
- target metadata

Replay must support:

- rerun exact command
- rerun only one target arm
- compare rerun against original summary
- attach the replay ID to follow-up incidents and regression cases

### Layer 6: Production / release QA

Purpose:

- ensure public performance claims and release artifacts are backed by reproducible evidence

CI should be split into:

- PR smoke correctness
- PR harness correctness
- nightly performance matrix
- release candidate evidence gate
- release publishing gate

This layer must also track whether the system is truly improving rather than merely moving a benchmark:

- public benchmark pass rate
- replay success rate
- regression reopen rate
- baseline drift outside allowed noise budget
- incident-to-regression conversion lag
- transcript review completion rate for release-facing runs

## Artifact model

All harness artifacts should use one canonical layout under `var/` and one stable JSON schema.

### Proposed local artifact layout

- `var/harness-runs/<run-id>/manifest.json`
- `var/harness-runs/<run-id>/stdout.log`
- `var/harness-runs/<run-id>/stderr.log`
- `var/harness-runs/<run-id>/report.json`
- `var/harness-runs/<run-id>/metrics.txt`
- `var/harness-runs/<run-id>/env.json`
- `var/harness-runs/<run-id>/context.json`
- `var/harness-runs/<run-id>/commands.json`
- `var/harness-runs/<run-id>/grader.json`
- `var/harness-runs/<run-id>/decision.json`
- `var/harness-runs/<run-id>/rerun.sh`
- `var/harness-runs/<run-id>/targets/<target-name>.json`

### Proposed tracked baseline layout

- `docs/benchmarks/baselines/<scenario>.json`
- `docs/benchmarks/incidents/<incident-id>.md`
- `docs/benchmarks/regressions/<case-id>.json`
- `docs/benchmarks/reference-solutions/<scenario>.md`
- `docs/benchmarks/transcript-reviews/<review-id>.md`

Tracked files should stay small and human-reviewed.
Bulk artifacts stay under `var/` and never enter git by default.

Every canonical run bundle must capture:

- task description
- input context snapshot
- environment manifest
- tool and command trace
- before and after observable results
- final decision
- grader result
- replay ID

The environment manifest must explicitly record:

- OS, kernel, and base image or host fingerprint
- CPU model, core count, and memory ceiling
- concurrency, timeout, and warmup settings
- network policy and target locality assumptions
- workspace initialization method
- dependency and cache policy
- random seed, payload seed, or fixed input source
- git SHA and dirty-tree state

## Grading model

This repository does not need LLM-first grading as a hard dependency.

It does need three grader classes:

### Rule graders

- clean-start declared and enforced
- command contains required flags
- same-shape comparison arms use identical benchmark knobs
- release benchmark includes official + memory + disk where claimed
- report file contains mandatory metadata fields

### Execution graders

- ingest count matches expected completed requests
- expected tests passed
- expected endpoints responded
- fault windows produced expected failures
- replay rerun produces output within allowed variance

### Review graders

Human-reviewed, not model-required:

- is this benchmark disclosure honest?
- is the comparison fair?
- is the public summary hiding important caveats?
- did the transcript show unexplained retries, restarts, or manual cleanup that the summary omitted?

This grader is only for public docs/release notes, not for core correctness.

## Reference scenarios and transcript review

Critical harness scenarios need deterministic reference solutions before they become blocking gates.

The minimum reference catalog should include:

- tiny memory ingest run with exact expected request and row counts
- tiny disk clean-start run with exact temp-dir and artifact expectations
- official vs memory vs disk public comparison shape with required metadata and control arms
- one injected-failure scenario with a known failure window and expected reporting behavior
- one replay scenario where the rerun must match within a declared variance budget

Each reference scenario needs:

- scenario definition
- exact command shape
- accepted environment bounds
- expected observable outputs
- allowed noise budget
- disqualifying deviations

Transcript review is a first-class QA asset, not an ad hoc debugging step.

For every release-facing benchmark or suspicious regression, the reviewer should inspect:

- stdout and stderr chronology
- per-target reports
- env manifest
- final public summary
- any cleanup or retry behavior

The review outcome must be stored as a tracked asset and linked to the replay ID or incident.

## CI and release lanes

### Lane A: Fast PR lane

Runs on every PR:

- `cargo test --workspace -- --nocapture`
- `cargo test -p vtbench -- --nocapture`
- new `vtbench` harness self-validation tests
- one local memory smoke benchmark with tiny duration

Goal:

- catch correctness regressions fast

### Lane B: Harness integrity lane

Runs on PRs touching `vtbench`, benchmark docs, release workflow files, comparator rules, or any policy that changes what a valid benchmark claim means:

- benchmark artifact schema tests
- replay bundle tests
- guardrail tests
- report comparator tests
- one local disk smoke benchmark

Goal:

- prove the harness itself still tells the truth

### Lane C: Nightly performance lane

Runs on schedule:

- clean-start official vs memory vs disk
- fresh single run
- fresh 5-round median
- one fault-injection run
- one disk direct-kernel sweep

Goal:

- catch silent drift and noisy regressions without blocking every PR

### Lane D: Release evidence lane

Runs before public benchmark updates or formal release:

- replay one saved baseline
- rerun public benchmark shape from clean start
- require matching benchmark metadata
- require generated summary artifact
- require release note evidence references

Goal:

- stop “one lucky run” from reaching public docs

Rule: code, benchmark-shape, policy, workflow, and publication-format changes all trigger some eval lane. Nothing that changes trust semantics ships without QA.

## Implementation phases

### Task 0: Define the benchmark scenario catalog, noise model, and reference solutions

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/README.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/noise-model.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/reference-solutions/README.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/reference-solutions/tiny-memory-ingest.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/reference-solutions/public-otlp-load.md`

**Step 1: Write the failing documentation expectations**

Define the minimum scenario catalog and require each blocking scenario to declare:

- exact command shape
- expected outputs
- acceptable noise budget
- disqualifying deviations

**Step 2: Validate the layout**

Run:

```bash
git diff --check
```

Expected: PASS after the docs are created.

**Step 3: Use these scenarios as the source of truth**

No blocking harness test or public benchmark mode should exist without a mapped reference scenario.

### Task 1: Establish canonical harness artifacts

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/report.rs`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/tests/report_schema_tests.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/main.rs`

**Step 1: Write the failing tests**

Add tests that require every persisted report to include:

- run id
- git SHA
- mode
- command-line options
- target metadata
- start/end timestamps
- duration
- success/error counts

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p vtbench report_schema -- --nocapture
```

Expected: FAIL because current `vtbench` output is not canonicalized into one stable artifact schema.

**Step 3: Write the minimal implementation**

Move report writing into a dedicated module and define one stable JSON schema used by every mode.

**Step 4: Re-run the tests**

Run:

```bash
cargo test -p vtbench report_schema -- --nocapture
```

Expected: PASS.

### Task 2: Add harness self-validation integration tests

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/tests/harness_end_to_end_tests.rs`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/tests/common/mod.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/main.rs`

**Step 1: Write the failing tests**

Add end-to-end tests that:

- boot an in-process memory-backed API
- run `vtbench http-ingest`
- assert completed request count and ingested row count are internally consistent
- run one fault-injected scenario and assert error counts and report timeline are sane

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p vtbench harness_end_to_end -- --nocapture
```

Expected: FAIL because current harness is only lightly unit-tested.

**Step 3: Write the minimal implementation**

Expose enough helper logic for integration tests to launch local targets and validate results.

**Step 4: Re-run the tests**

Run:

```bash
cargo test -p vtbench harness_end_to_end -- --nocapture
```

Expected: PASS.

### Task 3: Add replay bundle and exact rerun support

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/replay.rs`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/tests/replay_bundle_tests.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/main.rs`

**Step 1: Write the failing tests**

Require a run bundle to emit:

- `manifest.json`
- `report.json`
- `rerun.sh`
- `env.json`

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p vtbench replay_bundle -- --nocapture
```

Expected: FAIL because replay bundles do not exist yet.

**Step 3: Write the minimal implementation**

Persist all required artifacts under `var/harness-runs/<run-id>/`.

**Step 4: Re-run the tests**

Run:

```bash
cargo test -p vtbench replay_bundle -- --nocapture
```

Expected: PASS.

### Task 4: Add benchmark guardrails

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/tests/guardrail_tests.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/main.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/report.rs`

**Step 1: Write the failing tests**

Add tests that reject:

- non-empty data dirs during declared clean-start disk runs
- mixed `sync_policy`
- missing official arm for official comparison mode
- missing git SHA in report metadata
- missing warmup declaration in public benchmark mode

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p vtbench guardrail_tests -- --nocapture
```

Expected: FAIL because current harness does not enforce these policies.

**Step 3: Write the minimal implementation**

Implement explicit preflight validation before a benchmark run starts.

**Step 4: Re-run the tests**

Run:

```bash
cargo test -p vtbench guardrail_tests -- --nocapture
```

Expected: PASS.

### Task 5: Add baseline comparator and regression cases

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/compare.rs`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/tests/compare_tests.rs`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/baselines/.gitkeep`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`

**Step 1: Write the failing tests**

Require a compare command/path that can:

- read a saved baseline
- compare new report vs baseline
- fail if throughput or p99 regress past configured tolerances
- print whether the run is within noise budget

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p vtbench compare_tests -- --nocapture
```

Expected: FAIL because no comparator exists.

**Step 3: Write the minimal implementation**

Implement comparison logic for:

- exact metadata match checks
- tolerance-based throughput and p99 checks
- human-readable summary output
- replay-ID aware comparison output for incident linkage

**Step 4: Re-run the tests**

Run:

```bash
cargo test -p vtbench compare_tests -- --nocapture
```

Expected: PASS.

### Task 6: Add harness fault and recovery drills

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/tests/fault_recovery_tests.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtbench/src/main.rs`

**Step 1: Write the failing tests**

Add scenarios for:

- injected 503 window during HTTP ingest
- disk reopen after a harness-driven run
- replaying a captured bundle after a failed run
- asserting post-run metrics are persisted in the bundle

**Step 2: Run the tests to verify they fail**

Run:

```bash
cargo test -p vtbench fault_recovery_tests -- --nocapture
cargo test -p vtstorage disk_engine_recovers_ -- --nocapture
```

Expected: first command FAILs before implementation.

**Step 3: Write the minimal implementation**

Make the harness persist enough metadata to diagnose and replay failures.

**Step 4: Re-run the tests**

Expected: PASS.

### Task 7: Add transcript review and incident-to-regression assets

**Files:**
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/README.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/transcript-review-template.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/incident-template.md`
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/regression-template.md`

**Step 1: Define the operating loop**

Any suspicious run or release-facing benchmark must produce:

- replay ID
- transcript review record
- incident note where applicable
- regression case linkage when fixed

**Step 2: Validate document integrity**

Run:

```bash
git diff --check
```

Expected: PASS.

### Task 8: Split CI into harness-aware lanes

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/.github/workflows/harness-qa.yml`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/.github/workflows/linux-release.yml`

**Step 1: Write the failing workflow expectations**

Define the required lanes:

- PR smoke
- harness integrity
- nightly performance
- release evidence

**Step 2: Validate workflow syntax**

Run:

```bash
git diff --check
```

Expected: PASS after workflow files are created correctly.

**Step 3: Write the minimal implementation**

Keep the current release workflow, but move benchmark/harness-specific checks into their own workflow so release packaging and benchmark integrity are separable.

**Step 4: Re-run local verification**

Run:

```bash
cargo test --workspace -- --nocapture
cargo test -p vtbench -- --nocapture
```

Expected: PASS.

### Task 9: Add incident-to-eval operating loop and change-triggered eval policy

**Files:**
- Create: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/benchmarks/incident-to-eval.md`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/production-release-guide.md`
- Modify: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`

**Step 1: Document the loop**

Any benchmark incident must be converted into:

- replay bundle
- regression case
- root-cause note
- permanent test or comparator rule where applicable
- transcript review link

**Step 2: Define change-triggered eval policy**

Document that code, benchmark-shape, workflow, publication-format, and guardrail/policy changes all trigger an eval path before merge or release.

**Step 3: Define release gate policy**

A public benchmark claim is blocked if the incident-to-eval loop has unresolved open items for that benchmark mode.

**Step 4: Verify docs stay aligned**

Run:

```bash
git diff --check
```

Expected: PASS.

## Rollout order

Recommended implementation order:

1. Task 0: scenario catalog and reference solutions
2. Task 1: canonical artifacts
3. Task 2: harness self-validation
4. Task 3: replay bundle
5. Task 4: guardrails
6. Task 5: baseline comparator
7. Task 6: fault/recovery drills
8. Task 7: transcript review and incident assets
9. Task 8: CI lanes
10. Task 9: incident-to-eval docs and release policy

This order is intentional:

- reference scenarios first, so every later gate has a solvable target
- artifact schema first
- then prove harness correctness
- then make failures replayable
- then add strict gates
- then formalize review and incident assets
- only after that promote the system into CI and release policy

## Non-goals for the first version

Do not include these in v1:

- LLM-based graders as hard blockers
- cross-host noise normalization
- auto-generated public marketing text
- hardware lab orchestration
- full cluster chaos suite

Those can come later, but they should not delay the harness base layer.

## Success criteria

This plan is successful when the repository can honestly claim all of the following:

1. `vtbench` runs are replayable.
2. Benchmark comparison rules are machine-checkable.
3. Public benchmark reports cannot be emitted from incomplete evidence.
4. Harness regressions fail in CI before public numbers drift.
5. Every blocking scenario has a reference solution and a declared noise budget.
6. Every release-facing run has a transcript review record and replay ID.
7. Any benchmark incident can be turned into a permanent regression case in one follow-up patch.
