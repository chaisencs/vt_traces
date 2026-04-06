# Incident To Eval Loop

Every benchmark incident must become a durable QA asset.

The required loop is:

`incident -> transcript review -> root cause -> replay case -> regression asset -> verification -> release decision`

## Mandatory Steps

1. Preserve or locate the replay bundle.
2. Create a transcript review using `docs/benchmarks/transcript-review-template.md`.
3. Create an incident note using `docs/benchmarks/incident-template.md`.
4. Decide which permanent guard should prevent recurrence:
   - test
   - comparator rule
   - workflow gate
   - publication policy
5. Create a regression asset using `docs/benchmarks/regression-template.md`.
6. Link the fix commit and verification command.

## Release Policy

Public benchmark updates are blocked when:

- a benchmark mode has unresolved open incidents
- a release-facing run has no replay ID
- a release-facing run has no transcript review
- a claim lacks the required control arms or metadata

## Change-Triggered Eval Policy

The following changes must trigger an eval path before merge or release:

- code changes in `crates/vtbench/**`
- benchmark shape or comparator rule changes
- workflow changes under `.github/workflows/**`
- release note or public benchmark doc changes
- guardrail / publication policy changes
