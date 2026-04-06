# Formal Release Gate Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make the current release branch production-friendly on common 64-bit Linux and merge-ready for `master`.

**Architecture:** Tighten the release path rather than changing the storage engine itself. The work focuses on release artifacts, Linux/container validation, and user-facing release docs so the branch can be merged and tagged without additional release-prep churn.

**Tech Stack:** Rust workspace, GitHub Actions, Docker, Markdown docs

---

### Task 1: Strengthen the Linux release workflow

**Files:**
- Modify: `.github/workflows/linux-release.yml`
- Test: local YAML inspection + branch workflow run

**Step 1: Add artifact integrity output**

Generate `.sha256` files next to each Linux tarball and upload/publish them with the release artifacts.

**Step 2: Add a Docker smoke gate**

Extend the `x86_64` Linux job to build the repository `Dockerfile`, run the container, and assert `GET /healthz` succeeds.

**Step 3: Re-read the workflow**

Check the final YAML for job order, release artifact names, and tag publish behavior.

### Task 2: Make the container defaults production-friendlier

**Files:**
- Modify: `Dockerfile`

**Step 1: Run as a non-root user**

Create an application user, prepare `/var/lib/rust-victoria-trace`, and switch the runtime image to that user.

**Step 2: Align defaults with the production guide**

Set safe runtime defaults for the disk engine container so a plain `docker run` starts from a production-biased baseline.

### Task 3: Complete the public Linux release docs

**Files:**
- Modify: `README.md`
- Modify: `docs/production-release-guide.md`

**Step 1: Document release artifact usage**

Add a prebuilt Linux quickstart covering tarball extraction, checksum verification, and direct binary startup.

**Step 2: Document container startup**

Add a `docker run` example and explain the expected health and storage checks.

**Step 3: Refresh the merge gate text**

Update the guide so it reflects the now-green branch workflow instead of describing it as a remaining future gate.

### Task 4: Verify the formal release gate

**Files:**
- Verify only

**Step 1: Run workspace tests**

Run: `cargo test --workspace -- --nocapture`

**Step 2: Run release build**

Run: `cargo build --release -p vtapi -p vtbench`

**Step 3: Run Docker build and smoke locally**

Run the repository `Dockerfile`, start the container, and verify `GET /healthz`.

**Step 4: Push and confirm workflow**

Push the branch and confirm `.github/workflows/linux-release.yml` is green.
