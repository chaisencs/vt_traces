# Throughput Canary Runbook

This runbook is the shortest safe path for putting the `throughput` tier into production canary while preserving the VictoriaTraces-style split between `storage`, `insert`, and `select`.

Use it when:

- you want the high-throughput profile, not `stable`
- you are deploying on Linux VMs or bare metal with `systemd`
- you need a canary path first, not an all-at-once cutover

Do not use this runbook for an in-place replacement of the existing VictoriaTraces data directory. Keep the old deployment intact and switch traffic at the load balancer or gateway layer.

## Files Added For This Runbook

- `ops/systemd/rust-victoria-trace-storage@.service`
- `ops/systemd/rust-victoria-trace-insert.service`
- `ops/systemd/rust-victoria-trace-select.service`
- `ops/env/*.env.example`
- `scripts/production/run-role.sh`
- `scripts/production/smoke-canary.sh`
- `scripts/production/rollout-canary.sh`
- `scripts/production/rollback-canary.sh`

## 1. Prep Tonight

If you need a release bundle from a Linux `x86_64` build machine, use:

```bash
./scripts/production/package-linux-x86_64.sh
```

That produces:

- `dist/rust-victoria-trace-linux-x86_64.tar.gz`
- `dist/rust-victoria-trace-linux-x86_64.tar.gz.sha256`

Copy both files to the target hosts before the rest of this runbook.

Copy the role env files to the hosts and fill in real values:

```bash
sudo mkdir -p /etc/rust-victoria-trace
sudo cp ops/env/common.env.example /etc/rust-victoria-trace/common.env
sudo cp ops/env/storage-a.env.example /etc/rust-victoria-trace/storage-a.env
sudo cp ops/env/storage-b.env.example /etc/rust-victoria-trace/storage-b.env
sudo cp ops/env/insert.env.example /etc/rust-victoria-trace/insert.env
sudo cp ops/env/select.env.example /etc/rust-victoria-trace/select.env
```

Copy the units and scripts:

```bash
sudo cp ops/systemd/*.service /etc/systemd/system/
sudo cp scripts/production/run-role.sh /usr/local/bin/run-vt-role.sh
sudo cp scripts/production/smoke-canary.sh /usr/local/bin/smoke-canary.sh
sudo cp scripts/production/rollout-canary.sh /usr/local/bin/rollout-canary.sh
sudo cp scripts/production/rollback-canary.sh /usr/local/bin/rollback-canary.sh
sudo chmod +x /usr/local/bin/run-vt-role.sh /usr/local/bin/smoke-canary.sh /usr/local/bin/rollout-canary.sh /usr/local/bin/rollback-canary.sh
```

Place the release binary on every host:

```bash
sudo install -m 0755 vtapi /usr/local/bin/vtapi
```

## 2. Required Topology

Recommended minimum canary topology:

- `storage-a`
- `storage-b`
- `insert-a`
- `select-a`

This keeps the same operational split as VictoriaTraces:

- `storage` owns the local disks
- `insert` accepts external writes
- `select` accepts external reads and admin probes

## 3. Required Config Values

For `throughput`, storage hosts must set all of these:

- `VT_TRACE_INGEST_PROFILE=throughput`
- `VT_STORAGE_SYNC_POLICY=none`
- `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=268435456`

For all roles, fill in:

- real hostnames in `VT_CLUSTER_STORAGE_NODES`
- real hostnames in `VT_CLUSTER_CONTROL_NODES`
- real bearer tokens
- real storage paths

Do not reuse the old VictoriaTraces data directory contents.

## 4. Start Order

Start order is fixed:

1. `storage`
2. `insert`
3. `select`

Dry-run first:

```bash
VT_CANARY_STORAGE_UNITS="rust-victoria-trace-storage@a rust-victoria-trace-storage@b" \
VT_CANARY_INSERT_UNIT=rust-victoria-trace-insert \
VT_CANARY_SELECT_UNIT=rust-victoria-trace-select \
/usr/local/bin/rollout-canary.sh
```

Execute:

```bash
VT_CANARY_STORAGE_UNITS="rust-victoria-trace-storage@a rust-victoria-trace-storage@b" \
VT_CANARY_INSERT_UNIT=rust-victoria-trace-insert \
VT_CANARY_SELECT_UNIT=rust-victoria-trace-select \
/usr/local/bin/rollout-canary.sh --execute
```

## 5. Health And Smoke

Run the canary smoke after services are active:

```bash
VT_CANARY_WRITE_URL=http://insert-a:13001 \
VT_CANARY_READ_URL=http://select-a:13002 \
VT_CANARY_API_BEARER_TOKEN=change-me-public \
/usr/local/bin/smoke-canary.sh
```

The smoke script verifies:

- `insert` `healthz`
- `select` `healthz`
- OTLP/HTTP JSON trace write
- `trace-by-id`
- `search`
- `services`
- `field-values`
- `metrics` exposure for `vt_storage_trace_live_update_queue_depth` and `vt_storage_trace_seal_queue_depth`

Also verify on the storage hosts:

- the configured `VT_STORAGE_PATH` exists
- WAL files appear
- `.part` files appear once sealing starts

## 6. Traffic Ramp Tomorrow

Recommended ramp:

1. `1%` write traffic for `10m`
2. `5%` write traffic for `10m`
3. `20%` write traffic for `15m`
4. `50%` write traffic for `15m`
5. `100%` only if all checks stay clean

Keep production reads on the old system until the canary smoke and metrics stay healthy. Then move a small read slice to `select`.

## 7. Metrics To Watch

Watch these first:

- `vt_storage_trace_live_update_queue_depth`
- `vt_storage_trace_seal_queue_depth`
- `vt_storage_trace_head_segments`
- `vt_storage_trace_head_rows`
- process RSS on `storage`
- `insert` write latency and `5xx`
- `select` query latency and `5xx`

Rollback immediately if any of these happen:

- query `5xx` or timeouts climb
- `seal_queue_depth` keeps growing instead of settling
- RSS keeps rising without stabilizing
- smoke checks fail for `trace-by-id`, `search`, `services`, or `field-values`

## 8. Rollback

Rollback trigger means:

1. shift traffic back to VictoriaTraces at the LB or gateway
2. stop `select`
3. stop `insert`
4. stop `storage` once traffic is drained

Dry-run:

```bash
/usr/local/bin/rollback-canary.sh
```

Execute:

```bash
/usr/local/bin/rollback-canary.sh --execute
```

## 9. Final Go/No-Go Checklist

All of these should be true before you widen traffic:

- `healthz` is green on all active roles
- `metrics` is scrapeable
- smoke script passes
- storage paths show WAL and `.part`
- LB can shift traffic back to VictoriaTraces in one step
- on-call knows the rollback trigger and command
