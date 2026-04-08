# Production Release Guide

> AI-Agent时代的Trace存储系统正式发布说明
>
> 这份文档不是内部发布备忘录，而是面向开源用户、运维者和合并决策者的正式 release gate。

## 这份文档解决什么问题

如果你准备把 `rust_victoria_trace` 作为一个正式版本发布出去，这份文档回答四个最关键的问题：

- 它是否已经能跑在常见 64 位 Linux 上
- 它是否已经具备可重复的 release artifact
- 它是否已经有足够清楚的部署和运行说明
- 它是否已经达到可 merge 到 `master` 的门槛

当前目标很明确：

- run on common 64-bit Linux systems
- ship repeatable release artifacts
- keep code, docs, and benchmark evidence in a mergeable state

## 一眼看完当前发布状态

| 项目 | 当前状态 |
| --- | --- |
| 目标平台 | `x86_64-unknown-linux-gnu`、`aarch64-unknown-linux-gnu` |
| 二进制交付 | GitHub Actions 产出 Linux tarball 和 `.sha256` |
| 容器交付 | 仓库根目录 `Dockerfile`，non-root 运行，带 `HEALTHCHECK` |
| 推荐运行模式 | `disk` engine，显式 durability 和请求限流 |
| 文档入口 | `README.md`、性能报告、本指南 |
| merge gate | workspace tests、release build、Linux workflow、artifact、Docker smoke、benchmark 证据 |

## Supported Release Targets

The release workflow is set up to validate and publish these Linux targets:

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

Development and benchmarking may still happen on macOS, but Linux is the release target.

## Release Artifacts

The repository now carries three release-friendly entry points:

1. GitHub Actions workflow: `.github/workflows/linux-release.yml`
2. Container build: `Dockerfile`
3. Public benchmark write-up: `docs/2026-04-06-otlp-ingest-performance-report.md`

The GitHub Actions workflow does two different jobs on Linux:

- native `x86_64` test + release build
- cross-built `aarch64` release build

For the `x86_64` job, the workflow also builds the repository `Dockerfile` and smoke-checks `GET /healthz`.

When a tag matching `v*` is pushed, the workflow publishes tarball artifacts and `.sha256` checksum files for both Linux targets.

## 对外用户如何获得它

正式 release 对外提供两条主路径：

1. 预构建 Linux tarball
   适合裸机、VM、传统发布流程、systemd 管理。
2. Docker 镜像构建入口
   适合容器环境、Kubernetes、PoC 和 CI/CD 封装。

如果你是第一次上线，优先建议：

- Linux 上直接跑 `disk` engine
- 显式设置存储路径、sync 策略、segment 大小和请求限流
- 上线前先验证 `healthz`、`metrics` 和 WAL / `.part` 文件生成

## Linux Binary Quickstart

After tagging a release, GitHub Releases will publish:

- `rust-victoria-trace-linux-x86_64.tar.gz`
- `rust-victoria-trace-linux-x86_64.tar.gz.sha256`
- `rust-victoria-trace-linux-aarch64.tar.gz`
- `rust-victoria-trace-linux-aarch64.tar.gz.sha256`

Typical `x86_64` startup:

```bash
RELEASE=v0.1.0
curl -LO "https://github.com/chaisencs/vt_traces/releases/download/${RELEASE}/rust-victoria-trace-linux-x86_64.tar.gz"
curl -LO "https://github.com/chaisencs/vt_traces/releases/download/${RELEASE}/rust-victoria-trace-linux-x86_64.tar.gz.sha256"
sha256sum -c rust-victoria-trace-linux-x86_64.tar.gz.sha256
tar -xzf rust-victoria-trace-linux-x86_64.tar.gz
cd linux-x86_64
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=/var/lib/rust-victoria-trace \
VT_STORAGE_SYNC_POLICY=data \
VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608 \
VT_MAX_REQUEST_BODY_BYTES=8388608 \
VT_API_CONCURRENCY_LIMIT=1024 \
./vtapi
```

Recommended first checks:

- `curl http://127.0.0.1:13000/healthz`
- `curl http://127.0.0.1:13000/metrics | head`
- confirm WAL and `.part` files appear under the configured storage path

## Container Quickstart

The repository `Dockerfile` now runs as a non-root user and defaults to a production-biased disk configuration.

```bash
docker build -t rust-victoria-trace:local .
docker run --rm \
  -p 13000:13000 \
  -v "$(pwd)/var/vt-docker:/var/lib/rust-victoria-trace" \
  rust-victoria-trace:local
```

Recommended first checks for the container path:

- `curl http://127.0.0.1:13000/healthz`
- `curl http://127.0.0.1:13000/metrics | head`
- confirm the mounted directory contains WAL and `.part` files

## Production Runtime Recommendation

Do not treat the current runtime knobs as one fuzzy deployment mode. The release contract is now split into two explicit tiers:

| release tier | goal | required config | segment behavior | recommendation |
| --- | --- | --- | --- | --- |
| `stable` | durability / operational stability first | `VT_TRACE_INGEST_PROFILE=default` + `VT_STORAGE_SYNC_POLICY=data` | keep the normal segment size path, default `8388608` | the default choice for first production rollout |
| `throughput` | ingest throughput first | `VT_TRACE_INGEST_PROFILE=throughput` + `VT_STORAGE_SYNC_POLICY=none` | if `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES` is not set explicitly, it defaults to `67108864`, and `VT_STORAGE_TRACE_SEAL_WORKER_COUNT` defaults to `4` | use only after canary / replay / soak confirms the query plane is acceptable for your workload |

Recommended first-production `stable` runtime:

```bash
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=/var/lib/rust-victoria-trace \
VT_TRACE_INGEST_PROFILE=default \
VT_STORAGE_SYNC_POLICY=data \
VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608 \
VT_MAX_REQUEST_BODY_BYTES=8388608 \
VT_API_CONCURRENCY_LIMIT=1024 \
vtapi
```

High-throughput runtime, when you explicitly accept the durability / query tradeoff:

```bash
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=/var/lib/rust-victoria-trace \
VT_TRACE_INGEST_PROFILE=throughput \
VT_STORAGE_SYNC_POLICY=none \
VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=67108864 \
VT_STORAGE_TRACE_SEAL_WORKER_COUNT=4 \
VT_MAX_REQUEST_BODY_BYTES=8388608 \
VT_API_CONCURRENCY_LIMIT=1024 \
vtapi
```

Recommended operational checks:

- scrape `/metrics`
- verify `GET /healthz`
- validate WAL and `.part` files are created under the configured storage path
- for `throughput`, confirm `vt_storage_trace_seal_queue_depth` settles instead of growing without bound
- run at least one OTLP protobuf smoke test before exposing public traffic

## Topology Mapping

If you are migrating from VictoriaTraces, keep the same deployment mental model:

- `storage` persists data on local disks
- `insert` receives external writes and fans them out to the storage set
- `select` serves external queries and cluster governance

Compatibility endpoints for smooth migration:

- writers may keep the VictoriaTraces-style OTLP/HTTP path: `POST /insert/opentelemetry/v1/traces`
- that path accepts standard OTLP JSON camelCase payloads as well as OTLP protobuf
- Grafana Jaeger data sources may point at `http://<select-host>:<port>/select/jaeger`
- Jaeger query compatibility covers services, operations, trace lookup, trace search, and `dependencies`

That means the smoothest production path is usually:

1. keep your existing data volume conventions and mount the new storage path with `VT_STORAGE_PATH`
2. preserve your load-balancer layering as `insert` for writes and `select` for reads
3. map each process role explicitly with `VT_ROLE=storage|insert|select`
4. keep logging on stdout / stderr via `RUST_LOG` and let systemd, Kubernetes, or your log collector own persistence and rotation

## Merge-To-Master Gate

Before merging a formal version to `master`, require all of the following:

1. `cargo test --workspace -- --nocapture` passes
2. `cargo build --release -p vtapi -p vtbench` passes
3. GitHub Actions `linux-release` workflow is green on the branch
4. Linux `x86_64` and `aarch64` artifacts are produced successfully
5. Docker smoke on Linux `x86_64` passes
6. Release artifact checksums are generated successfully
7. Public docs are current:
   - `README.md`
   - `docs/2026-04-06-otlp-ingest-performance-report.md`
   - this guide
8. Benchmark evidence still shows disk above official on the agreed OTLP protobuf ingest shape

For Apple Silicon local benchmark evidence, require native ARM binaries:
- `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- use `target/aarch64-apple-darwin/release/vtapi` and `target/aarch64-apple-darwin/release/vtbench`

## Current Validation Snapshot

The release-prep state for this repository has already passed these local checks:

- `cargo test --workspace -- --nocapture`
- `cargo build --release -p vtapi -p vtbench`
- `docker buildx build --builder desktop-linux --platform linux/amd64 -t rust-victoria-trace:release-check-amd64 --load .`
- `docker buildx build --builder desktop-linux --platform linux/arm64 -t rust-victoria-trace:release-check-arm64 --load .`
- `docker run --rm --platform linux/amd64 -p 13181:13000 rust-victoria-trace:release-check-amd64` + `GET /healthz`
- `docker run --rm --platform linux/arm64 -p 13182:13000 rust-victoria-trace:release-check-arm64` + `GET /healthz`
- branch GitHub Actions `linux-release` workflow succeeded on the release candidate branch

That gives us direct evidence for:

- workspace correctness on the current source tree
- release binaries building successfully
- the provided `Dockerfile` producing runnable Linux images for both common 64-bit targets
- the branch-level Linux release workflow staying green with tests, release builds, Docker smoke, and artifact packaging

## Release Flow

Suggested release flow:

1. Merge the release candidate branch into `master`
2. Create a tag such as `v0.1.0`
3. Let GitHub Actions publish Linux tarballs
4. Attach the public performance report in the release notes

## Public Benchmark Reference

这不是“内部调优过程中的某一次截图”，而是对外可以引用的公开参考数字。

### Fresh Single Run

| target | spans/s | p99 |
| --- | ---: | ---: |
| official | `396475.630` | `0.673 ms` |
| disk | `430192.512` | `0.409 ms` |

### Fresh 5-Round Median

| target | spans/s | p99 |
| --- | ---: | ---: |
| official | `343086.506` | `0.902 ms` |
| disk | `359315.329` | `0.713 ms` |

### Metrics Visibility

After the bounded stats-side live-update drain change, the first `/metrics` scrape after a 5-round disk run dropped from about `30.7s` to about `14ms`.

## 对外如何表述这次发布

更准确的公开说法是：

- 这是一个已经具备正式 Linux 发布链路的 Rust Trace 存储系统
- 当前公开 benchmark 下，disk engine 在 agreed OTLP protobuf ingest 口径上高于 official VictoriaTraces
- 它不是“只会跑 benchmark”的 demo，而是已经具备二进制、容器、文档和 merge gate 的正式发布候选

不建议对外说成：

- “所有性能问题都已经解决”
- “已经在所有 workload 下都全面碾压”
- “读写两条链路已经没有剩余瓶颈”

## Known Remaining Work

This release gate is strong enough for a formal version, but not the end of performance work.

The next major optimization target is still on the write path:

- form larger same-shard append packets cheaply on HTTP ingest

The next major read-path cleanup target is:

- pay down live-update drain cost on query/search paths beyond `stats()`
