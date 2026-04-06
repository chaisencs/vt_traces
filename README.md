# Rust Victoria Trace

> AI-Agent时代的Trace存储系统
>
> 面向 Agent 工作流、工具调用链和高频小请求 OTLP ingest 的自托管 Trace 后端。
> 标准 OpenTelemetry 接入，磁盘优先存储，兼容 Jaeger / Tempo 查询接口，可直接运行在常见 64 位 Linux 上。

`rust_victoria_trace` 不只是对 VictoriaTraces 核心能力的一次 Rust 重写。它瞄准的是 AI-Agent 时代更常见的观测形态：大量短 trace、频繁 fan-out / fan-in、工具调用链、near-realtime 调试，以及对标准协议和自托管部署路径的更高要求。

如果你需要的是一套能够：

- 直接接 OTLP/HTTP 和 OTLP/gRPC
- 在磁盘引擎上保持不错的 ingest 吞吐和更低的 tail latency
- 跑在常见 `x86_64` / `aarch64` Linux 上
- 同时兼容 Jaeger / Tempo 查询生态

的 Trace 存储系统，这个项目就是为这类场景准备的。

## 快速导航

- 性能报告：[2026-04-06 OTLP Ingest Performance Report](./docs/2026-04-06-otlp-ingest-performance-report.md)
- 正式发布说明：[Production Release Guide](./docs/production-release-guide.md)
- Harness QA 方案：[2026-04-06 Harness QA Plan](./docs/plans/2026-04-06-harness-qa-plan.md)
- Benchmark QA 目录：[docs/benchmarks](./docs/benchmarks/README.md)
- Harness QA workflow：`.github/workflows/harness-qa.yml`
- Linux release workflow：`.github/workflows/linux-release.yml`
- 容器入口：[Dockerfile](./Dockerfile)

## 为什么它适合 AI-Agent 时代

| 典型问题 | AI-Agent 系统里的表现 | 本项目提供的能力 |
| --- | --- | --- |
| 高频小请求写入 | 一个 agent run 会拆成很多 tool spans、workflow spans、RPC spans | 磁盘引擎针对小请求 OTLP ingest 做了持续优化 |
| 需要近实时可见性 | 调试 agent loop 时，trace 晚几秒可见都很难受 | `/metrics` 可见性链路已做专项优化 |
| 生态必须标准化 | 你通常已经在用 OpenTelemetry、Jaeger、Tempo | 原生 OTLP 接入，兼容 Jaeger / Tempo 查询接口 |
| 部署要可控 | 生产常常需要自托管、Linux、容器和二进制交付 | Linux tarball、Dockerfile、GitHub Actions release workflow 都已就位 |

## 当前公开性能

下面这组公开结果来自同机、同口径、clean start 的 `vtbench otlp-protobuf-load`。
这个 benchmark 形状很贴近 AI-Agent 常见的高频小请求写入：

```bash
target/release/vtbench otlp-protobuf-load \
  --duration-secs=5 \
  --warmup-secs=1 \
  --concurrency=32 \
  --spans-per-request=5 \
  --payload-variants=1024
```

对外完整说明见 [2026-04-06 OTLP Ingest Performance Report](./docs/2026-04-06-otlp-ingest-performance-report.md)。

| target | fresh single spans/s | fresh single p99 | fresh 5-round median spans/s | fresh 5-round median p99 |
| --- | ---: | ---: | ---: | ---: |
| official VictoriaTraces | `396475.630` | `0.673 ms` | `343086.506` | `0.902 ms` |
| Rust disk engine | `430192.512` | `0.409 ms` | `359315.329` | `0.713 ms` |

当前公开基准里，disk engine：

- fresh single 吞吐高于 official，约 `+8.5%`
- fresh 5-round median 也高于 official，约 `+4.7%`
- `p99` 在 single run 和 5-round median 上都更低
- 5 轮压测后的首次 `/metrics` scrape 已从约 `30.7s` 降到约 `14ms`

## Harness QA 与可复现 benchmark

这个项目现在不只是在仓库里放了一些 benchmark 命令，还开始把 benchmark 当成正式 QA 资产管理。

当前已经具备的 QA 底盘包括：

- `vtbench` canonical run bundle
- replayable `rerun.sh`
- benchmark guardrails
- report comparator
- benchmark scenario catalog / noise model / reference solutions
- dedicated `.github/workflows/harness-qa.yml`

这意味着后续无论是性能优化线程、发布线程还是稳定性回归，都可以用同一套证据模型来验证：

- 这次结果是不是同口径
- 这次回退是不是真的代码回退
- 这条 public benchmark 结论能不能复现
- 这个 incident 有没有沉淀成长期回归资产

## 部署方式

| 方式 | 适合场景 | 入口 |
| --- | --- | --- |
| 预构建 Linux tarball | 裸机、VM、生产发布 | GitHub Releases |
| Docker | 容器化部署、PoC、Kubernetes 镜像封装 | [Dockerfile](./Dockerfile) |
| 从源码构建 | 本地开发、定制编译、基准测试 | `cargo build --release -p vtapi -p vtbench` |

正式 release 目标平台是常见 64 位 Linux：

- `x86_64-unknown-linux-gnu`
- `aarch64-unknown-linux-gnu`

## 使用预构建 Linux Release

打 tag 之后，GitHub Releases 会发布这四个 Linux 交付物：

- `rust-victoria-trace-linux-x86_64.tar.gz`
- `rust-victoria-trace-linux-x86_64.tar.gz.sha256`
- `rust-victoria-trace-linux-aarch64.tar.gz`
- `rust-victoria-trace-linux-aarch64.tar.gz.sha256`

`x86_64` 节点的最小启动方式：

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

启动后先做两步检查：

- `curl http://127.0.0.1:13000/healthz`
- `curl http://127.0.0.1:13000/metrics | head`

## 运行容器镜像

仓库根目录的 `Dockerfile` 会构建一个 production-biased 的 disk-engine 镜像。当前默认值包括：

- `VT_STORAGE_MODE=disk`
- `VT_STORAGE_SYNC_POLICY=data`
- `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608`
- `VT_MAX_REQUEST_BODY_BYTES=8388608`
- `VT_API_CONCURRENCY_LIMIT=1024`

容器以 non-root 用户运行，并自带 `HEALTHCHECK`。

```bash
docker build -t rust-victoria-trace:local .
docker run --rm \
  -p 13000:13000 \
  -v "$(pwd)/var/vt-docker:/var/lib/rust-victoria-trace" \
  rust-victoria-trace:local
```

容器起来之后建议立即检查：

- `curl http://127.0.0.1:13000/healthz`
- `curl http://127.0.0.1:13000/metrics | head`
- 确认挂载目录下出现 WAL 和 `.part` 文件

## 从源码构建

```bash
cargo build --release -p vtapi -p vtbench
```

启动一个磁盘模式单机节点：

```bash
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=./var/vt-single \
VT_STORAGE_SYNC_POLICY=data \
VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608 \
VT_MAX_REQUEST_BODY_BYTES=8388608 \
VT_API_CONCURRENCY_LIMIT=1024 \
target/release/vtapi
```

## 5 分钟接入和使用

下面是一条最短可验证路径：启动单机节点、写入一个 OTLP/HTTP JSON trace、再把它查出来。

先确认服务已经起来：

```bash
curl http://127.0.0.1:13000/healthz
```

写入一个示例 trace：

```bash
curl -X POST http://127.0.0.1:13000/v1/traces \
  -H 'content-type: application/json' \
  -d '{
    "resource_spans": [
      {
        "resource_attributes": [
          { "key": "service.name", "value": { "kind": "string", "value": "agent-runtime" } }
        ],
        "scope_spans": [
          {
            "scope_name": "demo.agent",
            "scope_version": "1.0.0",
            "scope_attributes": [],
            "spans": [
              {
                "trace_id": "trace-agent-demo-1",
                "span_id": "span-agent-demo-1",
                "parent_span_id": null,
                "name": "tool_call.search_docs",
                "start_time_unix_nano": 100,
                "end_time_unix_nano": 180,
                "attributes": [
                  { "key": "agent.name", "value": { "kind": "string", "value": "planner" } },
                  { "key": "tool.name", "value": { "kind": "string", "value": "search_docs" } }
                ],
                "status": { "code": 0, "message": "OK" }
              }
            ]
          }
        ]
      }
    ]
  }'
```

按 trace id 查询完整 trace：

```bash
curl http://127.0.0.1:13000/api/traces/trace-agent-demo-1
```

按 service 和时间窗口搜索：

```bash
curl "http://127.0.0.1:13000/api/v1/traces/search?start_unix_nano=50&end_unix_nano=200&service_name=agent-runtime&limit=10"
```

列出当前 services：

```bash
curl http://127.0.0.1:13000/api/v1/services
```

如果你已经在用 OpenTelemetry SDK 或 Collector，也可以直接把 trace export 指向：

- OTLP/HTTP traces：`POST /v1/traces`
- OTLP/gRPC traces：`POST /opentelemetry.proto.collector.trace.v1.TraceService/Export`

## 正式发布与生产可用性

当前仓库已经具备正式 release 所需的基础发布链路：

- GitHub Actions workflow：`.github/workflows/linux-release.yml`
- Harness QA workflow：`.github/workflows/harness-qa.yml`
- Linux `x86_64` / `aarch64` tarball 产物
- `.sha256` 校验文件
- non-root Docker runtime + `HEALTHCHECK`
- [Production Release Guide](./docs/production-release-guide.md) 中定义的 merge gate
- [docs/benchmarks](./docs/benchmarks/README.md) 中定义的 benchmark QA 资产和 incident-to-eval 流程

如果你准备把这版 merge 到 `master` 或发 `v*` tag，先按 [Production Release Guide](./docs/production-release-guide.md) 的 gate 逐项检查。

## 能力全景

### 接入与查询

- 标准 OTLP/HTTP traces：`POST /v1/traces`
- 标准 OTLP/HTTP logs：`POST /v1/logs`
- 标准 OTLP/gRPC unary traces：`POST /opentelemetry.proto.collector.trace.v1.TraceService/Export`
- 标准 OTLP/gRPC unary logs：`POST /opentelemetry.proto.collector.logs.v1.LogsService/Export`
- `application/json` 和 `application/x-protobuf` 两种 OTLP/HTTP 负载
- trace 查询、service 列表、trace 搜索、logs 搜索
- Jaeger 兼容 services / operations / traces
- Tempo 兼容 trace / search / tags / tag values

### 存储与可见性

- memory engine 和 disk engine
- span / log record 扁平化为结构化行记录
- 按 trace 维护时间窗口索引
- WAL + sealed `.part` 持久化，并带 sidecar 元数据
- checksum、坏尾部截断恢复和可配置 `fsync` 策略
- segment 轮转、重启恢复和小 `.part` 自动压实
- part 级 selective decode
- operation bloom hint、operation 精确索引、field/value 精确索引
- `/metrics` 首次 scrape 可见性链路已做专项优化

### 集群与一致性

- `insert` / `storage` / `select` 三类角色
- weighted rendezvous 副本放置，支持节点权重和拓扑域感知
- 写入并发 fan-out
- 可配置 write quorum / read quorum
- 快副本优先读、read repair、cluster rebalance
- control peers、leader 选举、peer state anti-entropy、epoch 视图
- journal 视图、journal append、leader 事件/治理事件复制
- membership / control / rebalance 后台轮询
- 故障节点失败退避和临时隔离

### 安全与运维

- public / internal / admin 三类 bearer token 边界
- 服务端 HTTPS / mTLS
- cluster client HTTPS / mTLS
- 证书和私钥文件轮询热加载
- 每个角色都暴露 Prometheus 风格指标
- 自带 `vtbench`，支持持续时长模式、warmup、p50 / p95 / p99 / p999、时间序列报告和故障窗口

## 详细运行与配置参考

### 运行单机模式

```bash
cargo run -p vtapi
```

可选的磁盘模式：

```bash
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=./var/vt-single \
VT_STORAGE_SYNC_POLICY=data \
VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608 \
VT_MAX_REQUEST_BODY_BYTES=8388608 \
VT_API_CONCURRENCY_LIMIT=1024 \
cargo run -p vtapi
```

单机模式默认监听地址：

- `127.0.0.1:13000`

### 运行基础集群

先启动两个 storage 节点：

```bash
VT_ROLE=storage \
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=./var/vt-storage-a \
VT_BIND_ADDR=127.0.0.1:13011 \
cargo run -p vtapi
```

```bash
VT_ROLE=storage \
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=./var/vt-storage-b \
VT_BIND_ADDR=127.0.0.1:13012 \
cargo run -p vtapi
```

再启动 insert：

```bash
VT_ROLE=insert \
VT_CLUSTER_STORAGE_NODES=http://127.0.0.1:13011,http://127.0.0.1:13012 \
VT_CLUSTER_REPLICATION_FACTOR=2 \
VT_CLUSTER_WRITE_QUORUM=2 \
VT_INTERNAL_BEARER_TOKEN=change-me-internal \
VT_API_BEARER_TOKEN=change-me-public \
VT_MAX_REQUEST_BODY_BYTES=8388608 \
VT_API_CONCURRENCY_LIMIT=1024 \
VT_CLUSTER_FAILURE_BACKOFF_MS=30000 \
VT_BIND_ADDR=127.0.0.1:13001 \
cargo run -p vtapi
```

最后启动 select：

```bash
VT_ROLE=select \
VT_CLUSTER_STORAGE_NODES=http://127.0.0.1:13011,http://127.0.0.1:13012 \
VT_CLUSTER_CONTROL_NODES=http://127.0.0.1:13002,http://127.0.0.1:13022 \
VT_CLUSTER_REPLICATION_FACTOR=2 \
VT_CLUSTER_WRITE_QUORUM=2 \
VT_CLUSTER_READ_QUORUM=1 \
VT_CLUSTER_STORAGE_TOPOLOGY=http://127.0.0.1:13011=az-a,http://127.0.0.1:13012=az-b \
VT_CLUSTER_STORAGE_WEIGHTS=http://127.0.0.1:13011=100,http://127.0.0.1:13012=100 \
VT_CLUSTER_CONTROL_REFRESH_INTERVAL_SECS=5 \
VT_CLUSTER_MEMBERSHIP_REFRESH_INTERVAL_SECS=15 \
VT_CLUSTER_REBALANCE_INTERVAL_SECS=30 \
VT_INTERNAL_BEARER_TOKEN=change-me-internal \
VT_API_BEARER_TOKEN=change-me-public \
VT_ADMIN_BEARER_TOKEN=change-me-admin \
VT_MAX_REQUEST_BODY_BYTES=8388608 \
VT_API_CONCURRENCY_LIMIT=1024 \
VT_CLUSTER_FAILURE_BACKOFF_MS=30000 \
VT_BIND_ADDR=127.0.0.1:13002 \
cargo run -p vtapi
```

### 启用 TLS / mTLS

服务端 TLS：

```bash
VT_TLS_CERT_PATH=./certs/server.crt \
VT_TLS_KEY_PATH=./certs/server.key \
VT_TLS_RELOAD_INTERVAL_SECS=30 \
cargo run -p vtapi
```

服务端 mTLS：

```bash
VT_TLS_CERT_PATH=./certs/server.crt \
VT_TLS_KEY_PATH=./certs/server.key \
VT_TLS_CLIENT_CA_CERT_PATH=./certs/clients-ca.crt \
VT_TLS_RELOAD_INTERVAL_SECS=30 \
cargo run -p vtapi
```

cluster 内部 HTTPS / mTLS client：

```bash
VT_CLUSTER_TLS_CA_CERT_PATH=./certs/storage-ca.crt \
VT_CLUSTER_TLS_CLIENT_CERT_PATH=./certs/client.crt \
VT_CLUSTER_TLS_CLIENT_KEY_PATH=./certs/client.key \
VT_CLUSTER_TLS_RELOAD_INTERVAL_SECS=30 \
cargo run -p vtapi
```

### 常用环境变量

- `VT_STORAGE_SYNC_POLICY`
  - `none` 或 `data`
  - `data` 会在关键写入路径上调用 `sync_data`
- `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES`
  - 活动 segment 轮转阈值
- `VT_MAX_REQUEST_BODY_BYTES`
  - 路由层请求体大小上限
- `VT_API_CONCURRENCY_LIMIT`
  - 路由层 fail-fast 并发上限，超载直接返回 `503`
- `VT_CLUSTER_STORAGE_TOPOLOGY`
  - 格式：`node-a=az-a,node-b=az-b`
  - 用于让副本优先跨拓扑域放置
- `VT_CLUSTER_STORAGE_WEIGHTS`
  - 格式：`node-a=100,node-b=50`
  - 用于加权 rendezvous 放置，让高配节点接收更多 trace 所有权
- `VT_CLUSTER_CONTROL_NODES`
  - 格式：`select-a,select-b`
  - 用于 `select` 之间的控制节点探测和 leader 选举；不设时默认只认本机 control node
- `VT_CLUSTER_CONTROL_REFRESH_INTERVAL_SECS`
  - `select` 角色后台轮询 control-plane leader 视图的周期，默认 `5s`
- `VT_CLUSTER_REBALANCE_INTERVAL_SECS`
  - `select` 角色后台轮询 `rebalance` 的周期，`0` 或不设表示关闭
- `VT_CLUSTER_MEMBERSHIP_REFRESH_INTERVAL_SECS`
  - `select` 角色后台轮询 `members` 控制面的周期，默认 `15s`
- `VT_API_BEARER_TOKEN`
  - 保护外部查询和写入接口
- `VT_INTERNAL_BEARER_TOKEN`
  - 保护 `storage` 内部 RPC，以及 `insert` / `select` 到 `storage` 的内部访问
- `VT_ADMIN_BEARER_TOKEN`
  - 保护 `select` 上的管理接口；不设时会回退到 `VT_INTERNAL_BEARER_TOKEN`
- `VT_TLS_CERT_PATH` / `VT_TLS_KEY_PATH`
  - 为当前角色启用 HTTPS
- `VT_TLS_CLIENT_CA_CERT_PATH`
  - 为当前角色启用 mTLS，要求客户端证书由这个 CA 签发
- `VT_TLS_RELOAD_INTERVAL_SECS`
  - 当证书来自文件路径时，周期性重载服务端证书、私钥和客户端 CA
- `VT_CLUSTER_TLS_CA_CERT_PATH`
  - `insert` / `select` 访问 `storage` 或本机 `select` 管理面时信任的 CA
- `VT_CLUSTER_TLS_CLIENT_CERT_PATH` / `VT_CLUSTER_TLS_CLIENT_KEY_PATH`
  - cluster 内部 HTTPS/mTLS client 使用的客户端证书
- `VT_CLUSTER_TLS_RELOAD_INTERVAL_SECS`
  - 周期性重载 cluster client 的 CA、客户端证书和私钥文件
- `VT_CLUSTER_TLS_INSECURE_SKIP_VERIFY`
  - 仅调试用途；允许跳过服务端证书校验

### 主要接口

单机模式或 select 角色：

- `GET /healthz`
- `GET /metrics`
- `GET /api/v1/services`
- `GET /api/v1/logs/search`
- `GET /api/v1/traces/search`
- `GET /api/v1/traces/:trace_id`
- `POST /v1/logs`
  - 支持 JSON 和 protobuf OTLP/HTTP
- `POST /v1/traces`
  - 支持 JSON 和 protobuf OTLP/HTTP
- `POST /opentelemetry.proto.collector.logs.v1.LogsService/Export`
  - 支持 OTLP/gRPC unary protobuf
- `POST /opentelemetry.proto.collector.trace.v1.TraceService/Export`
  - 支持 OTLP/gRPC unary protobuf
- `GET /api/traces/:trace_id`
- `GET /api/v2/traces/:trace_id`
- `GET /api/search`
- `GET /api/search/tags`
- `GET /api/v2/search/tags`
- `GET /api/search/tag/:tag_name/values`
- `GET /api/v2/search/tag/:tag_name/values`
- `GET /select/jaeger/api/services`
- `GET /select/jaeger/api/services/:service_name/operations`
- `GET /select/jaeger/api/traces`
- `GET /select/jaeger/api/traces/:trace_id`

仅 select 角色：

- `GET /admin/v1/cluster/members`
- `GET /admin/v1/cluster/leader`
- `GET /admin/v1/cluster/state`
- `GET /admin/v1/cluster/journal`
- `POST /admin/v1/cluster/journal/append`
- `POST /admin/v1/cluster/rebalance`

单机模式或 insert 角色：

- `POST /api/v1/logs/ingest`
- `POST /api/v1/traces/ingest`

storage 角色：

- `POST /internal/v1/rows`
- `GET /internal/v1/services`
- `GET /internal/v1/traces/index`
- `GET /internal/v1/traces/search`
- `GET /internal/v1/traces/:trace_id`

### 基准工具

```bash
cargo run -p vtbench -- storage-ingest --rows=100000 --batch-size=1000
```

```bash
cargo run -p vtbench -- storage-query --traces=10000 --spans-per-trace=5 --queries=50000
```

```bash
cargo run -p vtbench -- http-ingest --requests=2000 --spans-per-request=5 --concurrency=32
```

持续时长模式和延迟分位：

```bash
cargo run -p vtbench -- storage-ingest --rows=1000 --batch-size=100 --duration-secs=30
```

带 warmup 和机器可读报告：

```bash
cargo run -p vtbench -- storage-ingest --rows=1000 --batch-size=100 --duration-secs=30 --warmup-secs=5 --report-file=./var/bench-storage-ingest.json
```

带时间序列和故障窗口：

```bash
cargo run -p vtbench -- http-ingest --requests=40 --spans-per-request=2 --concurrency=4 --duration-secs=2 --warmup-secs=1 --sample-interval-secs=1 --fault-after-secs=1 --fault-duration-secs=1 --report-file=./var/bench-http-fault.json
```

输出会包含：

- `ops_per_sec`
- `latency_p50_ms`
- `latency_p95_ms`
- `latency_p99_ms`
- `latency_p999_ms`
- `latency_max_ms`
- `timeline`

### 生产部署建议

- 用至少 `3` 个 `storage` 节点，`insert` / `select` 各自多实例并放在负载均衡后面，再按副本数设置 `write_quorum` / `read_quorum`
- 对外发布前先用 `vtbench`、真实流量回放和故障演练产出吞吐、P99、节点失效恢复和证书轮换报告
- 证书签发、自动轮换和密钥托管建议交给外部 PKI / Secret 平台；本服务负责无重启接管文件更新
- 如果要把单机吞吐推到目标上限，优先调 `VT_API_CONCURRENCY_LIMIT`、segment 大小、磁盘 sync 策略、拓扑放置和副本数
