# Rust Victoria Trace

这是对 VictoriaTraces 核心能力的一次 Rust 重写。目前已经具备可运行的单机链路、集群角色拆分、Jaeger/Tempo 查询兼容层、logs 写入/查询垂直切片，以及可运行的 OTLP/HTTP、OTLP/gRPC、TLS/mTLS 和成员治理控制面。

## 最新性能报告

- 对外性能报告见：[2026-04-06 OTLP Ingest Performance Report](./docs/2026-04-06-otlp-ingest-performance-report.md)
- 正式版本/生产发布说明见：[Production Release Guide](./docs/production-release-guide.md)
- 在 commit `0019f74` 的同机同口径测试中，disk engine 在 fresh single 和 fresh 5-round median 上都高于 official VictoriaTraces
- fresh single：official `396475.630 spans/s / p99 0.673ms`，disk `430192.512 spans/s / p99 0.409ms`
- fresh 5-round median：official `343086.506 spans/s / p99 0.902ms`，disk `359315.329 spans/s / p99 0.713ms`
- 这版还把 5 轮压测后的首次 `/metrics` scrape 从约 `30.7s` 压到了约 `14ms`

## Linux / Production Release

- 正式 release 目标平台是常见 64 位 Linux：`x86_64-unknown-linux-gnu` 和 `aarch64-unknown-linux-gnu`
- 仓库提供了 GitHub Actions Linux release workflow：`.github/workflows/linux-release.yml`
- 仓库提供了开箱即用的容器构建入口：[Dockerfile](./Dockerfile)
- merge 到 `master` 前，请按 [Production Release Guide](./docs/production-release-guide.md) 的 gate 逐项检查

## 使用预构建 Linux Release

打 tag 之后，GitHub Releases 会发布这四个 Linux 交付物：

- `rust-victoria-trace-linux-x86_64.tar.gz`
- `rust-victoria-trace-linux-x86_64.tar.gz.sha256`
- `rust-victoria-trace-linux-aarch64.tar.gz`
- `rust-victoria-trace-linux-aarch64.tar.gz.sha256`

`x86_64` 节点的最小使用方式：

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

仓库根目录的 `Dockerfile` 会构建一个 production-biased 的 disk-engine 运行镜像，默认启用：

- `VT_STORAGE_MODE=disk`
- `VT_STORAGE_SYNC_POLICY=data`
- `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=8388608`
- `VT_MAX_REQUEST_BODY_BYTES=8388608`
- `VT_API_CONCURRENCY_LIMIT=1024`

本地构建和启动示例：

```bash
docker build -t rust-victoria-trace:local .
docker run --rm \
  -p 13000:13000 \
  -v "$(pwd)/var/vt-docker:/var/lib/rust-victoria-trace" \
  rust-victoria-trace:local
```

容器起来之后同样先检查：

- `curl http://127.0.0.1:13000/healthz`
- 确认挂载目录下出现 WAL 和 `.part` 文件

## 当前能力

- 标准 OTLP/HTTP 写入路径 `POST /v1/traces`
- `POST /v1/traces` 同时支持 `application/json` 和 `application/x-protobuf`
- 标准 OTLP/HTTP logs 写入路径 `POST /v1/logs`
- `POST /v1/logs` 同时支持 `application/json` 和 `application/x-protobuf`
- span 扁平化为结构化行记录
- OTLP log record 扁平化为共享存储内核上的结构化行记录
- 按 trace 维护时间窗口索引
- 内存版和磁盘版本地存储引擎
- 活动 WAL 加 sealed `.part` 文件持久化，并带 sidecar 元数据
- WAL record checksum、坏尾部截断恢复和可配置 `fsync` 策略
- 重启恢复和 segment 轮转后的自动小 part 压实
- sealed `.part` trace 读取支持 part 级 selective decode
- trace 查询、service 列表、trace 搜索
- trace 搜索支持按时间范围、service 名称、operation 名称和通用字段过滤
- logs 搜索支持按时间范围、service 名称、severity 和通用字段过滤
- 每个 trace 维护 operation bloom hint 和精确 operation 索引
- 每个 trace 维护通用字段 bloom hint 和精确 field/value 索引
- Jaeger 兼容的 services、operations、trace 查询和 trace 搜索接口
- Tempo 兼容的 trace 查询、search、tags 和 tag values 接口
- 标准 OTLP/gRPC unary 写入路径 `POST /opentelemetry.proto.collector.trace.v1.TraceService/Export`
- 标准 OTLP/gRPC unary logs 写入路径 `POST /opentelemetry.proto.collector.logs.v1.LogsService/Export`
- `insert` / `storage` / `select` 三类集群角色
- 基于 weighted rendezvous 的稳定副本放置，支持节点权重和拓扑域感知
- 写入时副本并发 fan-out
- 可配置的写 quorum
- 可配置的读 quorum
- trace 读取时可由快副本优先返回，避免被慢副本拖住
- 成功读 quorum 后的 read repair
- 用于修复目标副本缺失的管理型 cluster rebalance
- `select` 管理面可返回 cluster members 视图并主动探测 storage 健康
- `select` 控制面支持 control peers、稳定 leader 选举和 leader-gated rebalance
- `select` 控制面支持 peer state anti-entropy 和 epoch 视图
- `select` 控制面支持 journal 视图、journal append 和 leader 事件/治理事件复制
- `select` 可配置后台 membership refresh 轮询
- `select` 可配置后台 control refresh 轮询
- `select` 可配置后台 rebalance 轮询
- 故障节点的失败退避和临时隔离
- 请求体大小限制和 fail-fast 并发保护
- public / internal / admin 三类 bearer token 边界
- 可选的服务端 HTTPS / mTLS
- 服务端证书文件轮询热加载
- `insert` / `select` 到 `storage` 的 HTTPS/mTLS cluster client
- cluster client 的 CA / client cert / client key 文件轮询热加载
- 每个角色都暴露 Prometheus 风格指标
- 自带 `vtbench` 压测/基准工具，支持持续时长模式、p50/p95/p99/p999 延迟输出、时间序列报告和可注入故障窗口

## 运行单机模式

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

## 运行基础集群

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

## 启用 TLS / mTLS

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

## 常用环境变量

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

## 主要接口

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

## 基准工具

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

## 生产部署建议

- 用至少 `3` 个 `storage` 节点，`insert` / `select` 各自多实例并放在负载均衡后面，再按副本数设置 `write_quorum` / `read_quorum`
- 对外发布前先用 `vtbench`、真实流量回放和故障演练产出吞吐、P99、节点失效恢复和证书轮换报告
- 证书签发、自动轮换和密钥托管建议交给外部 PKI / Secret 平台；本服务负责无重启接管文件更新
- 如果要把单机吞吐推到目标上限，优先调 `VT_API_CONCURRENCY_LIMIT`、segment 大小、磁盘 sync 策略、拓扑放置和副本数
