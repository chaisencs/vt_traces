# 2026-04-09 Official Query Benchmark Harness

## 目标

为 `vtbench` 增加一条可复现的查询基准入口，用同机、同负载形状、同一组 Jaeger trace search 请求，对比 official VictoriaTraces 与当前 Rust disk 模式的真实查询表现。

这条 harness 不是内存热数据 benchmark，而是显式测“持久化后再重启”的查询链路。

## 范围

当前版本故意保持收敛：

- 查询接口：`GET /select/jaeger/api/traces`
- 对比对象：`official`、`disk`
- fixture 写入接口：`POST /insert/opentelemetry/v1/traces`
- 执行模型：
  - clean start
  - 灌固定 fixture
  - `official` 执行一次 `/internal/force_flush`
  - 停进程
  - 用同一数据目录重启
  - 等待 ready
  - 再执行 query-heavy benchmark

默认 workload 形状：

- `fixture-requests=20000`
- `duration-secs=5`
- `warmup-secs=1`
- `concurrency=16`
- `spans-per-request=5`
- `payload-variants=1024`

## 查询形状

harness 内置 4 类 Jaeger trace search case，并 round-robin 打过去：

- `checkout + GET /checkout + tags(http.method=GET, benchmark.case=checkout-get)`
- `checkout + POST /checkout + tags(http.method=POST, benchmark.case=checkout-post)`
- `payments + POST /pay + tags(http.method=POST, benchmark.case=payments-post)`
- `inventory + GET /stock + tags(http.method=GET, benchmark.case=inventory-get)`

每个请求固定 `limit=20`。

fixture 时间戳不是 1970 附近的合成值，而是“当前时间向前回退 5 分钟”的真实时间轴，避免 official 因 retention / latency offset 直接返回非法查询。

## CLI

```bash
target/aarch64-apple-darwin/release/vtbench official-query-compare \
  --official-bin=/tmp/victoria-traces-v0.8.0-arm64/victoria-traces-prod \
  --rust-bin=/abs/path/to/vtapi \
  --targets=official,disk \
  --rounds=3 \
  --official-port=13181 \
  --disk-port=13183 \
  --fixture-requests=2000 \
  --duration-secs=3 \
  --warmup-secs=1 \
  --concurrency=16 \
  --spans-per-request=5 \
  --payload-variants=256 \
  --output-dir=/tmp/rust-vt-official-query-bench-20260409
```

和 ingest harness 一样，`--url` 与 `--report-file` 由 harness 自己管理。

## 输出

输出目录结构：

- `manifest.json`
- `round-XX/<target>.json`
- `round-XX/<target>.stdout.log`
- `round-XX/<target>.stderr.log`
- `summaries/<target>-median.json`
- `summary.json`

review 时优先看：

- `summary.json`
- `summaries/official-median.json`
- `summaries/disk-median.json`

## 当前结果

本次正式 3 轮结果目录：

- `/tmp/rust-vt-official-query-bench-20260409`

median 结果：

- official：`28,022.114 qps`，`p99 2.162 ms`
- Rust disk：`167,437.718 qps`，`p99 0.275 ms`
- throughput ratio：`5.9752`
- p99 ratio：`0.1271`

## 说明

这条基准测到的是“公共 Jaeger trace search 产品链路”的真实表现，不是内部 `search_traces()` 原语的微基准。

因此它覆盖了：

- 持久化后重启恢复
- sealed-side prune
- trace materialization
- HTTP JSON 输出

如果后续要分析“为何赢”或“为何退化”，应继续叠加两类 benchmark：

- Rust 内部 search primitive benchmark
- 更大 fixture / 更复杂 filter / 更高并发的 query-heavy 压测
