# Rust Victoria Trace Performance Report

Last updated: `2026-04-06`  
Reference commit: `0019f74` (`Bound stats-side live update drain`)

## Headline

在同机、同口径、clean start 的 `vtbench otlp-protobuf-load` 基准下，`rust_victoria_trace` 的磁盘引擎已经稳定超过 official VictoriaTraces，同时保持更低的 `p99`。

这不是一次 warm-process 峰值截图，而是同时满足下面两条：

- fresh single run 高于 official
- fresh 5-round median 也高于 official

## Benchmark Shape

所有结果都来自同一台机器、同一组 benchmark 参数：

```bash
target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load \
  --duration-secs=5 \
  --warmup-secs=1 \
  --concurrency=32 \
  --spans-per-request=5 \
  --payload-variants=1024
```

对比对象有三组：

- official VictoriaTraces
- Rust memory engine
- Rust disk engine

我们特别看两类结果：

- fresh single run
- fresh clean-start 5-round median

原因很简单：official 在这台机器上本身就有明显漂移，所以只看单次峰值没有意义。

## Latest Results

### Fresh Single Run

| target | spans/s | p99 |
| --- | ---: | ---: |
| official | `396475.630` | `0.673 ms` |
| memory | `345892.289` | `0.629 ms` |
| disk | `430192.512` | `0.409 ms` |

相对 official：

- disk 吞吐提升约 `8.5%`
- disk `p99` 降低约 `39.2%`

### Fresh 5-Round Median

| target | spans/s | p99 |
| --- | ---: | ---: |
| official | `343086.506` | `0.902 ms` |
| memory | `135212.607` | `2.698 ms` |
| disk | `359315.329` | `0.713 ms` |

相对 official：

- disk 吞吐提升约 `4.7%`
- disk `p99` 降低约 `20.9%`

## Why This Result Matters

这组数据说明的不是“某个实验分支偶然跑出一次好成绩”，而是下面三件更重要的事：

1. Rust disk path 已经在当前 OTLP protobuf ingest 口径上站到 official 之上。
2. 赢的不只是吞吐，尾延迟也一起更好。
3. 我们没有靠一个脱离真实产品语义的 fake fast path 去换分数。

当前主线保留的关键方向有两条：

- disk-local same-shard combiner：尽量把小 trace block 更便宜地暴露给 disk append kernel
- bounded stats-side live-update drain：避免 `/metrics` 首次读取吞掉整笔 near-realtime backlog

## Recent Operational Win

除了 ingest 分数，这轮还顺手打掉了一个非常影响产品体验的读侧问题。

在之前版本里，disk 跑完 5 轮压测之后，第一次 `/metrics` scrape 大约要 `30.7s`。  
在 `0019f74` 这版里，这个首 scrape 已经降到约 `14ms`。

也就是说：

- ingest 没有掉回去
- metrics 可见性不再卡几十秒

这对真实部署很重要，因为 open-source 用户不会只看写入 benchmark，他们也会马上抓 `/metrics`、看 health、做 query。

## How To Reproduce

下面是最接近我们这份报告口径的复现方式。

### 1. Build

```bash
cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench
```

### 2. Start official

```bash
/path/to/victoria-traces-prod \
  -loggerLevel=ERROR \
  -httpListenAddr=127.0.0.1:13081 \
  -storageDataPath=/tmp/victoria-official-bench
```

### 3. Start Rust memory engine

```bash
RUST_LOG=error \
VT_BIND_ADDR=127.0.0.1:13082 \
VT_STORAGE_MODE=memory \
target/aarch64-apple-darwin/release/vtapi
```

### 4. Start Rust disk engine

```bash
RUST_LOG=error \
VT_BIND_ADDR=127.0.0.1:13083 \
VT_STORAGE_MODE=disk \
VT_STORAGE_PATH=./var/bench-disk \
target/aarch64-apple-darwin/release/vtapi
```

### 5. Run the benchmark

```bash
target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load \
  --url=http://127.0.0.1:13081/insert/opentelemetry/v1/traces \
  --duration-secs=5 \
  --warmup-secs=1 \
  --concurrency=32 \
  --spans-per-request=5 \
  --payload-variants=1024
```

memory 和 disk 只需要把 URL 分别改成：

- `http://127.0.0.1:13082/v1/traces`
- `http://127.0.0.1:13083/v1/traces`

如果你要验证稳定性，不要只跑一次。请从 clean start 开始做 5 轮串行复测。

## What We Rejected

这份报告也明确排除了几条已经测过但不值得继续的路线：

- outer trace microbatch layer
- passthrough `append_trace_blocks(...)` 重新走 outer batching
- pre-prepare raw block fusion
- 之前已经证伪并撤回的 async shard worker 路线

它们的共同问题是：要么控制面开销太重，要么写路径吞吐被拉低。

## Current Limits

这不是在宣称“所有工作都做完了”。

当前还没完全打穿的瓶颈主要有两块：

- 写路径上，HTTP ingress 仍然没有形成足够大的 same-shard append packet
- 读路径上，`stats()` 已经做了 bounded drain，但更广泛的 query/search 首读路径仍有 live-update drain 成本

所以这份报告的正确解读是：

- 当前主线已经足够强，可以公开展示
- 但它还有继续拉开差距的空间

## Bottom Line

如果你关心的是：

- OTLP protobuf ingest 吞吐
- 磁盘引擎的 tail latency
- near-realtime metrics visibility

那么当前这条 Rust disk 主线已经值得拿来跑一遍自己的 workload。  
在我们当前的同机同口径测试里，它已经不是“接近 official”，而是“稳定高于 official”。
