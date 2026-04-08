# Handoff Template

适用场景：

- 准备切模型
- 准备中断当前 thread
- 准备切到另一个仓库继续
- 怀疑当前 thread 已经过长或跑偏

## 已完成

- 重新对齐了当前正式版主线的真实状态：
  - `main` 已发到 `v0.1.1`
  - 当前公开 benchmark 口径仍是同机同口径 `vtbench otlp-protobuf-load --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
  - latest public numbers:
    - fresh single: official `396475.630 spans/s`, disk `430192.512 spans/s`
    - fresh 5-round median: official `343086.506 spans/s`, disk `359315.329 spans/s`
- 重新 review 了当前 disk 写路径和读路径的真实热点：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
    - `append_trace_blocks`
    - `run_trace_append_combiner`
    - `append_prepared_trace_shard_batch_once`
    - `append_prepared_trace_rows`
    - `ActiveSegment::append_block_rows`
    - `DiskTraceShardState::observe_live_updates`
    - `drain_pending_trace_updates[_with_limit]`
- 重新核对了这轮之前已经证伪的路线，确认这些不要再作为主线重押：
  - outer trace microbatch layer
  - passthrough 再走 outer batching
  - pre-prepare raw block fusion
  - async shard worker + per-request ack pipeline
  - `SegmentAccumulator` batch-local map merge
- 做了“业内成熟方案 -> 当前代码”的映射 review，重点看了 4 套值得借鉴的 ingest/storage 模式：
  - Grafana Tempo：recent traces 保留在 ingesters，query 同时查 ingesters 和 backend storage
  - ClickHouse async inserts：server-side buffer many small inserts, then flush larger parts
  - VictoriaMetrics / VictoriaLogs：recent data buffered in memory, periodically flushed to disk blocks/parts
  - Apache Pinot realtime consuming segment：partition-local consuming segment, seal after thresholds, then upload/store completed segment

## 未完成

- 还没有开始实现下一次“台阶式提升”的新主线。
- 还没有把 disk ingest 改成真正的 `WAL-first + per-shard head segment + background seal`。
- 还没有让 query path 同时读取：
  - in-memory head segments
  - sealed parts
- 还没有把当前 read-path `drain_pending_trace_updates()` 语义彻底替换掉。

## 当前阻塞

- 没有功能性阻塞，当前是结构设计与改造顺序问题。
- 真正的风险不在“能不能继续抠出几个点”，而在“如何在不丢 durability / 可见性语义的前提下，把小 request 从请求路径上挪走”。
- 这是一条比现有 same-shard combiner 更深的改造线，必须按阶段 gate 走，不能一次性全翻。

## 关键结论

- 当前主线已经赢 official，但幅度不大；如果目标是“再提升一大截”，继续优化当前 per-request append kernel 的边角成本，天花板已经很近。
- 关键原因很明确：当前 ack path 仍然同步承担了这些成本：
  - decode / shard split
  - `PreparedTraceBlockAppend::new`
  - active segment append
  - live update construction
  - `sync_policy=data` 下的 flush / `sync_data`
- 当前 combiner 指标也证明了“只靠 cross-request same-shard pickup”不够：
  - 最新 clean 5-round run 后，input/output blocks 只是 `1442528 -> 1416916`
  - 也就是 HTTP path 上真实物理 coalescing 很轻，赢分主要来自 cheaper handoff，不是更大的物理 append unit
- 所以下一次真正有机会拿到大台阶的路线，不该再是“继续想办法把很多小 request 临时拼大”，而该是：
  - **把物理写入单元从 request/block 改成 per-shard mutable head segment**
  - **把 durability 从“最终 part/segment 写入”降到“WAL group commit”**
  - **把最终 columnar/part 构建挪到 background seal**

### 我认为最靠谱的下一主线

#### 1. WAL-first, head-segment-second, sealed-part-last

写入确认路径只做三件事：

1. decode / flatten / shard route
2. append to shard-local WAL buffer
3. append to shard-local mutable head segment, then在 group commit 完成后 ack

不要在 ack path 上继续做：

- request-sized final part construction
- request-sized final live-index merge into global persisted view
- request-sized segment-cut / part-like bookkeeping

#### 2. 每个 trace shard 维护一个 consuming head segment

这个 head segment 是 queryable 的，但它不是最终 part。

建议 head segment 至少包含：

- append-only row arena / encoded row arena
- trace window map
- trace -> row refs
- service / operation / indexed field 的 append-friendly head index
- head-segment-local string intern tables

这样 recent trace 不需要等 seal 才可见，query 直接看 head + sealed parts。

#### 3. background seal 按阈值切块

当 head segment 达到阈值时 seal：

- rows / bytes threshold
- idle threshold
- age threshold

seal 时一次性做：

- larger packed row batch / columnar part build
- persisted metadata build
- immutable read index publish
- WAL truncate / checkpoint

#### 4. 读路径直接 federation，不再靠 read-side drain 补账

查询 trace / search / services / field values 时：

- 先看 relevant head segments
- 再看 sealed parts

不要继续让 query/metrics 通过 `drain_pending_trace_updates()` 临时把写侧 backlog 吸到读侧。

## 为什么我现在认为这条路把握最高

### 与成熟系统的同构点

- Tempo 的 querier 会同时查 ingesters 和 backend storage，所以 recent traces 不需要先落成最终对象存储块：
  - https://grafana.com/docs/tempo/latest/introduction/architecture/
- ClickHouse async inserts 把 many small inserts 先写入 server-side buffer，再 flush 成更大的 parts；它解决的正是“小写入太频繁导致 part 开销过高”：
  - https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse
- VictoriaMetrics 明确写到 recently ingested samples 会先在内存里 buffer 几秒再 flush 到 disk，这样能提升 ingest performance：
  - https://docs.victoriametrics.com/keyconcepts/
- VictoriaLogs 也明确按 data blocks 存储并强调 block size / per-block overhead：
  - https://docs.victoriametrics.com/victorialogs/faq/
- Pinot realtime consuming segment 也是 partition-local mutable segment，完成后再上传/存储 completed segment：
  - https://docs.pinot.apache.org/release-0.12.1/operators/operating-pinot/decoupling-controller-from-the-data-path

### 与当前代码的直接映射

- 你现在已经有：
  - shard-local append combiner
  - shard-local active segment
  - packed row encoding
  - sealed `.part`
  - trace shard state
- 也就是说，真正缺的不是“有没有分 shard / 有没有 WAL / 有没有 part”，而是：
  - **head segment 作为 first-class ingest object**
  - **durability / visibility / final part build 三者解耦**

## 预期收益

我现在愿意认真押注的，不是“再抠 5%”，而是这条线有现实机会拿到：

- ingest throughput: `+20% ~ +50%`
- p99: 大概率会一起改善，尤其在 `sync_policy=data` 场景
- resource efficiency:
  - 更少 fsync 次数
  - 更少 request-sized checksum / batch header / segment bookkeeping
  - 更少 read-side drain 补账
  - 更少 tiny append unit 对 CPU cache / IO path 的浪费

这不是保证值，但这是我现在唯一愿意拿来争取“再提升一大截”的主线。

## 明确不建议继续重押的路线

- 继续围绕 current combiner 抠局部逻辑
- 再次尝试 outer batching
- 再次尝试 async worker ack pipeline
- 再次尝试 raw block fusion
- 把主要时间花在 `Vec::contains` 之类纯局部微优化上

这些都还值得作为辅助手段，但不值得作为“下一次大跃迁”的主线。

## 已改动文件

- 新增 handoff：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-06-rust-vt-head-segment-leap.md`

## 已执行验证

- 命令：
  - 本地文档/代码 review：
    - `sed -n ... docs/2026-04-06-otlp-ingest-performance-report.md`
    - `sed -n ... docs/plans/2026-04-06-disk-vs-official-recovery-plan.md`
    - `sed -n ... progress.md`
    - `sed -n ... findings.md`
    - `rg -n ... crates/vtstorage/src/disk.rs`
  - 外部资料检索：
    - Grafana Tempo official docs
    - ClickHouse official blog/docs
    - VictoriaMetrics / VictoriaLogs official docs
    - Apache Pinot official docs
- 结果：
  - 本地证据确认当前主线已稳定超过 official，但只领先一个中等幅度
  - 当前最大剩余 ceiling 来自 ack path 仍承担最终 append / merge / sync 成本
  - 外部成熟方案高度一致地指向：head/buffer/consuming-segment + background flush/seal

## 相关文件与证据

- 代码：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/engine.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/main.rs`
- 文档：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/2026-04-06-otlp-ingest-performance-report.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-06-disk-vs-official-recovery-plan.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/findings.md`
- 外部资料：
  - Grafana Tempo architecture:
    - https://grafana.com/docs/tempo/latest/introduction/architecture/
  - ClickHouse async inserts:
    - https://clickhouse.com/blog/asynchronous-data-inserts-in-clickhouse
  - VictoriaMetrics key concepts:
    - https://docs.victoriametrics.com/keyconcepts/
  - VictoriaLogs FAQ:
    - https://docs.victoriametrics.com/victorialogs/faq/
  - Apache Pinot realtime / data path:
    - https://docs.pinot.apache.org/release-0.12.1/operators/operating-pinot/decoupling-controller-from-the-data-path

## 下一步建议

1. 新线程第一轮只做设计落地，不直接大改实现：
   - 定义 `HeadTraceShard` / `ConsumingTraceSegment`
   - 定义 WAL group commit 语义
   - 定义 head + sealed read federation
2. 先打最小可证伪版本：
   - trace-by-id head query
   - WAL-durable append ack
   - single-shard head seal to existing part format
3. 只有最小版本在同口径 benchmark 上显著赢 current mainline，才继续扩展 search/services/field-values 路径。

## 下一线程启动词

```text
基于 /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-06-rust-vt-head-segment-leap.md 继续。
请先读取：
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-06-rust-vt-head-segment-leap.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/2026-04-06-otlp-ingest-performance-report.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-06-disk-vs-official-recovery-plan.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs

这轮只做：
- 设计并实现 disk ingest 的 `WAL-first + per-shard consuming head segment` 最小版本
- ack path 只保留 decode / shard route / WAL append / head append / group commit ack
- 让 trace-by-id 查询同时读取 head segment 和 sealed parts
- 给 head segment / group commit / seal path 补指标
- 用同机同口径 `vtbench otlp-protobuf-load` 对 current disk mainline 做 fresh single + fresh 5-round median A/B

不要做：
- 不要重新启用 outer trace batching 主线
- 不要重新走 async shard worker + per-request ack pipeline
- 不要把时间主要花在零碎 Rust micro-opt 上
- 不要一上来把 search/services/field-values 全部重写完
- 不要改变 benchmark 口径

完成后给我：
- 新旧架构对比图或文字说明
- current mainline vs new head-segment path 的 spans/s 和 p99
- fsync / flush / batch / seal 指标
- near-realtime 可见性语义和 crash-recovery 语义
- 是否足够值得继续把 search/services/field-values 迁到 head+sealed federation
```
