# Handoff Template

适用场景：

- 准备切模型
- 准备中断当前 thread
- 准备切到另一个仓库继续
- 怀疑当前 thread 已经过长或跑偏

## 已完成

- 已把 disk ingest 主线继续打到“最新稳定组 median 超过 official VictoriaTraces”：
  - latest fresh 5-round stability run：
    - official median: `306,361 spans/s`, `p99 1.048 ms`
    - memory median: `311,230 spans/s`, `p99 0.919 ms`
    - disk median: `334,613 spans/s`, `p99 0.762 ms`
    - `disk > official`：`3/5` 轮
- 已完成并保留这轮有效优化：
  - `DiskStorageEngine.active_segments` 改成按 shard 独立持有，消掉单一 `active_segment` 全局串行。
  - disk ingest 增加 pre-sharded fast path；已经按 shard 的 `TraceBlock` 不再二次 `TraceBlockBuilder` 重建。
  - 修正 batched trace WAL recover/seal 解析，让 reader 和当前 `row_count + length block + payload block` writer 布局一致。
- 当前代码已经重新验证通过：
  - `cargo test -p vtstorage -- --nocapture`
  - `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- checkpoint 已经提交并推到 GitHub：
  - branch: `init`
  - commit: `2fd0eca` `Checkpoint rust victoria trace performance progress`
  - annotated tag: `checkpoint/rust-vt-perf-20260406`
- 用户已经明确接受下一阶段采用“`per-shard worker + batched live merge`”方案，即写入成功后查询可以是毫秒级 near-realtime，而不是必须严格写后立刻可见。

## 未完成

- 还没有开始实现真正的 async `per-shard worker + batched live merge`。
- live index merge 仍然在请求线程里同步做，主要还在：
  - `DiskTraceShardState::observe_live_updates`
  - `merge_trace_services`
  - `merge_trace_operations`
  - `merge_trace_fields`
- 还没有给 near-realtime 可见性补上显式指标：
  - queue depth
  - batch size
  - batch wait time
  - ack-to-visible latency
- 还没有验证 async shard worker 方案下的稳定组 benchmark。

## 当前阻塞

- 没有功能性阻塞，当前是纯性能与架构取舍问题。
- 这台机器 benchmark 有噪声；单次 best run 不能当最终结论，必须回到同机同口径 5 轮稳定组。
- 如果下一线程为了对比短期保留同步旧路径，可以接受一个很窄的实验开关；但不要长期双维护两套 ingest 语义。

## 关键结论

- 现在这份代码已经达到“disk 稳定超过 official”的阶段，但要继续明显拉开差距，最可能出现台阶式提升的方向不是继续抠零碎 micro-opt，而是：
  - `per-shard worker + batched live merge`
- 当前仍然最重的同步热区在：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
    - `append_prepared_trace_blocks`
    - `append_block_rows`
    - `observe_live_updates`
- 还有一个结构性差距：
  - memory/state 那边很多 merge 已接近 set 语义
  - disk live shard 这边仍有较多 `Vec::contains` / pair 线性扫描
- 预计 async shard worker 下的“准实时”可见性目标：
  - 轻载通常 `0.1 ms - 1 ms`
  - 正常压测负载通常 `1 ms - 5 ms`
  - 热点 shard 积压时可能 `5 ms - 20+ ms`
- 下一线程不要回头重谈大方向，也不要把时间主要花在纯语法级 Rust micro-opt 上。

## 已改动文件

- 这轮主改：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
- 当前稳定主线还涉及：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/batching.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/memory.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/state.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/src/proto.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-05-rust-vt-performance-thread2.md`
- 当前工作树状态：
  - git 已 clean，只有 `var/` 仍是未跟踪本地目录，不要把它带进 commit。

## 已执行验证

- 命令：
  - `cargo test -p vtstorage -- --nocapture`
  - `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
  - fresh single run:
    - `vtbench otlp-protobuf-load --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
  - fresh 5-round stability run:
    - 同一 benchmark 参数
    - official: `http://127.0.0.1:13081/insert/opentelemetry/v1/traces`
    - memory: `http://127.0.0.1:13082/v1/traces`
    - disk: `http://127.0.0.1:13083/v1/traces`
- 结果：
  - crate tests 通过
  - release build 通过
  - latest fresh single run：
    - official: `286,730 spans/s`, `p99 1.068 ms`
    - memory: `293,310 spans/s`, `p99 0.924 ms`
    - disk: `316,046 spans/s`, `p99 0.866 ms`
  - latest fresh 5-round stability run：
    - official median: `306,361 spans/s`, `p99 1.048 ms`
    - memory median: `311,230 spans/s`, `p99 0.919 ms`
    - disk median: `334,613 spans/s`, `p99 0.762 ms`
    - `disk > official`: `3/5`
  - rounds 2-5 only（剔除 round1 官方异常低点）参考值：
    - official median: `318,409 spans/s`, `p99 0.998 ms`
    - disk median: `333,735 spans/s`, `p99 0.797 ms`

## 相关文件与证据

- 代码：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/src/proto.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/state.rs`
- 文档：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-05-rust-vt-performance-thread2.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-06-rust-vt-performance-thread3.md`
- 日志：
  - `/tmp/rust-vt-stability-20260406-round4.jsonl`
  - `/tmp/rust-vt-stability-20260406-round3.jsonl`
- SQL / 接口：
  - official ingest: `POST /insert/opentelemetry/v1/traces`
  - our ingest: `POST /v1/traces`
  - gRPC ingest path 也已支持 sharded protobuf decode，但还未做 async shard worker。
- Git / checkpoint：
  - repo: `git@github.com:chaisencs/vt_traces.git`
  - branch: `init`
  - commit: `2fd0eca`
  - tag: `checkpoint/rust-vt-perf-20260406`

## 下一步建议

1. 直接实现 `per-shard worker + batched live merge`，让请求线程只负责 decode/prepare/enqueue，worker 负责 append + live merge。
2. 补 near-realtime 可见性的指标与观测：
   - queue depth
   - batch size
   - batch wait
   - ack-to-visible latency
3. 每完成一轮有效实现，都继续用同机同口径 `vtbench otlp-protobuf-load` 跑 official / memory / disk 三组；如果单轮看起来有台阶，再跑 fresh 5 轮稳定组。

## 下一线程启动词

```text
基于 /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-06-rust-vt-performance-thread3.md 继续。
请先读取：
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-06-rust-vt-performance-thread3.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md

这轮只做：
- 实现 disk ingest 的 `per-shard worker + batched live merge`
- 请求线程尽量只做 decode / prepare / enqueue，worker 做 append + live merge
- 给 near-realtime 可见性补指标：queue depth、batch size、batch wait、ack-to-visible latency
- 每完成一轮有效优化，都继续用同机同口径 `vtbench otlp-protobuf-load` 对 official VictoriaTraces、memory、disk 三组重测

不要做：
- 不要回头为了兼容性长期保留旧同步路径
- 不要重新讨论大方向
- 不要换 benchmark 口径
- 不要重新引入这轮已经证伪并撤回的 `SegmentAccumulator` batch-local map merge 方案
- 不要把时间主要花在零碎 Rust micro-opt 上

完成后给我：
- 最新 official vs memory vs disk 的 spans/s 和 p99
- 新增 async shard worker / batch merge 的代码位置
- 实测的 ack-to-visible latency 量级
- 还没打穿的下一层瓶颈
```
