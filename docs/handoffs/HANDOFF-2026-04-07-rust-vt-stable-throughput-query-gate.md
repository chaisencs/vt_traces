# Handoff Template

适用场景：

- 当前 thread 已经很长，准备切线程继续
- 这轮已经把 ingest 吞吐主线推到明显高于 official，但 production 前还缺查询 gate

## 已完成

- 把 trace ingest 明确拆成两档 profile：
  - `default`：保留 blocking boundary / runtime responsiveness 语义
  - `throughput`：direct append，吞吐优先
- `throughput + sync_policy=none` 下已启用 deferred WAL staging：
  - request ack path 不再做 request-path WAL write
  - seal / drop 时再把 staged WAL 落盘并 seal
- 为 `throughput + sync_policy=none` 增加了自动 disk override：
  - 若用户未显式设置 `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES`，默认提升到 `256MiB`
  - 目的是压掉 head rollover / seal backlog 噪声
- 跑完 fresh clean benchmark gate，当前最强结果：
  - throughput single: `655130.821 spans/s`, `p99 0.869 ms`, `errors=0`
  - throughput 5-round median: `671273.875 spans/s`, `p99 0.841 ms`, `errors=0`
- 这组结果相对 official：
  - single 吞吐 `+65.2%`
  - 5-round median 吞吐 `+95.7%`
  - 5-round median `p99` `-6.8%`
- 查明 single/median tail 噪声的大头之一确实是 seal backlog：
  - 旧 probe：`head_segments=106`, `seal_queue_depth=96`
  - 新 probe：`head_segments=10`, `seal_queue_depth=0`

## 未完成

- 还没有给 production 下结论
- 还没有把“真正稳定版”跑成一组新的正式 gate
  - 我当前建议的稳定版定义是 `default + data`
  - 但这轮主要 benchmark 的是 `throughput + none`
- 还没有系统评估查询面：
  - `trace-by-id`
  - `search`
  - `services`
  - `field-values`
- 还没有做 mixed read/write query gate
- 还没有做 production canary / soak

## 当前阻塞

- 没有代码级硬阻塞
- 真正阻塞 production 决策的是两个未完成项：
  - 查询性能 / 查询可见性还没正式 benchmark
  - 是否接受 `throughput + none` 的 crash-loss tradeoff 还没做发布决策

## 关键结论

- 现在已经不该再把 ingest 继续往前乱推；先把两档 profile 稳住
- `throughput` 不是“默认稳定版”，而是“高吞吐发布版”
- 我当前对两档的定义：
  - 稳定版：`VT_TRACE_INGEST_PROFILE=default` + `VT_STORAGE_SYNC_POLICY=data`
  - 高性能版：`VT_TRACE_INGEST_PROFILE=throughput` + `VT_STORAGE_SYNC_POLICY=none` + `VT_STORAGE_TARGET_SEGMENT_SIZE_BYTES=268435456`
- `official` 更像偏高性能的真实产品路径，不像我这里定义的“真正稳定版”
- `trace-by-id` 明确会联读 `sealed parts + active head`
- `search/services/field-values` 不完全是同一套读模型，production 前必须单独测

## 已改动文件

- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/main.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/engine.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/disk_engine_tests.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`

## 已执行验证

- 命令：`cargo fmt --all`
- 结果：通过
- 命令：`cargo test -p vtstorage`
- 结果：通过
- 命令：`cargo test -p vtapi`
- 结果：通过
- 命令：`cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
- 结果：通过

## 相关文件与证据

- 代码：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/main.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- 文档：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/2026-04-06-otlp-ingest-performance-report.md`
- 日志 / benchmark：
  - `/tmp/vt_trace_profile_gate_20260407_174936/default/single/report.json`
  - `/tmp/vt_trace_profile_gate_20260407_174936/default/median.json`
  - `/tmp/vt_throughput_deferred_wal_20260407_180315/single/report.json`
  - `/tmp/vt_throughput_deferred_wal_20260407_180315/median.json`
  - `/tmp/vt_throughput_profile_autotuned_20260407_181147/single/report.json`
  - `/tmp/vt_throughput_profile_autotuned_20260407_181147/median.json`
  - `/tmp/vt_throughput_metrics_probe_20260407_180625/metrics.txt`
  - `/tmp/vt_throughput_autotuned_probe_20260407_181552/metrics.txt`
- 关键接口 / 读路径位置：
  - `trace-by-id rows_for_trace`: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - storage query trait: `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/engine.rs`

## 下一步建议

1. 先把两档 profile 固化成明确的发布矩阵，不再继续改 ingest 主线。
2. 只补查询 gate：
   - `trace-by-id`
   - `search`
   - `services`
   - `field-values`
   - mixed read/write
3. 查询 gate 通过后，再决定：
   - `default + data` 是否作为真正稳定版
   - `throughput + none + 256MiB segment` 是否作为高吞吐发布版

## 下一线程启动词

```text
基于 /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-07-rust-vt-stable-throughput-query-gate.md 继续。
请先读取：
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/2026-04-06-otlp-ingest-performance-report.md
- /tmp/vt_throughput_profile_autotuned_20260407_181147/median.json
- /tmp/vt_throughput_autotuned_probe_20260407_181552/metrics.txt

这轮只做：
- 固化 stable vs throughput 两档发布定义
- 跑并评估 query gate：trace-by-id、search、services、field-values、mixed read/write
- 给出 production 使用建议

不要做：
- 不要继续改 ingest 热路径
- 不要改 benchmark 口径
- 不要把默认版和高吞吐版混成同一发布语义

完成后给我：
- stable 和 throughput 的明确发布矩阵
- 查询性能和 near-realtime 可见性结论
- 是否可以直接上生产，以及推荐先上哪一档
```
