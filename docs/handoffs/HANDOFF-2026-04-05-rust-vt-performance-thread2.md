# Handoff Template

适用场景：

- 准备切模型
- 准备中断当前 thread
- 准备切到另一个仓库继续
- 怀疑当前 thread 已经过长或跑偏

## 已完成

- 已把 benchmark 运行环境切到本机 native `aarch64-apple-darwin`，不再用 Rosetta `x86_64` Rust 二进制和官方 `arm64` 做不公平对比。
- memory ingest 已保留 shard-aware fast path：对 API 层已经按 shard 解好的 `TraceBlock`，memory storage 不再二次拆分；同 shard block 会在一次锁获取里 ingest。
- 这轮继续打了 disk append 热路径，并保留了一项稳定收益优化：
- `crates/vtstorage/src/disk.rs`
  - `append_trace_block` 现在先对 `TraceBlock` 做一次 row metadata 预处理。
  - 这份预处理结果同时复用于：
    - active segment 锁内的 `SegmentAccumulator::observe_prepared_block_row`
    - append 后锁外的 live update 构建
  - 原先 append 完再对同一 `TraceBlock` 扫第二遍 metadata 的路径已经删掉。
- 新增回归测试已保留并通过：
  - `disk::tests::prepared_block_row_metadata_can_feed_live_updates_without_rescanning_block`

## 未完成

- 还没有稳定超过 VictoriaTraces。
- 当前这轮最终确认组，同机同口径 `vtbench otlp-protobuf-load`：
  - official: `314,001 spans/s`, `p99 1.009 ms`
  - memory: `308,066 spans/s`, `p99 0.852 ms`
  - disk: `270,654 spans/s`, `p99 0.924 ms`
- 同一稳定 build 的本轮中间单跑里，memory 最高打到过：
  - memory: `317,497 spans/s`, `p99 0.806 ms`
  - 但最终确认组未复现，所以不要把它当稳定结论。
- disk 仍然离“明显超过 official”差得更远，当前主要瓶颈已经从 block 二次扫描转移到了：
  - live shard merge 的 string intern + postings merge
  - active segment 锁内逐 row `observe_prepared_block_row` 的 intern/去重

## 当前阻塞

- 没有功能性阻塞，当前仍是纯性能攻坚。
- 这台机器的 benchmark 会有轻微噪声；短时单跑峰值不可信，最终结论必须回到同一轮的顺序单跑确认组。
- 远端 GitHub `https://github.com/chaisencs/vt_traces` 之前 push `init` 分支失败过一次，返回 403；如果下一线程要做远端同步，需要重新处理认证或权限。

## 关键结论

- 现在不要再回头讨论大方向，主线路线仍然是对的：
  - native arm64
  - block-first ingest
  - pre-sharded memory fast path
  - disk append 临界区继续瘦身
- 这轮验证过一个看起来更“结构化”的 disk batch-merge 方案，但 benchmark 明显回退，已经撤回，不要把它当下一步：
  - 尝试把 `SegmentAccumulator` 改成 batch-local `FxHashMap<&str, PreparedTraceBatchUpdate>`
  - 结果 disk 从一轮单跑 `268,869 spans/s` 掉到约 `242,328 spans/s`
  - 该实验已完全撤回，最终代码里没有留下
- 这轮最终保留的只有“预处理一次 block metadata，再复用给 live updates”这一刀。
- 下一线程要继续追性能，但要避免再上“看起来更批量、实际上把锁内热路径做胖”的方案。
- benchmark 口径继续固定为：
  - `vtbench otlp-protobuf-load`
  - `--duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
- 用户要求明确：
  - 不为兼容性保留旧格式或旧路径
  - 不重新讨论大方向
  - 不换 benchmark 口径
  - 目标是最终性能大幅超过 VictoriaTraces

## 已改动文件

- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- 之前已经保留在稳定主线上的相关文件：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/memory.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/batching.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/state.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/src/proto.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/src/model.rs`

## 已执行验证

- 命令：
  - `cargo fmt --all`
  - `cargo test -p vtstorage -- --nocapture`
  - `cargo test --workspace`
  - `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13081/insert/opentelemetry/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13082/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13083/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
- 结果：
  - `cargo fmt --all` 通过
  - `cargo test -p vtstorage -- --nocapture` 通过
  - `cargo test --workspace` 通过
  - `cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench` 通过
  - 最终确认 benchmark：
    - official: `314,001 spans/s`, `p99 1.009 ms`
    - memory: `308,066 spans/s`, `p99 0.852 ms`
    - disk: `270,654 spans/s`, `p99 0.924 ms`
  - 本轮中间值，仅供参考，不作为最终结论：
    - memory: `317,497 spans/s`, `p99 0.806 ms`
    - disk: `268,869 spans/s`, `p99 1.034 ms`

## 相关文件与证据

- 代码：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/memory.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/src/proto.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/src/model.rs`
- 文档：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-05-rust-vt-performance.md`
- 日志 / 进程：
  - 官方对照进程：`/tmp/victoria_official/victoria-traces-prod`
  - 官方监听：`127.0.0.1:13081`
  - 我们 memory：`127.0.0.1:13082`
  - 我们 disk：`127.0.0.1:13083`
- SQL / 接口：
  - benchmark 接口：
    - 官方 `/insert/opentelemetry/v1/traces`
    - 我们 `/v1/traces`

## 下一步建议

1. 继续打 disk live merge，优先改 `observe_live_updates -> merge_trace_services/operations/fields` 这一串，目标是减少 string intern 和 postings merge 的重复工作。
2. 在 `observe_prepared_block_row` 上只做低开销的微优化，不要再上 batch-local map 这类会把锁内热路径做胖的结构。
3. 如果 disk 还没有明显台阶，再回到 OTLP front-end，继续压 `FieldNameCache.prefixed` 和 `span_id/parent_span_id` 尾巴，看 memory 能不能先稳定超过 official。

## 下一线程启动词

```text
基于 /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-05-rust-vt-performance-thread2.md 继续。
请先读取：
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-05-rust-vt-performance-thread2.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md

这轮只做：
- 继续追 ingest 性能，优先优化 disk live merge / postings merge，其次是 active segment 锁内 `observe_prepared_block_row` 的低开销微优化
- 每完成一轮优化，都用同机同口径 `vtbench otlp-protobuf-load` 对官方 VictoriaTraces、memory、disk 三组重测

不要做：
- 不要回头为了兼容性保留旧格式或旧路径
- 不要重新讨论大方向
- 不要换 benchmark 口径
- 不要重新引入这轮已经证伪并撤回的 `SegmentAccumulator` batch-local map merge 方案

完成后给我：
- 最新官方 vs memory vs disk 的 spans/s 和 p99
- 这一轮新增优化点的代码位置
- 还没打穿的下一层瓶颈
```
