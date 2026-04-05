# Handoff Template

适用场景：

- 准备切模型
- 准备中断当前 thread
- 准备切到另一个仓库继续
- 怀疑当前 thread 已经过长或跑偏

## 已完成

- 已把 trace ingest 主路径切到 block-first 方向，当前热路径不再依赖 `Vec<TraceSpanRow>` 作为唯一主合同。
- 已完成 trace fast path 的多 shard batching、shared-field group 存储、protobuf sharded decode、memory/disk ingest state 分片。
- 这轮又直接优化了 3 个热点：
- `vtstorage::state::IndexedState::ingest_block`
  现在会预计算 shared-field group 的索引贡献，行级只扫 row-specific fields；同时引入 block-local `trace_id/string` intern cache。
- `vtingest::proto::FastTraceRowsDecoder::decode_key_value`
  现在对最近一次 field-name / string / integer / double 做快缓存，减少重复 hash 与 intern。
- `vtcore::model::TraceBlockBuilder::push_prevalidated_split_fields`
  现在对最近一次 shared-field group 做快路径命中，减少重复 shared slice 哈希。
- 相关测试已补齐并通过：
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/tests/model_tests.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/batching_engine_tests.rs`
- workspace 全量测试已通过。

## 未完成

- 还没有超过 VictoriaTraces。
- 当前更真实的同机 protobuf 压测下：
- VictoriaTraces 官方单机持久化：`296,947 spans/s`
- 我们 memory：`257,545 spans/s`
- 我们 disk：`172,340 spans/s`
- `disk` 仍明显落后，当前最大剩余差距基本在：
- disk append 临界区仍然偏胖
- live postings / row refs 仍然不够紧凑
- OTLP front-end 仍有 span name / trace_id / span_id 等分配成本

## 当前阻塞

- 没有功能性阻塞，当前是纯性能攻坚。
- 远端 GitHub `https://github.com/chaisencs/vt_traces` 之前 push `init` 分支失败过一次，返回 403；如果下一线程要做远端同步，需要重新处理认证或权限。

## 关键结论

- 当前线程上下文已经很长，继续在这个 thread 里推进会越来越不利于稳定分析和基准口径管理，建议切新 Thread。
- 短 benchmark 峰值不可信，当前应统一使用 `vtbench otlp-protobuf-load` 这套 5s duration / 1s warmup / 32 concurrency / 1024 payload variants 的口径。
- 这轮刚打完的三个热点方向是对的，收益已经出来：
- memory 从上一稳定口径 `162,319 spans/s` 提到 `257,545 spans/s`
- disk 从上一稳定口径 `75,481 spans/s` 提到 `172,340 spans/s`
- 现在不要再回头讨论旧 WAL/旧 API 兼容，用户已经明确说还没上线，不需要前后兼容包袱。

## 已改动文件

- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/src/model.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/src/proto.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/state.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/tests/model_tests.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/tests/batching_engine_tests.rs`

## 已执行验证

- 命令：
- `cargo fmt --all`
- `cargo test --workspace`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13081/insert/opentelemetry/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13082/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/target/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13083/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024`
- 结果：
- `cargo fmt --all` 通过
- `cargo test --workspace` 通过
- 官方：`296,947 spans/s`
- memory：`257,545 spans/s`
- disk：`172,340 spans/s`

## 相关文件与证据

- 代码：
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtcore/src/model.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtingest/src/proto.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/state.rs`
- 文档：
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/task_plan.md`
- 日志：
- 官方对照进程：`/tmp/victoria_official/victoria-traces-prod`
- 当前本机监听端口：
- 官方 `127.0.0.1:13081`
- 我们 memory `127.0.0.1:13082`
- 我们 disk `127.0.0.1:13083`
- SQL / 接口：
- 压测接口：
- 官方 `/insert/opentelemetry/v1/traces`
- 我们 `/v1/traces`

## 下一步建议

1. 继续打 disk append 临界区，目标是把 active segment 锁内的工作再拆薄，优先看 payload copy、checksum、索引排队点。
2. 把 `row_refs / live postings` 继续压成更紧凑的表示，减少 disk live index 的常驻内存和 merge 成本。
3. 继续压 OTLP front-end 的分配热点，尤其是 `trace_id/span_id/name` 和 `Field` 构建路径，重新做同机 benchmark，看 memory 是否先能追平/超过官方。

## 下一线程启动词

```text
基于 /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-05-rust-vt-performance.md 继续。
请先读取：
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-05-rust-vt-performance.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/progress.md

这轮只做：
- 继续追 ingest 性能，优先优化 disk append 临界区、live postings/row refs 的紧凑表示、以及 OTLP front-end 的剩余分配热点
- 每完成一轮优化，都用同机同口径 `vtbench otlp-protobuf-load` 对官方 VictoriaTraces、memory、disk 三组重测

不要做：
- 不要回头为了兼容性保留旧格式或旧路径
- 不要重新讨论大方向
- 不要换 benchmark 口径

完成后给我：
- 最新官方 vs memory vs disk 的 spans/s 和 p99
- 这一轮新增优化点的代码位置
- 还没打穿的下一层瓶颈
```
