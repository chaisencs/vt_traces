# Thread Capability Package

## 1. Thread Identity

- Thread 名称：Rust VT design realignment
- 主题边界：对标 VictoriaTraces 设计，重构 Rust data plane，同时保留 readiness/recovery/cluster 优势
- 当前仓库：`/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace`
- 主要受众：继续做架构收敛、benchmark、实现改造的后续 Thread

## 2. Mission

- North Star：让 Rust rewrite 的数据平面重新回到可持续超过 official 的轨道，同时不丢掉 recovery v2、strict readiness、cluster semantics
- 成功标准：
  - 数据平面设计方向与 upstream 的高性能原则重新对齐
  - benchmark 体系保持固定，不重新发明
  - 新 Thread 直接从“实现下一步改造”开始，而不是重新理解目标
- 当前明确不做的事情：
  - 不重新讨论官方 benchmark 是否可信
  - 不回到只做局部微优化的路线
  - 不牺牲 strict readiness 语义
- 为什么这个目标重要：
  - 当前 head 已经从早先领先 official 的位置掉到明显落后，问题是结构性的

## 3. Input / Output Contract

- 典型输入：
  - “继续做 Rust VT 和 official 的设计对齐”
  - “继续把 hot path 拉回超过 official”
  - “继续做 recovery v2 兼容的 data plane 改造”
- 期望输出：
  - 更新后的设计文档
  - benchmark 证据
  - 实际代码改动
  - 对下一轮改造收益和风险的判断
- 输出风格要求：
  - 先给结论
  - 用固定 benchmark 口径说话
  - 区分“已被 benchmark 证实”和“仍是待验证假设”
- 新 Thread 在面对模糊输入时应如何处理：
  - 先读取 canonical assets
  - 继续沿用已固定的比较口径和判断框架
  - 不重新定义 North Star

## 4. Core Abstractions

- 抽象 1：
  - 定义：`data plane`
  - 用法：专指 steady-state ingest/write/query acceleration 的底层结构，不含 cluster control plane 和 auth
- 抽象 2：
  - 定义：`recovery v2`
  - 用法：指 manifest + per-shard checkpoint + binary meta 的恢复加速体系；它是加速结构，不是正确性依赖
- 抽象 3：
  - 定义：`same-shape benchmark`
  - 用法：专指 `vtbench official-compare` 的 clean-start、same-host、same-load-shape ingest 对比口径

## 5. Canonical Assets

- 必读文档：
  - [/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/victoriatraces-source-analysis-report.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/victoriatraces-source-analysis-report.md)
  - [/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/architecture.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/architecture.md)
  - [/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-recovery-index-v2-design.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-recovery-index-v2-design.md)
  - [/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-victoriatraces-design-realignment.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-victoriatraces-design-realignment.md)
- 关键 runbook：
  - [/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-official-benchmark-harness.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-official-benchmark-harness.md)
- benchmark / baselines / results：
  - current head vs official summary: `/tmp/rust-vt-official-bench-current-5r-20260408-2052/summary.json`
  - old `0019f74` vs official summary: `/tmp/rust-vt-official-bench-0019f74-manual-20260408-2054/summary.json`
- 样例输出：
  - the realignment design doc above is the canonical comparison artifact

## 6. Reusable Capabilities

- 能力 1：
  - 触发条件：用户问“为什么现在不如 official”
  - 标准动作：先对比 old winning commit、current head、official native baseline
  - 输出物：同口径 benchmark 结论，而不是猜测
- 能力 2：
  - 触发条件：用户问“该抄 upstream 什么”
  - 标准动作：沿四个维度比较 ingest/query/persistence/cluster
  - 输出物：保留项、采用项、拒绝项
- 能力 3：
  - 触发条件：准备切新 Thread
  - 标准动作：交付 capability package，而不是只给聊天摘要
  - 输出物：可直接开工的入口文档和 canonical asset 列表

## 7. Standard Workflow

1. 先判断：这是 benchmark 问题、热路径问题，还是架构问题
2. 再提炼：把问题归到 ingest/query/persistence/cluster 四维之一
3. 再纠偏：确认是否偏离固定 benchmark 口径
4. 再补齐：查 canonical docs 和已有结果
5. 最后产出：文档、证据、代码或下一轮实现建议

## 8. Evidence / Benchmark System

- 对标对象：official VictoriaTraces native-arm64
- 比较口径：`vtbench official-compare`, clean-start, same host, same load shape
- 数据来源：
  - harness manifest
  - round reports
  - median summaries
  - top-level summary
- 命令入口：
  - `target/aarch64-apple-darwin/release/vtbench official-compare --official-bin=... --rust-bin=... --rounds=...`
- 结果落盘位置：
  - `/tmp/rust-vt-official-bench-*`
- 哪些结果被视为可信：
  - native-arm64 official
  - clean-start medians
  - same harness, same shape, same machine

## 9. Do Not Repeat

- 不要重新讨论的结论：
  - current head 相对 `0019f74` 的回退是真实的
  - 纯 combiner 外层回退不是主因
  - recovery v2 本身不是 steady-state ingest 的主热点
- 不要重新做的准备动作：
  - 不要重新设计 benchmark harness
  - 不要重新从 upstream 仓库源码零散推导整体架构
- 已经确认价值不高的方向：
  - 只在当前 row-oriented hot path 上继续做小修小补
  - 只盯 combiner 外壳
- 容易导致目标漂移的行为：
  - 为了追吞吐直接丢掉 readiness/recovery/cluster 语义

## 10. Decision Heuristics

- 当输入很散时，优先：收敛到 ingest/query/persistence/cluster 四维
- 当目标模糊时，优先：回到 North Star 和 benchmark evidence
- 当已有文档很多但能力没有固定时，优先：写 capability package，不写流水账 handoff
- 当需要切 Thread 时，优先：让新 Thread 继承能力、方法、证据体系

## 11. Open Hypotheses / Next Bets

- 当前最值得继续验证的假设：
  - manifest 支持 `wal|part` 覆盖后，rotate-only steady-state ingestion 会显著改善吞吐
  - 把 head search/list index 进一步移出 ack path 会继续接近 old winning path
  - sealed-segment persistent filter index 是重新超越 official 的必要条件
- 下一轮最可能有价值的动作：
  - 先做 segment-kind-aware recovery manifest
  - 再恢复 rotate-only steady-state path
  - 然后设计第一个 prefix-seek persistent filter index
- 哪些点仍然不确定：
  - 最佳 wal->part 转换时机
  - sealed data block format 的第一版应该多接近 mergeset

## 12. Next Thread Entry

- 新 Thread 第一步必须做什么：
  - 先读 canonical assets，尤其是 realignment doc 和 benchmark harness doc
- 新 Thread 不要做什么：
  - 不要重新定义 official compare 口径
  - 不要重新走“先微优化当前 row path”路线
- 完成后应该给出什么输出：
  - 新增设计或代码
  - benchmark 证据
  - 对下一轮最值钱动作的更新判断
