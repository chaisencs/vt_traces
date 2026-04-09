# VictoriaTraces 设计对齐方案

## 目标

以
[/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/victoriatraces-source-analysis-report.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/victoriatraces-source-analysis-report.md)
里总结出的 upstream 设计为对照基线，重新审视 Rust 重写版，并明确三件事：

1. 当前 Rust 设计里哪些必须保留。
2. upstream VictoriaTraces 里哪些值得吸收。
3. steady-state 热路径上哪些事情应该明确停止继续做。

这份文档的目标不是“把 VictoriaTraces 原样照搬一遍”，而是：

- 把 Rust 实现重新拉回到 upstream 级别的低开销 data plane。
- 保住 Rust 版本已经形成的更强 readiness 和 cluster 语义。
- 让系统重新回到可以稳定超过 official 的 benchmark 轨道上。

## 配套细化文档

这份文档只负责总方向，不负责把每一层都展开到可直接实现的粒度。配套的细化设计如下：

1. [2026-04-08-ingest-and-persistence-detailed-design.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-ingest-and-persistence-detailed-design.md)
   重点回答写入热路径、WAL/segment/part 生命周期、durability 边界和后台 seal/merge 职责。
2. [2026-04-08-index-detailed-design.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-index-detailed-design.md)
   重点回答 head index、sealed persistent filter index、recovery checkpoint 的职责分层。
3. [2026-04-08-query-execution-detailed-design.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-query-execution-detailed-design.md)
   重点回答 route/prune/scan/decode/merge 查询框架，以及 trace-by-id、trace-search、tag-values、service/operation list 的执行路径。
4. [2026-04-08-ha-recovery-consistency-detailed-design.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-ha-recovery-consistency-detailed-design.md)
   重点回答 livez/readyz、quorum、read repair、rebalance、topology-aware placement 和 recovery v2 的边界。
5. [2026-04-09-storage-cost-model-framework.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-09-storage-cost-model-framework.md)
   统一定义写放大、读放大、空间放大、恢复放大、head 内存放大、后台债务和顶流预算口径。

## Benchmark 现实校准

当前的架构讨论不能脱离已经固定下来的同口径 benchmark 体系，基线文档见
[2026-04-08-official-benchmark-harness.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-official-benchmark-harness.md)。

在当前 native-arm64 基线上：

- `0019f74` 的 disk engine 吞吐大约是 official 的 `1.20x`，`p99` 大约是 official 的 `0.57x`。
- 当前带持久化恢复格式的 working tree 吞吐只有 official 的 `0.73x`，`p99` 则大约是 official 的 `2.07x`。

这说明问题不是“恢复设计变好了，所以系统自然慢一点”。更准确地说，是 data plane 的整体形态已经明显偏离了之前那条能赢 official 的路径。

## 对比矩阵

### 1. 写入热路径

upstream VictoriaTraces 的做法：

- 先把 span 扁平化成类似日志行的结构。
- 按 `trace_id` 做路由。
- 在网络层和内部写入层做批量化。
- 用异步方式维护内部的 trace-window 加速索引流。
- 更依赖内存缓冲和后台 flush/merge，而不是同步维护一套很重的 trace-native head 状态。

当前 Rust 设计的做法：

- 先扁平化成 `TraceBlock`。
- 做 trace-aware shard 路由。
- 通过 active WAL segment 追加写入。
- 在写入路径上同步维护 trace-native head 结构。
- segment 达到阈值后，在 steady-state ingest 阶段就进入 snapshot + seal 语义。

upstream 的优势：

- 同步写路径 CPU 开销更低。
- head 路径上的簿记工作更少。
- “先接收写入”和“再做长期布局优化”分离得更清楚。

当前 Rust 的优势：

- readiness 语义更明确。
- durability 语义更强。
- head 数据的 trace-native 读正确性更直接。

当前 Rust 的缺点：

- ack path 上同步做了太多索引维护和簿记。
- row-oriented 持久化仍然是 steady-state 的主形态。
- segment 滚动时触发的工作量仍然明显高于一次便宜的 rotate。

### 2. 查询与索引模型

upstream VictoriaTraces 的做法：

- 采用两段式 trace 查询。
- 维护独立的内部 `trace_id` 时间窗索引流。
- 使用 `indexdb + mergeset`，依赖排序字节串上的 prefix-seek 完成 tag 过滤。
- 把查询加速能力尽量压到 immutable indexed structure 和 cache 上。

当前 Rust 设计的做法：

- 已经有两段式 trace-window-first lookup。
- search/list 加速目前主要还是依赖内存中的 trace-native map 和 roaring 集合。
- recovery v2 能持久化 shard checkpoint，但还没有 upstream 风格的持久化过滤索引。

upstream 的优势：

- 持久化过滤索引的开销更低。
- 查询加速更多绑定在 immutable file 上，而不是 head state。
- 对重复 tag 查询来说，压缩和剪枝形态更成熟。

当前 Rust 的优势：

- trace-native 正确性模型更简单。
- 更容易推理整条 trace 的拼装逻辑。
- 已经具备可信的 recovery checkpoint 体系。

当前 Rust 的缺点：

- 缺少持久化的 prefix-seek 过滤索引。
- search/list 成本与 ingest 时的 head 维护绑得太死。
- sealed part 上的查询加速能力明显还落后于 upstream。

### 3. 持久化与恢复语义

upstream VictoriaTraces 的做法：

- 写入先进入 buffer 和 in-memory parts。
- 再 flush 成 immutable file parts。
- 后台持续把小 parts merge 成更大的 parts。
- 它不是经典的逐条 WAL replay 模型。
- 它用更弱一点的即时 durability，换更低的 steady-state 写入成本。

当前 Rust 设计的做法：

- 使用 active WAL segments + sealed parts。
- 增加了 recovery v2：manifest + per-shard 恢复文件。
- 保持 strict readiness 语义。
- 但当前把 data-plane 的 segment rolling 和即时 snapshot/seal 机制耦合在了一起。

upstream 的优势：

- 写路径不会被“未来查询怎么加速”这件事主导。
- 后台 merge 才是布局优化的主承担者。
- immutable file hierarchy 才是核心存储抽象。

当前 Rust 的优势：

- restart 行为更容易运维。
- readiness 语义更明确，更适合生产环境。
- recovery acceleration 已经和正确性解耦。

当前 Rust 的缺点：

- recovery v2 目前默认假设 covered segment 是 part-centric 的。
- steady-state ingest 仍然在为 head/roll/seal 语义付出过多成本。
- data-plane 的存储形态仍然更接近 row-native，而不是 block-native。

### 4. Cluster 与 HA

upstream VictoriaTraces 的做法：

- 一个相对简单的分片集群。
- 没有内建复制协议。
- HA 主要依赖外部体系。

当前 Rust 设计的做法：

- 有 write quorum。
- 有 read quorum。
- 有 read repair。
- 有 rebalance。
- 有 topology-aware placement。
- 有更强的 auth boundary。
- 有更明确的 readiness 分层。

这一块反而是当前 Rust 重写版已经领先 upstream 的地方。哪怕 raw disk ingest 现在还没有赢，它在能力模型上已经更强。

结论是：

- 应该保留 Rust 重写版在 cluster、readiness、recovery 方面的优势。
- 真正需要向 upstream 对齐的，是 storage / data-plane 的热路径设计。

## 必须保留的部分

下面这些东西，不能因为 upstream 没有就轻易扔掉：

1. 严格区分 `livez` / `readyz`。
2. 把 recovery v2 作为恢复加速结构保留下来。
3. 保留 trace-aware routing 和 cluster 侧的 quorum / repair 语义。
4. 保留 `insert` / `select` / `storage` 已经建立起来的安全边界和角色边界。

这些不是偶然复杂度，而是已经形成产品价值的能力。

## 应该从 upstream 吸收什么

### 吸收 1：异步化 trace-window 维护

upstream 给出的启发不是“去掉 trace 加速”，而是“不要在 ingest ack path 上把 trace 查询加速结构完整同步物化”。

Rust 侧应该这样走：

- 保留 trace-window-first lookup。
- 把更多 search/list 索引工作从同步 head ingest 路径上挪出去。
- 让 ingest 时的 trace bookkeeping 只保留 trace-by-id 真正需要的最小集合。

### 吸收 2：基于 immutable structure 的查询加速

upstream 把过滤加速主要放在 immutable indexed structure 上。

Rust 侧应该这样走：

- 为 sealed data 增加持久化 filter/index 层。
- 优先采用 sorted、prefix-seek-friendly、compressed 的结构，而不是继续扩大可变内存 map。
- 让 head-only 的结构尽量小、尽量短命。

### 吸收 3：让 flush/merge 主导布局优化

upstream 把“接收写入”和“优化磁盘布局”分成了两件事。

Rust 侧应该这样走：

- steady-state segment roll 不应该默认立即触发昂贵的 seal 语义。
- 更重的布局升级工作应该交给后台 worker 或后续阶段。
- 写入接受路径本身应该尽量便宜、尽量稳定。

### 吸收 4：排序压缩的 block 形态

upstream 在这些点上收益很大：

- 共同前缀利用
- 类 delta 编码
- 压缩后的 immutable blocks
- 多级目录结构上的 seek

Rust 侧应该这样走：

- sealed data 逐步脱离 row-oriented 存储。
- 逐步转向更适合查询和压缩的 sorted compressed block 结构。

## 明确要停止做的事情

1. 不要再继续把更多同步 head search/list 索引维护堆到 ingest ack path 上。
2. 不要把 row-oriented WAL payload 当成长期的 canonical storage format。
3. 不要让热路径为每一种未来查询优化都立即买单。
4. 不要把 recovery 改进和 steady-state 写入变慢绑死在一起。

## 建议的设计对齐路线

### Phase A：先让 recovery v2 能兼容更便宜的数据平面

在改热路径之前，恢复元数据需要先补一个结构能力：

- recovery manifest 必须理解 covered segment 的 kind，而不只是 segment id。
- covered entry 需要能指向 `wal` 或 `part`。

只有这样，后面才能把 steady-state 改回更便宜的 rotate-only 路径，同时不打坏 recovery v2。

### Phase B：恢复 rotate-only 的 steady-state ingestion

在 manifest 兼容之后：

- active segment 达到阈值。
- writer rotate 到一个新的 active segment。
- 旧 segment 成为一个 immutable 的、以 WAL 为载体的 sealed candidate。
- 更昂贵的 part 转换放到 ack path 之外再做。

这才更接近 upstream “先 ingest，再优化布局”的原则。

### Phase C：缩小 head index 的职责

head state 应该只保留 near-realtime trace-by-id 真正需要的东西：

- trace window
- row reference 或 block reference
- 最小必要的 serviceability metadata

search/list/tag 相关结构应该越来越多地来自 immutable 的后台构建索引，而不是同步 head map。

### Phase D：建设持久化的 prefix-seek 过滤索引

Rust 重写版不需要逐字逐节复刻 `indexdb + mergeset`，但它必须具备同类架构属性：

- immutable
- sorted
- prefix-seek-friendly
- compressed
- cacheable
- mergeable

这应该成为 sealed data 的持久化查询加速层。

### Phase E：把 sealed data 从 row-oriented 进化到 block-native

更长期看，sealed data 的格式应该朝下面这个方向收敛：

- 压缩 block
- 支持快速 seek 的目录元数据
- 查询剪枝结构
- merge 驱动的布局演进

只有走到这一步，系统才有比较大把握重新建立对 official 的长期性能优势。

## 决策总结

### 当前 Rust 设计优于 upstream 的地方

- service readiness 语义
- recovery 的可运维性
- 内部 cluster 语义
- auth 和 role boundary

### upstream 优于当前 Rust 设计的地方

- 更低开销的 steady-state ingestion
- 更清晰地把 ingest 和 query-optimization work 解耦
- 更成熟的持久化 filter/index 结构
- 更成熟的 compressed immutable layout + merge 策略

### 最优的组合设计

- upstream 风格的低开销 data plane
- Rust 风格的强 readiness、recovery 和 cluster 语义

这就是后续设计对齐的目标。

## 近期最值得下注的动作

1. 升级 recovery manifest，让它支持 `wal` 和 `part` 两种 covered entry。
2. 在 recovery-v2-compatible 的前提下，重新引入 rotate-only steady-state ingestion。
3. 继续移除或延后同步 head search/list indexing 工作。
4. 设计第一版面向 sealed segments 的 persistent prefix-seek filter index。

## 明确拒绝的方向

1. 盲目回退 `0019f74` 之后的所有 storage 改动。  
   原因：recovery 和 operability 的收益是真实的，不能一起丢掉。
2. 复制 upstream 更弱的 HA 语义。  
   原因：那会把 Rust 版本已经形成的产品优势主动砍掉。
3. 永远只在当前 row-oriented head path 上继续做微优化。  
   原因：现在的 benchmark gap 已经清楚说明，问题是结构性的。
