# 查询执行细化设计

> 本文是
> [2026-04-08-victoriatraces-design-realignment.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-victoriatraces-design-realignment.md)
> 的第三份细化文档，专门定义查询执行链路、不同查询类型的执行路径，以及如何把查询性能建立在 immutable structure 上。

## 1. 目标

查询执行设计要回答两个关键问题：

1. 为什么当前 sealed data 查询性能还落后于 VictoriaTraces。
2. 怎样在保留 trace-native 正确性的前提下，把查询链路重新做轻。

这份设计的核心目标是：

1. 用统一的执行框架覆盖 trace-by-id、trace-search、tag-values、service/operation list。
2. 让“route -> prune -> scan -> decode -> merge”成为标准查询流水线。
3. 把更多成本挪到 prefix-seek / block pruning 阶段，而不是 row decode 阶段。
4. 让查询加速更多依赖 sealed persistent index，而不是依赖庞大的 head map。

## 2. 当前问题总结

当前查询侧的主要问题，不是某一个函数慢，而是执行模型过于依赖热态内存和 trace-native 结构：

1. sealed part 缺少成熟的持久化过滤索引，导致 prune 不够早。
2. 很多 search/list 仍然隐含依赖 head 维护时顺手建立的结构。
3. 查询代价更容易在 scan / decode 阶段爆发，而不是在 route / prune 阶段被截住。
4. cluster fan-out 之后的 merge 成本也因此被放大。

所以查询设计的重点不是“写更多缓存”，而是重建执行层次。

## 3. 统一执行框架

所有查询都统一走五段：

```mermaid
flowchart LR
    A["route"] --> B["prune"]
    B --> C["scan"]
    C --> D["hydrate/decode"]
    D --> E["merge/finalize"]
```

### 3.1 Route

作用：

1. 决定要访问哪些 shard / replica。
2. 决定是只看 head、只看 sealed，还是两者都看。
3. 决定查询时间窗。

### 3.2 Prune

作用：

1. 基于 persistent filter index 排除不可能命中的 part / block。
2. 对 trace-by-id 先缩小到候选时间窗和候选块。
3. 对 service/list/tag-values 先缩小 key range。

这是查询设计里最值钱的一层。只要 prune 做得够早，后面的 decode 和 merge 压力就会显著下降。

### 3.3 Scan

作用：

1. 读取候选索引块或数据块。
2. 在 candidate set 上顺序扫描，而不是全量扫描。
3. 尽量维持顺序 I/O 和块级连续访问。

### 3.4 Hydrate / Decode

作用：

1. 把最终必要的数据从 block/native encoding 恢复成查询结果需要的结构。
2. 对 trace-by-id 才做整条 trace 的完整 hydration。
3. 对 list/search 只解码必要字段，不做过度对象化。

### 3.5 Merge / Finalize

作用：

1. fan-in 多个 shard / replica 的部分结果。
2. 去重、排序、截断、分页。
3. 形成最终 API 响应。

## 4. 查询类型拆分

### 4.1 Trace-by-id

Trace-by-id 应保留“两段式查询”：

1. 先查 trace lookup 索引，得到时间窗和候选 part/block。
2. 再只对这些候选位置回查真实 trace payload。

要求：

1. 优先命中 head 最小结构和 sealed persistent trace lookup index。
2. 避免在全 retention 时间窗里做 `trace_id` 全局过滤。
3. hydration 必须是懒的，只在候选块缩到足够小之后才发生。

### 4.2 Trace Search

Trace search 的核心不是“把 trace-by-id 做很多次”，而是：

1. 先根据过滤条件查 persistent filter index。
2. 得到候选 trace windows / candidate blocks。
3. 再对少量候选做聚合和排序。

Rust 侧未来要对齐 upstream 的，是这一步的 persistent filter 和 prefix-seek 能力，而不是继续扩大 head map。

### 4.3 Service / Operation List

这类查询最适合从 sealed persistent index 中直接得出结果，不应再主要依赖：

1. 大型 head map
2. 运行时 scan 所有 trace 元数据

理想路径是：

1. prefix-seek 到 `tenant | service` 或 `tenant | operation`
2. 取 candidate terms
3. 去重、排序、分页

### 4.4 Tag Values

tag-values 查询最需要 prefix-seek 风格的结构。推荐执行路径：

1. route 到目标 shards
2. 按 `tenant | field_name | prefix(value)` 做 index seek
3. 只 scan 候选 value blocks
4. merge 去重后返回

如果 tag-values 还要主要依赖全量 row decode，那就说明索引设计仍然没对齐。

## 5. Head 与 Sealed 的查询边界

### 5.1 Head 查询职责

Head 只负责：

1. 最新尚未 seal 数据的近实时可见性。
2. trace-by-id 的最小实时读取。
3. 在 sealed 还没覆盖的尾部补齐查询结果。

### 5.2 Sealed 查询职责

Sealed 应该逐步成为：

1. 绝大多数 search/list 查询的主路径。
2. trace-by-id 历史部分的主路径。
3. tag-values / service-list / operation-list 的主路径。

这样一来，查询性能才能更多跟 immutable structure 绑定，而不是跟内存大小绑定。

## 6. Block-native 查询思路

为了让 sealed data 真正快起来，查询执行需要从 row-native 思维过渡到 block-native 思维。

### 6.1 块级元数据

每个 block 至少要暴露：

1. `min_ts`
2. `max_ts`
3. `trace_count`
4. `field existence bitmap`
5. 可选 `service/op dictionary summary`

这样 prune 才有足够的信息，不必一上来就 decode block body。

### 6.2 列级或半列级解码

对 search/list 类查询，不应该整行整块都 hydrate。更合理的做法是：

1. 先只读 block header / dictionary
2. 只解码过滤相关列
3. 只在最终需要返回详情时再解码更多 payload

这与 VictoriaTraces 借助 immutable structure 降低 query cost 的思路是一致的。

## 7. Cluster 查询执行

这部分是 Rust 当前相对更强的地方，需要保留并细化。

### 7.1 Route 层

Route 层需要同时考虑：

1. shard ownership
2. replica placement
3. read quorum
4. topology-aware 优先访问

### 7.2 Fan-out 策略

查询不一定每次都要 fan-out 到全部副本。建议：

1. trace-by-id 优先命中 trace-aware ownership 最可能的副本集合。
2. search/list 在满足 read quorum 的前提下优先访问延迟更低的副本。
3. 只在副本失败或校验冲突时扩大 fan-out。

### 7.3 Merge 层

Merge 不只是简单拼接结果，还应该承担：

1. 去重
2. 读修复触发条件判断
3. conflict resolution
4. top-k 裁剪

这一层是我们相对于 upstream 真正有价值的扩展能力。

## 8. 读修复与查询执行的边界

当前系统有 read repair，这个能力值得保留，但必须避免让它污染普通查询热路径。

建议原则：

1. 正常查询优先返回结果，不把读修复变成默认阻塞阶段。
2. 只在明确检测到版本冲突、缺副本或 quorum mismatch 时触发 repair。
3. repair 的代价和普通查询响应时间分开记账。

这样既保住高可用能力，也避免把正常查询拖慢。

## 9. 性能目标

查询执行设计需要服务这些具体目标：

1. 大多数 search/list 查询在 prune 后只接触少量 candidate parts/blocks。
2. tag-values / service-list / operation-list 不再依赖大规模 row decode。
3. trace-by-id 的历史部分不再依赖全量 head state。
4. cluster fan-out 后的 merge 成本显著下降。

### 9.1 成本模型

本节沿用
[2026-04-09-storage-cost-model-framework.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-09-storage-cost-model-framework.md)
里的统一口径，但查询层需要把总成本拆成：

1. `T_query = T_route + T_prune + T_scan + T_decode + T_merge + T_repair_optional`
2. `RA_query = scanned_bytes / returned_bytes`
3. `DA_query = decoded_bytes / returned_bytes`
4. `FanoutAmp = replicas_contacted / replicas_required_for_quorum`
5. `MergeAmp = merge_input_items / returned_items`

这里最关键的不是把某一个系数做到理论最小，而是确保：

1. 成本主要消耗在 route/prune 前后，而不是 row decode 之后。
2. `repair_optional` 真的只是可选尾部成本，而不是健康路径的默认成本。

### 9.2 顶流预算

1. `trace-by-id`
   目标：历史查询的 candidate blocks 维持在 `O(1) - O(10)` 量级。
   说明：如果 trace-by-id 的候选块数随 retention 线性增长，说明 trace lookup 设计失真。
2. selective `trace search`
   目标：part 级 prune 至少达到 `90%+`，block 级 prune 进入 `99%` 量级。
   说明：这是“过滤查询主要依赖 immutable index”是否真的成立的核心信号。
3. `tag-values / service-list / operation-list`
   目标：主要扫描 index bytes，而不是 row bytes。
   说明：如果这三类查询仍然有显著 `decoded_bytes`，说明执行路径还没真正 index-first。
4. `DA_query`
   目标：对 list/search 类查询明显小于 row-native 方案。
   说明：它不需要绝对很小，但必须体现“只解码必要字段”的收益。
5. `FanoutAmp`
   目标：健康集群中尽量逼近 quorum 所需副本数，不默认扫满全部副本。
6. `MergeAmp`
   目标：merge 工作量更多跟 top-k / returned items 成正比，而不是跟 scanned items 成正比。
7. `T_repair_optional / T_query`
   目标：健康路径下接近 `0`
   说明：如果 repair 成本经常成为 query 主成本，系统就不是稳态，而是持续漂移。

## 10. 关键指标

必须持续暴露：

1. `vt_query_pruned_parts_total`
2. `vt_query_pruned_blocks_total`
3. `vt_query_scanned_blocks_total`
4. `vt_query_decoded_blocks_total`
5. `vt_query_trace_lookup_candidates_total`
6. `vt_query_trace_hydrated_blocks_total`
7. `vt_query_fanout_shards_total`
8. `vt_query_fanout_replicas_total`
9. `vt_query_merge_input_items_total`
10. `vt_query_read_repair_triggers_total`

## 11. 与 VictoriaTraces 的对比结论

VictoriaTraces 的查询性能优势，并不主要来自“trace 对象模型更聪明”，而来自：

1. 两段式 trace 查询。
2. immutable 的过滤索引结构。
3. prefix-seek 友好的编码。
4. 后台 merge 形成的更顺序读布局。

Rust 侧应该保留的则是：

1. 更强的 trace-native 正确性。
2. 更强的 cluster / quorum / repair 语义。
3. 更强的 readiness 和恢复模型。

查询执行设计的方向就是：借 upstream 的查询形态，把我们的 HA 与正确性能力承接进去。

## 12. 明确拒绝的方向

1. 不继续扩大 head-only 查询加速结构。
2. 不用“大缓存”替代持久化过滤索引。
3. 不让 read repair 成为普通查询默认阻塞阶段。
4. 不把 recovery checkpoint 混成 query-serving index。

## 13. 实施顺序建议

1. 先落地统一的 route/prune/scan/decode/merge 执行框架。
2. 再让 trace search 和 tag-values 迁移到 sealed persistent index。
3. 然后减少 head 对 service/list/search 的职责。
4. 最后再做更激进的 block-native 解码与 merge 优化。

这样可以保证每一步都有可观测的性能收益，也避免一次性重写全部查询路径。
