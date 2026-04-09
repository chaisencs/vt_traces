# 索引细化设计

> 本文是
> [2026-04-08-victoriatraces-design-realignment.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-08-victoriatraces-design-realignment.md)
> 的第二份细化文档，专门定义索引分层、索引职责和索引构建时机。

## 1. 目标

索引设计要同时解决两个问题：

1. 让 search/list/tag-values/query acceleration 不再绑死在 ingest 热路径上。
2. 让 sealed data 的查询性能逐步逼近甚至超过 VictoriaTraces 的 `indexdb + mergeset` 路线。

因此，这份设计的目标不是“做一套功能更全的内存 map”，而是：

1. 把索引分成 head、sealed、checkpoint 三层。
2. 让 head index 尽量小、尽量短命。
3. 让 sealed index 成为主要查询加速来源。
4. 让 recovery checkpoint 只服务启动加速，不承担通用查询职责。

## 2. 当前问题总结

当前索引侧的问题主要有三类：

1. head 状态承担了过多 search/list/query acceleration 责任。
2. sealed part 缺少成熟的持久化过滤索引，只能依赖更多内存状态补齐查询体验。
3. recovery v2 虽然解决了启动恢复，但它不是通用查询索引，却容易被误用成“已经有持久化索引了”。

要把这些问题拆开，就必须先定义索引分层。

## 3. 索引分层模型

建议把整个系统的索引统一分成四层：

### L0：Request-local 路由索引

职责：

1. 按 `trace_id` 或 shard key 做 ingest 路由。
2. 在请求生命周期内完成 batch 分桶。

特征：

1. 完全临时。
2. 不持久化。
3. 不服务查询。

### L1：Head Realtime Index

职责：

1. 支持近实时 trace-by-id。
2. 支持 `ready` 后的最新数据可见性。
3. 作为未 seal 数据的最小可查询结构。

特征：

1. 生命周期短。
2. 尽量 small and exact。
3. 只维护 trace-by-id 真正需要的信息。

### L2：Sealed Persistent Filter Index

职责：

1. 支持 trace search、service list、operation list、tag values 等查询加速。
2. 支持高效 prune segment / part / block。
3. 让查询更多依赖 immutable structure，而不是 head map。

特征：

1. 持久化。
2. 与 sealed part 一起生成。
3. 支持 prefix-seek、字典化、压缩和块级裁剪。

### L3：Recovery Checkpoint

职责：

1. 只服务 startup 加速。
2. 记录 shard state 和 covered segment set。
3. 降低 readyz 恢复耗时。

特征：

1. 不是 query index。
2. 不是 source of truth。
3. 损坏时 soft-fail。

## 4. Head Index 设计

### 4.1 Head Index 应该保留什么

Head index 只保留以下最小集合：

1. `trace_id -> trace window`
2. `trace_id -> row refs / block refs`
3. 近实时 trace hydration 所需的最小 metadata
4. 统计类 counters

如果某个结构不是 trace-by-id 立即查询所必需，就不应放在同步 head index。

### 4.2 Head Index 不该继续承担什么

下面这些东西应逐步移出 head：

1. 完整 service -> trace set 映射
2. operation -> trace set 映射
3. 通用 tag/value -> roaring 集
4. 各种 list/search 的重型内存加速结构

原因很简单：这些结构扩大了 ingest 维护成本，却没有形成与其成本对等的 sealed-side 长期收益。

### 4.3 Head Ref 形态

Head 中的 ref 不再强调“行级对象图”，而要强调“最小可回查引用”。

推荐形态：

1. `segment_id`
2. `block_id` 或 `row_range`
3. `min_ts` / `max_ts`
4. 可选 `span_count`

当 sealed data 逐步转向 block-native 存储后，head ref 也应该自然过渡到 block ref，而不是长期固化在 row ref 上。

## 5. Sealed Persistent Filter Index 设计

### 5.1 设计目标

sealed index 要达到的不是“也能查”，而是：

1. 能高效 prune 不相关数据。
2. 能减少 decode 放大。
3. 能减少 fan-out 后的 merge 放大。
4. 能在不依赖大内存 map 的前提下提供稳定查询性能。

### 5.2 基础 key 空间

不要求逐字复刻 VictoriaTraces 的 `indexdb + mergeset`，但必须具备同类属性。建议采用排序字节串 key 空间：

1. `tenant | service | trace_window`
2. `tenant | operation | trace_window`
3. `tenant | field_name | field_value | trace_window`
4. `tenant | trace_id | trace_window`

value 部分不直接存大对象，而优先存：

1. `part_id`
2. `block_range`
3. `doc_count`
4. `min_ts` / `max_ts`
5. `dictionary_id` 或 postings pointer

### 5.3 索引粒度

不建议一上来就直接维护全局超细粒度 postings。建议分三层粒度：

1. segment / part 级 existence
2. block 级 candidate set
3. 必要时再进入更细粒度 row decode

这能在可控复杂度下先把 prune 做起来。

### 5.4 编码方式

sealed index 需要天然支持这些特征：

1. prefix-compressed sorted keys
2. dictionary-coded field names / values
3. block header + checksum
4. 压缩 payload
5. 可顺序扫描和二分 seek

这正是我们要向 upstream 吸收的核心，而不是继续扩大 mutable hash map。

## 6. Trace 查询专用索引

### 6.1 Trace-by-id 不等于通用 tag 索引

这一点必须显式写清：

1. `trace_id` 查询属于专用路径。
2. `trace search` 属于过滤路径。
3. 两者虽然共享部分底层 part/block 数据，但索引职责不同。

因此要保留一条专门的 trace-window / trace lookup 加速路径，而不是把 trace-by-id 混进通用 tag 索引里。

### 6.2 两段式模型继续保留

当前两段式思路是对的，应保留：

1. 先找到 trace 可能位于哪些时间窗/part/block。
2. 再回查真实 trace 数据。

区别在于，未来更多时间窗和候选块信息应该来自 sealed persistent index，而不是更多内存 head map。

## 7. 索引构建时机

### 7.1 同步阶段

同步 ingest 阶段只允许构建：

1. shard 路由必须的最小结构
2. head trace-by-id 最小结构

### 7.2 rotate 后异步阶段

segment 变成 immutable WAL 后，可以异步做：

1. segment summary 提取
2. dictionary 构建
3. part materialize
4. sealed filter index 构建

### 7.3 merge 阶段

part merge 期间可以进一步做：

1. postings 合并
2. block 重新排序或重编码
3. 小索引块合并成更大、更连续的读布局

也就是说，真正昂贵的索引重写只能发生在后台 merge 体系里，而不是发生在 ack path 上。

## 8. Recovery Checkpoint 与 Query Index 的边界

这是当前最容易混淆的一点，必须单独写出来。

### 8.1 Recovery Checkpoint 不做什么

它不承担：

1. service list 查询
2. tag values 查询
3. 通用 trace search
4. 长期过滤加速

### 8.2 它真正做什么

它只承担：

1. 快速恢复 head-like shard state
2. 让 startup 复用已覆盖的 persisted state
3. 避免每次启动全量回扫 segment

这条边界如果不守住，就会再次把 recovery 结构和 steady-state query path 绑死。

## 9. 与 VictoriaTraces 的对比结论

VictoriaTraces 的强项不在“它有更复杂的对象模型”，而在：

1. 它把过滤索引做成了 immutable、可排序、可 prefix-seek 的结构。
2. 它让 merge 成为索引布局优化的主要承担者。
3. 它让 query acceleration 更少依赖热态内存。

我们要保留的则是：

1. 更强的 trace-native 正确性模型。
2. 更强的 readiness 与恢复模型。
3. 更强的 cluster / HA 语义。

索引设计的任务就是把这两类优势合起来，而不是在二者之间二选一。

## 10. 性能目标

索引设计需要服务这些具体目标：

1. ingest 路径上同步索引维护 CPU 显著下降。
2. sealed data 查询时，segment / block prune 比例显著提升。
3. tag-values / service-list / trace-search 不再主要依赖大内存 head 结构。
4. recovery checkpoint 大小和重写成本与 query index 解耦。

### 10.1 成本模型

本节沿用
[2026-04-09-storage-cost-model-framework.md](/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/plans/2026-04-09-storage-cost-model-framework.md)
里的统一口径，但索引层只关心四类核心系数：

1. `MA_head = head_bytes / hot_logical_bytes`
2. `SA_index = sealed_filter_index_bytes / sealed_part_bytes`
3. `WA_index_build = index_build_write_bytes / sealed_part_bytes`
4. `PruneGain = 1 - candidate_blocks / total_blocks_in_window`

额外还有一条必须单独记账：

`CheckpointCoupling = checkpoint_rewrite_bytes / index_build_write_bytes`

这不是标准放大系数，而是用来验证 recovery checkpoint 是否重新和 query index 绑死。

### 10.2 顶流预算

1. `MA_head`
   目标：`0.03x - 0.12x`
   说明：head 只能保留 near-realtime trace-by-id 必需结构。
   如果显著超过这个区间，通常意味着 search/list 状态重新渗回了热路径。
2. `head_base_metadata_per_live_trace`
   目标：基础元数据维持在 `O(10^2 bytes)`，不应轻易到 `KiB/trace` 量级。
3. `SA_index`
   目标：`0.08x - 0.30x`
   说明：sealed persistent filter index 可以占空间，但不能接近或超过数据本体。
4. `WA_index_build`
   目标：新增 sealed data 的索引构建写放大尽量压在 `0.10x - 0.35x`
   说明：如果索引构建本身接近一次完整重写 data bytes，说明索引布局过重。
5. `PruneGain`
   selective 查询目标：part 级 prune 至少一个数量级，block 级 prune 至少两个数量级。
   说明：如果 selective 查询仍然广泛触达 block body，索引设计就还不成熟。
6. `CheckpointCoupling`
   目标：尽量接近 `0`
   说明：checkpoint rewrite 应该跟 hot shard state 变化绑定，而不是跟 sealed filter index 重写绑定。

## 11. 关键指标

必须持续暴露：

1. `vt_storage_head_trace_count`
2. `vt_storage_head_trace_ref_count`
3. `vt_storage_head_index_bytes`
4. `vt_storage_sealed_filter_index_bytes`
5. `vt_storage_filter_index_build_queue_depth`
6. `vt_storage_filter_index_pruned_parts_total`
7. `vt_storage_filter_index_pruned_blocks_total`
8. `vt_storage_trace_lookup_candidates_total`
9. `vt_storage_trace_lookup_hydrated_blocks_total`

## 12. 实施顺序建议

1. 先收缩 head index 的职责。
2. 再为 sealed part 建立最小可用的 persistent filter index。
3. 然后把 trace search、service list、tag values 逐步迁移到 sealed index。
4. 最后再做更激进的 block/postings 压缩优化。

这样推进的好处是：每一步都能独立 benchmark，也能避免一开始把复杂度堆得太高。
