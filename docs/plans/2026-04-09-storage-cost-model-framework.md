# 存储设计统一成本模型框架

> 本文为写入/持久化、索引、查询、HA 四份细化设计提供统一成本语言。后续所有“更快、更省、更稳”的判断，都应尽量落到这里定义的成本维度和放大系数上，而不是只停留在抽象描述。

## 1. 目标

统一成本模型的目的有三个：

1. 让不同设计文档对“成本”说的是同一件事。
2. 让 benchmark、profiling、生产指标和设计文档之间能互相映射。
3. 让“是否达到顶流设计水位”有一套可讨论的硬标准。

## 2. 基础术语

### 2.1 逻辑单位

1. `request`
   一个外部写入或查询请求。
2. `span`
   逻辑 trace 数据的最小写入单位。
3. `trace`
   查询与路由上的主逻辑单位。
4. `batch`
   写入热路径上的物理 ingest 单元。
5. `segment`
   持久化滚动单元。
6. `part`
   面向查询的 immutable 持久化单元。
7. `block`
   查询和压缩上的最小物理扫描单元。
8. `shard`
   数据与执行分区单元。
9. `replica`
   可用性和一致性单元。

### 2.2 数据量口径

1. `logical_bytes`
   用户实际写入的逻辑 payload 大小。
2. `durable_bytes`
   为了先把数据持久接住而写出的字节数。
3. `layout_bytes`
   为了 part 化、merge、索引构建而重写的字节数。
4. `index_bytes`
   持久化查询索引占用的字节数。
5. `checkpoint_bytes`
   恢复加速结构占用或重写的字节数。
6. `scanned_bytes`
   查询真实扫描过的字节数。
7. `decoded_bytes`
   查询真实解码过的字节数。
8. `returned_bytes`
   最终返回给用户的结果字节数。

## 3. 成本维度

所有设计都用下面七类成本描述：

### 3.1 热路径 CPU 成本

定义：请求在同步路径上消耗的 CPU 工作量。

希望它主要跟这些量成正比：

1. `requests`
2. `batches`
3. `durability epochs`

而不是跟这些量成正比：

1. `unique tags`
2. `search/list 索引项`
3. `未来 merge 要做的布局优化`

### 3.2 syscall / I/O 成本

定义：热路径上真正进入内核的写 syscall、fsync、fd 切换、网络发送次数与对应字节量。

这类成本在当前 Rust 实现里已经被证明是第一类核心瓶颈之一，不能再只看 CPU flame graph。

### 3.3 延迟成本

定义：请求从进入系统到满足其角色语义的耗时。

需要拆成：

1. `ack latency`
2. `ready latency`
3. `query p50/p95/p99`
4. `repair / rebalance 附加延迟`

### 3.4 空间成本

定义：系统为了同一份逻辑数据长期保留多少字节。

拆成：

1. data bytes
2. index bytes
3. checkpoint bytes
4. replicated bytes

### 3.5 放大成本

这是最关键的一类。所有顶流存储系统都在对抗几种放大：

1. 写放大
2. 读放大
3. 解码放大
4. 空间放大
5. 恢复放大

### 3.6 后台调度成本

定义：后台 worker 为 part build、merge、index build、checkpoint rewrite 消耗的 CPU、I/O 和队列深度。

顶流系统的共性是：

1. 后台成本很高是可以接受的。
2. 后台成本污染热路径是不能接受的。

### 3.7 一致性与 HA 成本

定义：为了 quorum、replication、repair、rebalance、topology 付出的额外网络、存储和延迟成本。

这类成本不是要消灭，而是要明确记账，避免“为了 benchmark 看起来快”而偷掉语义。

## 4. 标准放大系数

### 4.1 Durability 写放大

`WA_durable = durable_bytes / logical_bytes`

含义：

1. 为了先把数据可靠接住，至少写出了多少字节。
2. 通常主要由 WAL 或等价 durable append 决定。

### 4.2 布局写放大

`WA_layout = layout_bytes / logical_bytes`

含义：

1. 为了变成 part、merge、重编码、重建索引，后台又重写了多少字节。

### 4.3 总写放大

`WA_total = (durable_bytes + layout_bytes + checkpoint_rewrite_bytes) / logical_bytes`

说明：

1. 它不是“越低越好”这么简单。
2. 顶流系统的重点是：把放大更多留在后台，并保证热路径不被放大主导。

### 4.4 持久化索引空间放大

`SA_index = index_bytes / sealed_data_bytes`

含义：

1. 为 sealed 查询加速额外付出了多少存储空间。

### 4.5 Head 内存放大

`MA_head = head_bytes / hot_logical_bytes`

含义：

1. 近实时可见数据，在内存中需要多少倍元数据和引用开销。

### 4.6 查询读放大

`RA_query = scanned_bytes / returned_bytes`

说明：

1. 这个值对不同查询类型波动会很大。
2. 对返回结果极小的查询，不应只看这个值，还要看 prune 比例和 decoded_bytes。

### 4.7 查询解码放大

`DA_query = decoded_bytes / returned_bytes`

含义：

1. 为得到最终结果，系统实际解码了多少倍数据。

### 4.8 恢复放大

`RecA = recovery_read_bytes / persisted_logical_bytes`

含义：

1. 启动恢复时，为了重新达到 ready，系统读了多少倍的已持久化数据。

顶流恢复设计的关键特征是：

`RecA` 应尽量跟 `uncovered tail` 成正比，而不是跟全部 retained bytes 成正比。

## 5. 顶流设计的五个硬标准

如果要说“设计达到业内顶流水位”，至少要同时满足：

### 5.1 热路径成本不被未来优化绑架

写入 ack 成本主要由：

1. decode
2. batch append
3. durability 边界

决定，而不是由：

1. 查询索引构建
2. part seal
3. merge
4. recovery checkpoint rewrite

决定。

### 5.2 查询成本主要消耗在 prune 之前

更准确地说：

1. 好系统把大多数候选在 route / prune 阶段就丢掉。
2. 差系统把大多数成本花在 scan / decode 后才发现“不匹配”。

### 5.3 恢复成本与尾部未覆盖数据成正比

warm restart 时，ready 耗时不应与“总 retention 数据量”线性绑定。

### 5.4 强语义成本清楚可记账

quorum、repair、replication、rebalance 带来的额外成本要清楚记录，不能混在普通吞吐指标里假装不存在。

### 5.5 预算可以用指标验证

如果一个设计没有对应的指标来验证预算是否失真，它还不算真正可评审。

## 6. 建议的目标区间

下面这些不是当前已验证结果，而是面向“顶流设计”的目标带宽，用于约束方案。

### 6.1 写入与持久化

1. `WA_durable`
   单副本目标区间：`1.00x - 1.15x`
2. `WA_layout`
   长周期平均目标区间：`0.50x - 1.50x`
3. `checkpoint_rewrite_bytes / logical_bytes`
   长周期目标区间：`0.02x - 0.15x`

### 6.2 索引

1. `SA_index`
   sealed data 目标区间：`0.08x - 0.30x`
2. `MA_head`
   hot logical bytes 目标区间：`0.03x - 0.12x`

### 6.3 查询

1. selective search 的 `candidate_part_ratio`
   目标：`< 0.10`
2. selective search 的 `candidate_block_ratio`
   目标：`< 0.02`
3. tag-values / service-list / operation-list
   目标：主要扫描 index bytes，而不是 row bytes
4. trace-by-id
   目标：历史查询的 hydration 候选块数维持在 `O(1) - O(10)` 量级，而不是随 retention 增长

### 6.4 恢复

1. warm restart 下 `RecA`
   目标区间：`0.05x - 0.20x`
2. ready latency
   主要与 uncovered tail 和 checkpoint load 成正比，而不是与总 retained bytes 成正比

## 7. 红线指标

一旦出现下面这些情况，基本说明设计在失真：

1. ingest ack path 的 CPU 与 tag cardinality 明显线性相关。
2. `sync=none` 下每个 batch 仍然强制 flush。
3. service-list / tag-values 还需要大量 row decode。
4. warm restart 仍然近似全量回扫 retained data。
5. read repair 在健康集群中成为常态路径。

## 8. 如何使用这份框架

后续每份细化设计至少都要显式回答：

1. 它主要优化的是哪一类成本。
2. 它牺牲了哪一类成本。
3. 它对应的放大系数是什么。
4. 它如何被指标验证。

只有做到这四点，设计讨论才真正进入“成本模型级别”，而不是停留在抽象偏好层面。
