# Big Leap — Profiling 修正版

> 基于 2026-04-07 的真实 CPU profiling 数据，修正优化方向。

## Profiling 结果（macOS `sample`，10 秒采样，ARM64 native）

Benchmark: `otlp-protobuf-load`, concurrency=32, spans-per-request=5, sync_policy=none

**Storage path 2113 samples = 100%**

| 瓶颈 | samples | 占比 | 性质 |
|---|---:|---:|---|
| **thread::park (等 combiner)** | 980 | **46.4%** | 阻塞/竞争 |
| **WAL write() syscall** | 908 | **43.0%** | 内核 I/O |
| SegmentAccumulator hashmap ops | 90 | 4.3% | CPU |
| crc32fast | 43 | 2.0% | CPU |
| mpsc send/recv | 38 | 1.8% | 同步开销 |
| encode_trace_block_rows_packed | 28 | 1.3% | CPU |
| prepare_block_row_metadata | 27 | 1.3% | CPU |

另外 protobuf decode 约 250 samples（占 handler 总工作量 ~10%）。

## 我之前的判断哪里错了

| 之前的预估 | 实际 profiling | 偏差 |
|---|---|---|
| encode_trace_block_rows_packed = ~25% | **1.3%** | **偏高 20 倍** |
| SegmentAccumulator indexing = ~15% | **4.3%** | 偏高 3.5 倍 |
| WAL I/O = ~10-15% | **43%** | **偏低 3-4 倍** |
| Combiner contention 未单独估计 | **46.4%** | 完全漏掉 |

**根本原因：sync_policy=none 下，BufWriter::flush 的 write() syscall 成本远高于预期，并且 combiner 的单线程串行设计导致大量 HTTP 线程阻塞等待（thread::park），这两项合计占了 89% 的存储路径时间。**

## 修正后的瓶颈层次

```
#1 (46%) Combiner contention — 32 个 HTTP 线程争同一个 shard 的 combiner
#2 (43%) WAL write() syscall — BufWriter flush 触发内核 write()
#3 (4%)  Accumulator indexing — hashmap 操作
#4 (2%)  CRC32
#5 (1%)  Row encoding
```

**系统是 I/O + 竞争瓶颈，不是 CPU 编码瓶颈。**

## 修正后的优化方向

### Priority 1: 消除 combiner 竞争（攻击 #1，46%）

当前 combiner 设计的核心问题：
- 32 个 tokio 线程通过 `std::sync::mpsc::channel` 提交请求
- 只有获得 `combining=true` 的那 1 个线程做实际 WAL 写入
- **其他 31 个线程在 `mpsc::recv()` 上阻塞（变成 `thread::park`）**
- 这相当于 32 路并发变成了 1 路串行

**方案 A: 异步 combiner（推荐）**
- 把 `std::sync::mpsc` 替换为 `tokio::sync::oneshot`
- combiner 等待结果不再阻塞 tokio 线程
- 阻塞的线程可以去处理其他 HTTP 请求
- 这不减少 combiner 的串行工作量，但让 tokio 线程池不再白白 park

**方案 B: 多 combiner / 减少竞争**
- 增加 trace shard 数（当前默认 `available_parallelism().clamp(1,16)`）
- 或者让每个 shard 有多个 combiner slot
- 但 shard 数 sweep 之前做过，16 已经是当前机器最优

**方案 C: 跳过 combiner，直接写**
- 当只有一个 pending request 时，跳过 coalesce 直接走 prepare → WAL write
- 避免无意义的排队/唤醒

### Priority 2: 减少每次 WAL write 的 syscall 成本（攻击 #2，43%）

write() syscall 占 43% 说明每次 flush 的数据量太小，syscall overhead/data ratio 很高。

**方案 A: 增大 BufWriter 缓冲区 + 延迟 flush**
- 当前 `TRACE_WAL_WRITER_CAPACITY_BYTES = 1MB`，但每次 combiner run 后立即 flush
- 如果让多个 combiner batch 共享一次 flush（group commit epoch），syscall 次数大幅下降

**方案 B: 用 io_uring (Linux) 或 writev 做异步/批量写**
- 不阻塞线程等 write() 返回

**方案 C: 对 sync_policy=none 跳过 flush**
- 当 sync_policy=none 时不需要保证每次 append 都 flush
- 让 BufWriter 自己管 flush 时机（缓冲区满或 segment roll 时才 flush）

### Priority 3: Slim Head Index（攻击 #3，4.3%）

仍然值得做，但优先级降低。在 #1 和 #2 解决之前，这只能贡献个位数百分比。

### 去优先级的方向

| 方向 | 原始优先级 | 修正后 | 原因 |
|---|---|---|---|
| Columnar WAL | Phase 1 第一位 | **降到 Phase 3** | encode 只占 1.3%，消灭它不会有可见收益 |
| Zero-Copy Proto | Phase 2 | **降到 Phase 4** | decode 只占 ~10%，不是当前瓶颈 |
| Arena Interning | Phase 2 | **降到 Phase 4** | hashmap+intern 只占 ~4%，不紧急 |

## 修正后的执行顺序

```
Step 1: 异步化 combiner 等待（tokio::sync::oneshot 替代 mpsc）
    ↓ benchmark gate
Step 2: 减少 WAL flush 频率（sync_policy=none 时延迟 flush）
    ↓ benchmark gate
Step 3: Slim Head Index
    ↓ benchmark gate
Step 4: 评估 → 如果还需要继续 → 考虑 group commit epoch / Columnar WAL
```

## 对 GPT 建议的重新评估

| GPT 建议 | 修正后的判断 |
|---|---|
| Slim Head Index 第一优先级 | **部分同意**：方向对，但 profiling 显示它只攻击 4.3% |
| Columnar WAL 推迟 | **完全同意**：profiling 证明 encode 只占 1.3% |
| "先拿稳定领先再加码" | **同意策略**：但真正的大机会在 combiner + I/O，不在 CPU |

## 核心结论

**之前所有人（包括我）都在猜 CPU 编码是瓶颈。Profiling 证明不是。真正的瓶颈是：32 路并发被 combiner 串行化为 1 路，加上每次串行 WAL write 都做一次内核 syscall。攻击这两个瓶颈，才有机会做到"一大截"。**
