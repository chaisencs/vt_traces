# Big Leap Performance Plan: 目标翻倍

> **目标：** 把 disk engine 的 OTLP protobuf ingest 吞吐从当前 ~430K spans/s 提升到 **800K+ spans/s**，在当前 ~8.5% 领先 official 的基础上拉开到 **2x 以上**。
>
> **核心判断：** 当前代码没有充分利用 Rust 的语言优势。热路径上存在多层冗余数据变换、不必要的内存分配和序列化瓶颈，每一层都在浪费 CPU。单独优化任何一层只能拿到个位数百分比，但组合起来攻击全链路就有机会翻倍。

---

## 现有热路径瓶颈全图

从 HTTP 请求到 bytes 落盘，当前每个 span 要经过以下 7 层处理：

```
Wire bytes (protobuf)
  → [1] prost decode + Arc<str> alloc → ExportTraceServiceRequest
  → [2] OTLP flatten + field cache → TraceBlock (columnar, owned)
  → [3] route_trace_blocks_for_append (shard check/split)
  → [4] combiner: coalesce_trace_blocks_for_shard (rebuild TraceBlock!)
  → [5] PreparedTraceBlockAppend::new
       ├─ prepare_shared_group_metadata (field scan)
       ├─ prepare_block_row_metadata (per-row Arc clone + shard hash)
       └─ encode_trace_block_rows_packed (RE-ENCODE EVERY ROW!)
  → [6] ActiveSegment::append_block_rows
       ├─ CRC32 compute
       ├─ BufWriter write (WAL)
       ├─ extend cached_payload_bytes (DOUBLE COPY)
       └─ SegmentAccumulator::ingest_prepared_block_rows (string intern + index)
  → [7] group_commit: flush + optional fsync
```

### 瓶颈定量估计（sync_policy=None，5 spans/request）

| 层 | 占 CPU 估计 | 核心浪费 |
|---|---:|---|
| [1+2] protobuf decode + flatten | ~25% | 完整物化所有 string 到 `Arc<str>` |
| [4] combiner coalesce | ~5% | 多个 block 合一要重建整个 TraceBlock |
| [5] encode_trace_block_rows_packed | **~25%** | **把已经 decode 好的 columnar 数据逐行重新 encode！** |
| [5] prepare_block_row_metadata | ~8% | 每行 clone Arc、重算 shard hash |
| [6] WAL write + double copy | ~12% | payload 同时写 BufWriter 和 cached_payload_bytes |
| [6] SegmentAccumulator index | ~15% | 同步做 service/operation/field interning |
| [7] flush | ~5% | BufWriter flush syscall |
| 其他 (HTTP, routing) | ~5% | |

**关键洞察：`encode_trace_block_rows_packed` 是最大的单点浪费。** 它把 columnar `TraceBlock` 逐行重新 serialize 成 row-oriented 格式写 WAL，然后读的时候再逐行 decode 回来。这完全是一个不必要的 round-trip。

---

## 三阶段优化路线

### Phase 1: 消除冗余变换层（目标 +40-60%，低风险）

这一阶段的核心思路：**不要把 columnar 数据打散成 row 再写 WAL，直接写 block encoding。**

#### 1A: Columnar WAL Format — 消灭 `encode_trace_block_rows_packed`

**当前问题：**
- `encode_trace_block_rows_packed` 对每一行重新 encode trace_id、span_id、name、timestamps、所有 fields
- 这些数据已经在 `TraceBlock` 里以 columnar 形式存在了
- `vtcore::codec` 里已经有 `encode_trace_block` 可以直接 encode 整个 block

**改动：**
1. 引入 `WAL_VERSION_BLOCK_BINARY = 4`，WAL batch 里存的不再是 N 条 length-prefixed row bytes，而是一个 `encode_trace_block(block)` 的 blob
2. `PreparedTraceBlockAppend` 不再需要 `encoded_row_bytes` + `encoded_row_ranges`，改为 `encoded_block_bytes: Vec<u8>`
3. `ActiveSegment::append_block_rows` 简化为：写 batch_body_len + CRC32 + block bytes
4. Head segment 读改为 `decode_trace_block` 然后按需提取 row

**省掉的工作：**
- 每个 row 的逐字段 write_string（当前热路径最重的 CPU 消耗）
- per-row length prefix 计算
- encoded_row_ranges 构建

**保留的工作：**
- `prepare_block_row_metadata` 仍然需要（为 SegmentAccumulator 提供 trace_id、时间窗口）

**对 recovery 的影响：**
- WAL recovery 读到 version=4 时直接 `decode_trace_block`，构建 SegmentAccumulator
- seal path 已有 block → part 的路径，不受影响

**预估收益：** 纯 CPU 省 ~20-25%，转化为吞吐 +25-35%

#### 1B: Prepare-Before-Combiner — 并行化 CPU 密集工作

**当前问题：**
- HTTP 线程把原始 `TraceBlock` 扔进 combiner 队列，combiner 线程做所有 prepare + write
- prepare（metadata 提取 + encoding）是纯 CPU 工作，完全可以并行
- 当前设计让 N 个 HTTP 线程竞争同一个 combiner，prepare 变成串行瓶颈

**改动：**
1. `submit_trace_shard_blocks` 在 enqueue 之前先调用 `PreparedTraceBlockAppend::new`
2. `DiskTraceAppendRequest` 持有 `PreparedTraceBlockAppend` 而非 `Vec<TraceBlock>`
3. combiner 只做 `PreparedTraceShardBatch::from_prepared_blocks` + WAL write
4. 消除 `coalesce_trace_blocks_for_shard`（不再需要合并原始 block）

**省掉的工作：**
- combiner 不再做 coalescing（copy all block data into new builder）
- combiner 不再做 prepare（encode + metadata extract）
- prepare 工作被分散到 N 个 HTTP 线程并行执行

**预估收益：** combiner 串行段缩短 ~60%，系统并行度提升，吞吐 +10-15%

#### 1C: Slim Head Index — 推迟搜索元数据到 seal

**当前问题：**
- `SegmentAccumulator::ingest_prepared_block_rows` 在每次 append 时同步 intern service name、operation name、indexed field name/value
- 这些 index 只有 `search_traces`/`list_services` 用，`trace-by-id` 不需要
- 这个工作占了 ack path ~15% CPU

**改动：**
（已有 plan 在 `2026-04-07-disk-next-hotspots-plan.md`，这里复用）
1. Head accumulator 只维护 trace_id → window + row_refs
2. service/operation/field 索引推到 seal 时构建
3. search 查询只看 sealed segments（head 期间 search 有短暂延迟，可接受）

**预估收益：** ack path CPU -15%，吞吐 +10-15%

#### 1D: 消除 Head Payload Double-Copy

**当前问题：**
- `append_block_rows` 把 encoded payload 同时写入 WAL BufWriter 和 `cached_payload_bytes`
- `cached_payload_bytes` 用于 head segment 的 trace-by-id 读

**改动（配合 1A）：**
- Head segment 存 `Vec<Arc<TraceBlock>>` 而非 raw bytes
- `read_rows` 直接从 TraceBlock 解出 rows，不需要 decode
- 删除 `cached_payload_bytes`、`cached_payload_ranges`、`cached_row_length_prefixes`

**省掉的工作：**
- 整个 `extend_from_slice` payload copy（当前每次 append 都在做）
- 整个 `decode_trace_row` on head read（直接从 TraceBlock 取）

**预估收益：** CPU -5-8%

#### Phase 1 组合预估

| 优化 | CPU 节省 | 吞吐提升 |
|---|---|---|
| 1A Columnar WAL | -20-25% | +25-35% |
| 1B Prepare-Before-Combiner | -8-10% | +10-15% |
| 1C Slim Head Index | -12-15% | +10-15% |
| 1D No Double-Copy | -5-8% | +5-8% |
| **Phase 1 Total** | **-45-58%** | **+50-73%** → **~650-740K spans/s** |

---

### Phase 2: 利用 Rust 零拷贝优势（目标再 +30-50%，中等风险）

Phase 1 之后如果到了 700K 区间但还没翻倍，Phase 2 继续攻击 decode 层。

#### 2A: Zero-Copy Protobuf Field References

**核心洞察：** Rust 的一大语言优势是可以安全地持有对 input buffer 的引用，Go 做不到（GC 会移动对象）。当前代码完全没利用这一点——每个 string 都 `Arc<str>::from()` 做了堆分配。

**改动：**
1. 引入 `BorrowedTraceBlock<'a>` 或 `TraceBlockView<'a>`，字段引用原始 `Bytes` buffer
2. OTLP decode 阶段不分配 `Arc<str>`，而是记录 `(offset, len)` 到原始 protobuf bytes
3. 只在需要持久化的地方（head segment cache、shard index intern）才 copy 出 owned string
4. WAL 写路径：直接从原始 bytes 里 slice 出需要的部分，零拷贝

**关键实现细节：**
- Axum 提供 `body: Bytes`（引用计数的 shared buffer）
- 可以 `body.clone()` 把引用传给 storage 层，不做 deep copy
- `BorrowedTraceBlock` 持有 `Bytes` + 各字段的 offset/len vectors
- 当 TraceBlock 需要比 request 活更久（head segment cache），才 `Arc<TraceBlock>` 化

**预估收益：** decode 层 CPU -50-60%（从 ~25% 总 CPU 降到 ~10%），系统吞吐 +15-25%

#### 2B: Vectored/Scatter-Gather WAL Writes

**当前问题：**
- `BufWriter::write_all` 做 userspace buffering，flush 时一次 `write` syscall
- 但 WAL 数据天然是多段的（header + checksum + payload），可以用 `writev` 避免 copy into BufWriter

**改动：**
1. 用 `std::io::IoSlice` + `write_vectored` 替代 `BufWriter`
2. WAL header、CRC bytes、block bytes 作为 3 个 IoSlice 一次写出
3. 如果 block bytes 本身也是多段的（如 1B 的多个 prepared blocks），可以全部放进 iovec

**预估收益：** I/O path CPU -3-5%

#### 2C: 更快的 Checksum

**当前问题：**
- `crc32fast::Hasher` 用软件或 SSE4.2 加速的 CRC32
- ARM 有 `CRC32C` 硬件指令，比 CRC32 更快

**改动：**
1. 切换到 `crc32c` crate（或 `xxhash-rust`），利用硬件 CRC32C
2. 或者在 `sync_policy=None` 时直接用 xxHash3（更快但非标准 CRC）

**预估收益：** checksum CPU -50%（占总 ~2-3%），吞吐 +1-2%

#### 2D: Arena-Based String Interning

**当前问题：**
- `StringTable::intern` 每个新 string 做 `Arc::<str>::from(value)` — 堆分配 + 原子引用计数
- 在高吞吐下，分配器压力和 cache line bouncing 显著

**改动：**
1. 每个 active segment 用一个 bump arena（如 `bumpalo`）
2. string intern 从 arena 分配，不做 Arc
3. seal 时把 arena 里的 strings 转为 owned（arena 生命周期和 segment 一样）

**预估收益：** intern path CPU -30-40%（占总 ~5%），吞吐 +2-3%

#### Phase 2 组合预估

| 优化 | 额外吞吐提升 |
|---|---|
| 2A Zero-Copy Proto | +15-25% |
| 2B Vectored Writes | +3-5% |
| 2C Fast Checksum | +1-2% |
| 2D Arena Interning | +2-3% |
| **Phase 2 Total** | **+21-35%** → 如果 Phase 1 到 700K，Phase 2 可推到 **850-950K** |

---

### Phase 3: 极限压榨（目标再 +30%+，高风险）

如果 Phase 1+2 到了 900K 附近还想继续推，Phase 3 是更激进的方向。

#### 3A: Direct Protobuf-to-WAL Pipeline

**核心思路：** 完全跳过 `TraceBlock` 物化。OTLP protobuf wire bytes 经过最小解析（提取 trace_id + timestamps 用于索引），然后 **原样** 写入 WAL。

**实现：**
1. 引入 `WAL_VERSION_OTLP_PASSTHROUGH = 5`
2. WAL 存的是 `[trace_metadata_header][original_protobuf_bytes]`
3. trace_metadata_header 只包含：trace_id(s)、min/max timestamps、row count
4. 读取时按需 decode protobuf

**收益：** 消灭 decode + re-encode 两层（~50% 总 CPU），但实现复杂度很高

#### 3B: io_uring for WAL (Linux)

**改动：**
1. 用 `tokio-uring` 或 `io-uring` crate
2. WAL writes 提交到 io_uring ring，不阻塞任何线程
3. group commit 变成等 io_uring completion event

**收益：** 消灭 WAL write 的 syscall 上下文切换开销

#### 3C: SIMD-Accelerated Field Scanning

**改动：**
- 用 SIMD 扫描 protobuf varint boundaries
- 用 SIMD 做 string comparison（`resource_attr:service.name` lookup）
- 用 SIMD 做 CRC32C

**收益：** 微观层面的 CPU 节省，在极高吞吐时变得有意义

---

## 执行顺序和 Gate

### 执行顺序

```
Phase 1A (Columnar WAL)
    ↓ benchmark gate
Phase 1B (Prepare-Before-Combiner)
    ↓ benchmark gate
Phase 1C (Slim Head Index)
    ↓ benchmark gate
Phase 1D (No Double-Copy)
    ↓ full benchmark gate (fresh single + 5-round median vs official)
    ↓ IF 未达 2x → continue to Phase 2
Phase 2A (Zero-Copy Proto)
    ↓ benchmark gate
Phase 2B-D (supporting optimizations)
    ↓ full benchmark gate
    ↓ IF 未达 2x → assess Phase 3 feasibility
```

### 每步 Gate 标准

每个子阶段结束后必须跑同口径 benchmark：

```bash
cargo build --release --target aarch64-apple-darwin -p vtapi -p vtbench
target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load \
  --url=http://127.0.0.1:13083/v1/traces \
  --duration-secs=5 --warmup-secs=1 \
  --concurrency=32 --spans-per-request=5 --payload-variants=1024
```

**Pass 条件：**
1. errors = 0
2. 吞吐不低于上一步
3. p99 不劣化超过 10%
4. 全部 cargo test pass

**Reject 条件：**
1. 吞吐下降
2. p99 劣化超过 10%
3. 测试失败
4. seal 积压爆炸

---

## 为什么这次有机会翻倍

1. **当前最大瓶颈是冗余编码，不是 I/O 或算法**。`encode_trace_block_rows_packed` 是纯浪费的 CPU 工作，Columnar WAL 直接消灭它。

2. **Rust 的零拷贝优势完全没有被利用**。当前代码的 `Arc<str>` 全物化风格和 Go 版本没有本质区别。Phase 2A 才是真正 exploit Rust 语言优势的地方。

3. **CPU 密集工作被 combiner 串行化了**。Phase 1B 把 prepare 推到 HTTP 线程，让 N 核并行工作。

4. **多个独立优化层的乘法效应**。如果 Phase 1A 省 25% CPU，1B 省 10%，1C 省 15%，1D 省 5%，剩余 CPU 只有当前的 45%，吞吐理论上翻倍。

5. **之前的失败路线攻击了错误的瓶颈**。Outer microbatch、async shard worker、pre-prepare block fusion 都在试图减少 WAL I/O 次数或改变排队形态，但实际瓶颈不在 I/O 次数上，而在 CPU 编码工作上。
