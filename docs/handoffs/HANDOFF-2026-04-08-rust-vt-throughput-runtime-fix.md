# Handoff Template

适用场景：

- 当前 thread 已经很长，准备切新线程继续
- 这轮已经完成协议兼容和两机部署验证，但真实流量下暴露出新的 runtime / query 问题

## 已完成

- 已补齐 VictoriaTraces 兼容入口，目标是让上游只改 URL：
  - `insert` 兼容 `POST /insert/opentelemetry/v1/traces`
  - ingest 兼容标准 OTLP JSON camelCase payload
  - `select` 兼容 `/select/jaeger`，并补了 `dependencies`
  - Jaeger trace search 兼容 `tags`、`minDuration`、`maxDuration`
- 兼容层代码已 push 到远端分支：
  - branch: `codex/perf-trace-microbatch-leap`
  - commit: `40b2781`
- 兼容层测试已通过：
  - `cargo test -p vtapi --test http_api_tests`
  - 结果：`51 passed; 0 failed`
- 兼容层补丁后按原 ingest-only 口径复测，性能仍明显高于 official：
  - `/v1/traces`: `698772.296 ops/s`, `p99 0.742ms`
  - `/insert/opentelemetry/v1/traces`: `644780.769 ops/s`, `p99 0.788ms`
  - 对 official fresh single `396475.630` / 5-round median `343086.506` 仍有显著优势
- 两台 Linux 机器已部署成功并验证：
  - `store1.fd.inf.bj1.wormpex.com`
  - `store2.fd.inf.bj1.wormpex.com`
- 当前部署拓扑：
  - `store1`: `storage-a + insert + select`
  - `store2`: `storage-b + insert + select`
- `insert` 侧已按用户要求改成内网无鉴权：
  - 两台机器的 `/etc/rust-victoria-trace/insert.env` 已移除 `VT_API_BEARER_TOKEN`
  - `wtrace` sender 无需加 `Authorization`，只改 URL 即可写入
- 已实测：
  - 无鉴权 `POST /insert/opentelemetry/v1/traces` 返回 `200 {"ingested_rows":1}`
  - `select` 可查回兼容写入的 trace
  - 用户已把小流量切到 Rust `insert`，并确认真实业务 trace 能查到

## 未完成

- 当前 `throughput` 档在真实流量下的 production runtime gate 没过
- Jaeger `dependencies` 仍是重路径，不能直接给 Grafana 用
- `throughput` 档还没有真实可控的内存包线
- 还没有重新给用户一个可上线的 production 档建议
- `store1` 的 `storage@a` 在停流后重启恢复阶段卡住，还没查清

## 当前阻塞

- 最大阻塞不是协议兼容，而是 runtime 行为：
  - 每台 `storage` 的 `vtapi` RSS`~64GB`
  - `vt_storage_trace_seal_queue_depth` 在 `103/104`
  - 停流后 backlog 观察一段时间没有明显回落
- Jaeger 查询面会进一步放大问题：
  - `/select/jaeger/api/traces` 会先 search hit，再全量拉 trace rows 组装 Jaeger payload
  - `/select/jaeger/api/dependencies` 当前按 `usize::MAX` 扫时间窗，并对命中 trace 做全量读取
- 用户已暂停写入，不打算继续拿当前候选做 production canary
- `store1` 当前额外有一个现场问题：
  - `systemctl status` 显示 `rust-victoria-trace-storage@a` active
  - 但 `127.0.0.1:13011` 长时间未监听
  - `curl http://127.0.0.1:13011/healthz` 持续 `Connection refused`
  - 这说明 `storage@a` 卡在启动/恢复阶段，尚未完成 bind

## 关键结论

- 协议兼容目标已经达到：
  - `wtrace` 只改 `insert` URL 就能写入
  - `select/jaeger` 路由也已存在
- 但“可兼容”不等于“可直接上生产”：
  - 当前 `throughput` 候选在真实流量下未通过 runtime gate
  - 真实阻塞点是 seal backlog、head 膨胀、Jaeger 重查询
- `free` 接近 0 不是唯一判断依据；真正危险信号是：
  - `MemAvailable` 仍有 `52-55GB`
  - 但 `storage` 进程 RSS 已膨胀到 `~64GB`
  - `seal_queue_depth` 卡在百级
- 不建议继续把 Grafana Jaeger datasource 接到当前 `select`
- 下一线程应该先救 runtime，再谈 Grafana 和 production

## 已改动文件

- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/tests/http_api_tests.rs`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
- `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/production-release-guide.md`

## 已执行验证

- 命令：`cargo test -p vtapi official_victoria_traces_insert_path_accepts_standard_otlp_json -- --exact`
- 结果：通过
- 命令：`cargo test -p vtapi jaeger_trace_search_supports_tags_and_duration_filters -- --exact`
- 结果：通过
- 命令：`cargo test -p vtapi jaeger_dependencies_endpoint_returns_service_edges -- --exact`
- 结果：通过
- 命令：`cargo test -p vtapi --test http_api_tests`
- 结果：`51 passed; 0 failed`
- 命令：`target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13183/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/vt_perf_compat_20260408_142139/candidate_v1.json`
- 结果：`698772.296 ops/s`, `p99 0.742ms`
- 命令：`target/aarch64-apple-darwin/release/vtbench otlp-protobuf-load --url=http://127.0.0.1:13183/insert/opentelemetry/v1/traces --duration-secs=5 --warmup-secs=1 --concurrency=32 --spans-per-request=5 --payload-variants=1024 --report-file=/tmp/vt_perf_compat_20260408_142139/candidate_official_path.json`
- 结果：`644780.769 ops/s`, `p99 0.788ms`
- 命令：两台 Linux 节点重建 release 包并滚动部署 `40b2781`
- 结果：`insert`/`select`/`storage` 基础启动和兼容 smoke 通过
- 命令：无鉴权 `POST /insert/opentelemetry/v1/traces`
- 结果：两台节点都返回 `200 {"ingested_rows":1}`

## 相关文件与证据

- 代码：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/main.rs`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs`
- 文档：
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/README.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/2026-04-06-otlp-ingest-performance-report.md`
  - `/Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/production-release-guide.md`
- 基准结果：
  - `/tmp/vt_perf_compat_20260408_142139/candidate_v1.json`
  - `/tmp/vt_perf_compat_20260408_142139/candidate_official_path.json`
- 现场运行证据：
  - `store1` runtime metrics（聊天中已记录）：
    - `vt_rows_ingested_total 10179005`
    - `vt_storage_trace_head_segments 119`
    - `vt_storage_trace_head_rows 7337724`
    - `vt_storage_trace_live_update_queue_depth 0`
    - `vt_storage_trace_seal_queue_depth 103`
    - `storage vtapi RSS 64069728 KB`
  - `store2` runtime metrics（聊天中已记录）：
    - `vt_rows_ingested_total 10206997`
    - `vt_storage_trace_head_segments 120`
    - `vt_storage_trace_head_rows 7365473`
    - `vt_storage_trace_live_update_queue_depth 0`
    - `vt_storage_trace_seal_queue_depth 104`
    - `storage vtapi RSS 64427816 KB`
- 相关现场目录：
  - `store1 storage path`: `/data2/rust-victoria-trace/storage-a`
  - `store2 storage path`: `/data2/rust-victoria-trace/storage-b`
- 上游兼容确认：
  - `/Users/sen.chai/blf_prod_workspace/FD-3341/wtrace/consumer-saveSpanToHbase/src/main/java/com/wormpex/fd/trace/task/otel/VictoriaTracesStorage.java`

## 下一步建议

1. 先只处理 `store1` 的 `storage@a` 启动卡住问题，不要动 `store2`。
2. 确认 `storage@a` 是卡在恢复/扫描/启动前绑定阶段，还是已经进入异常死锁/长阻塞。
3. 修 `Jaeger dependencies`：
   - 不能再 `usize::MAX` 扫时间窗
   - 必须加硬上限和可控退化
4. 修 `throughput` 内存包线：
   - 优先查 seal backlog 为什么停流后仍不降
   - 查 deferred WAL + head segments + single seal worker 的组合是否导致恢复困难和内存长期滞留
5. 修完后重跑真实写流量验证，再决定：
   - 是否还保留 `throughput` 作为 production 候选
   - `select/jaeger` 是否能安全给 Grafana

## 下一线程启动词

```text
基于 /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/handoffs/HANDOFF-2026-04-08-rust-vt-throughput-runtime-fix.md 继续。
请先读取：
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/app.rs
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtapi/src/main.rs
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/crates/vtstorage/src/disk.rs
- /Users/sen.chai/wmt_shop_env_projects/codex_projects/opentelemetry-migrate/rust_victoria_trace/docs/2026-04-06-otlp-ingest-performance-report.md
- /tmp/vt_perf_compat_20260408_142139/candidate_v1.json
- /tmp/vt_perf_compat_20260408_142139/candidate_official_path.json

这轮只做：
- 先诊断 store1 上 rust-victoria-trace-storage@a 为什么 active 但 13011 长时间不监听
- 修 Jaeger dependencies 和 Jaeger trace search 的重查询路径，给出可控上限
- 修 throughput 档真实流量下的 seal backlog / head 内存膨胀问题
- 重新给出 production 可用性判断

不要做：
- 不要再扩功能面
- 不要重新折腾部署资产
- 不要只用 ingest-only benchmark 宣称 production 可用
- 不要忽略 store1 当前启动卡住的问题直接继续放流量

完成后给我：
- store1 启动卡住的根因和修复结果
- Jaeger 查询面的修复方案和验证结果
- throughput 档在真实流量下的内存/封盘结论
- 是否还能继续走高吞吐 production 方案，以及需要什么边界条件
```
