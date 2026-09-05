# Task 4：长期记忆权威、版本与 tombstone

日期：2026-09-05

## 目标

建立独立于 SessionStore 的长期记忆权威表，保存同一作用域和 key 的完整版本链，
让删除通过 tombstone 屏蔽旧值，并为异步乱序和并发更新提供明确的拒绝语义。

## 实现

- 新增 `DurableMemoryStore`，提供 `create`、`update`、`forget`、`get_active` 和
  `list_versions` 接口。
- 新增 `durable_memories` 迁移。旧版本只标记为 `superseded`，删除追加
  `retracted` 版本，不物理覆盖历史。
- `expected_version` 使用 CAS；`source_seq` 必须严格递增，旧来源不能覆盖新版本。
- 同一记录或同一来源事件重试返回已落库版本；同一来源事件携带不同内容时抛出
  `StaleWriteError`，避免重复记忆和静默冲突。
- 同一 memory key 的 `memory_id` 在版本链中保持稳定；`session_only` 记录拒绝进入
  长期权威表。
- `DurableMemoryStore.create()` 现在只接受 `active` 初始记录；candidate、
  needs_confirmation 以及其它生命周期状态不会遮蔽已有 active 记忆，候选晋升留给
  Task 7 的策略流程。
- 并发更新和删除改用带 `version`、`key`、`active` 状态条件的 CAS 更新；条件更新
  影响行数为零时返回 `StaleWriteError`，不插入新版本。唯一版本约束产生的
  `IntegrityError` 也统一转换为 `StaleWriteError`，调用方不再依赖底层数据库异常类型。
- `forget()` 同时收到 `key` 和 `memory_id` 时现在比较完整记忆身份；即使历史数据中
  出现同 key 的不同 ID，也会拒绝请求，不会静默删除错误版本。

## 验证

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_durable_versions.py -q
10 passed

G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest -q
171 passed

G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

## 边界

本 Task 不负责 Outbox、向量/图索引、自然语言提取和召回。跨数据库事务、索引投影
和长期记忆的自动晋升留到后续任务；本表是可独立读取和重建派生索引的长期状态权威。
