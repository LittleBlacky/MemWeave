# Task 5 开发日志：事务 Outbox 任务存储

日期：2026-08-29

## 本次范围

本轮先实现 Outbox 的持久化任务存储和 LocalWorker 的成功消费闭环，不实现指数退避调度或外部投影 handler。

## 实际变更

- 新增 `outbox` 表和 `0002_outbox` Python migration；
- 新增 `OutboxStatus`、`OutboxItem` 和 `OutboxStore`；
- 支持幂等入队、领取、应用、可重试、死信；
- processing 任务按租约过期后可被重新领取；
- 领取使用事务和行锁（SQLite 由 `BEGIN IMMEDIATE` 保证串行）；
- payload 以 JSON 文本保存，任务状态和尝试次数持久化。
- 新增 `LocalWorker.run_once()`，按 topic 调用 handler 并在成功后标记 `applied`。

## TDD 记录

- RED：`tests/test_outbox.py` 导入 `memweave.outbox` 失败；`tests/test_worker.py` 导入 `memweave.worker` 失败；
- GREEN：实现表结构、状态转换和 `LocalWorker` 后，入队幂等、重试、租约恢复、终态和成功消费测试通过。

## 验证

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_outbox.py tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
36 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

## 已知边界

- 指数退避策略、最大尝试次数和 handler 幂等消费已在本 Task 的后续子任务实现；
- Outbox 入队尚未与事件追加/会话投影合并为同一个领域事务。

## LocalWorker 第一阶段提交

`LocalWorker.run_once()` 的成功消费路径已单独提交：`50257df feat: add local outbox worker success path`。

## LocalWorker 重试策略

新增可注入时钟的失败处理：handler 异常时按指数退避重新入队，延迟为 `base_delay * 2^(attempts-1)` 并受 `max_delay` 限制；达到 `max_attempts` 后进入 `dead_letter`。增加确定性测试覆盖 10 秒、20 秒退避和第三次死信。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_worker.py tests/test_outbox.py tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
37 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

重试策略提交：`640206c feat: add outbox retry backoff and dead letters`。

## LocalWorker 幂等消费

新增 `outbox_consumer_receipts` 表和 `consumer_id + idempotency_key` 去重记录。Worker 调用 handler 前登记 receipt，已完成 receipt 的重投递直接跳过；handler 失败释放 receipt 后进入原有 retryable/dead-letter 流程，处理中断可由租约恢复。

该机制保证已确认完成的消费不会重复调用，但不承诺任意外部副作用 exactly-once；handler 仍需使用幂等键，或与 receipt 共享事务。

幂等消费验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_worker.py tests/test_outbox.py tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
40 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

幂等消费提交：`dc569f6 feat: add durable outbox consumer receipts`。

## Task 5 边界收敛

日期：2026-09-05

- `claim()` 的状态更新增加与候选状态相同的条件，通用关系数据库下丢失行锁时，
  只有一个并发消费者能成功把任务切换为 `processing`。
- Outbox 公共入口校验 topic、payload、幂等键、任务 ID 和租约参数类型，避免底层
  `AttributeError` 或隐式转换泄漏给调用方。
- `LocalWorker` 找不到 topic handler 时按正常失败策略进入 `retryable`，达到最大尝试
  次数后进入 `dead_letter`，不再留下永久 `processing` 任务。
- 发现租约过期后旧 Worker 仍可按 `item_id` 提交状态和消费 receipt，可能覆盖新 Worker
  的处理结果。新增持久化 `lease_token` 作为 fencing token；任务状态转换和 receipt
  完成/释放都必须匹配当前 token，旧 Worker 的提交会被拒绝。
- 新增 `0017_outbox_lease_tokens` 迁移；旧库中的历史 processing 任务会在重新 claim
  时获得新 token，不能复用旧的无 token 状态。
- `enqueue()` 的幂等预检查与唯一约束之间存在并发窗口；现在唯一键冲突会在事务回滚后
  重新读取获胜任务，并按完整请求校验后返回，不再把正常重复提交暴露为数据库异常。
- `processing` 任务若缺少 `locked_at` 会被视为无租约孤儿并允许重新领取，避免历史数据
  或异常修复留下永久卡住的任务。
- `claim(topic=...)` 与 `enqueue()` 统一校验 topic，避免非字符串或空白过滤条件落到底层
  数据库后才报错。
- 消费 receipt 的完成/释放路径同时校验 outbox 当前租约 token，避免旧 Worker 在任务被
  新 Worker 接管后仍能修改 receipt 状态。
- 重试时间现在必须是带时区的 `datetime`，并规范化为 UTC 后持久化，避免不同偏移量的
  ISO 字符串按字典序比较造成调度错误。
- 消费 receipt 遇到未知状态时现在显式拒绝，不会把损坏数据误当成过期 processing
  自动接管。
- Outbox payload 使用严格 JSON 序列化，拒绝 `NaN/Infinity` 等非标准数字，保证重启
  反序列化和幂等指纹稳定。

## 后续维护事项

- 第三方厂商 Adapter 不在当前 Task 5 实现范围内，但必须作为后续开发事项持续维护：先
  稳定统一 `MemoryIndexAdapter` 契约和本地参考实现，再以可选扩展包提供向量、图和关键词
  索引厂商 Adapter，并为每个 Adapter 运行幂等、乱序、删除和重建契约测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_outbox.py tests/test_worker.py -q
16 passed
```
