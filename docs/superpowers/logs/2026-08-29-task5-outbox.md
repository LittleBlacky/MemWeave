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

幂等消费提交：`dc569f6 feat: add durable outbox consumer receipts`。
