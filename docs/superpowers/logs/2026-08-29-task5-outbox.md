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

- 指数退避策略、最大尝试次数和 handler 幂等消费将在后续 Task 5 子任务实现；
- Outbox 入队尚未与事件追加/会话投影合并为同一个领域事务。

## LocalWorker 第一阶段提交

`LocalWorker.run_once()` 的成功消费路径已单独提交：`50257df feat: add local outbox worker success path`。
