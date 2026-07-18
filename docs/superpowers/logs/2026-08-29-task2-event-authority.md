# Task 2 开发日志：SQLite 权威存储和不可变事件日志

日期：2026-08-29

## 目标

实现单机可运行的 SQLite 权威层，保证事件追加、流内严格递增序号、重复投递幂等、事件不可变和并发写入安全。

## 实际变更

- 创建 `src/memweave/db.py`：SQLite WAL、外键、busy timeout、事务和读连接管理。
- 创建 `src/memweave/events.py`：`EventStore.append`、`list_after` 和 `last_seq`。
- 增加 `events`、`stream_heads` 和 `projection_watermarks` 表。
- 事件保存 `event_id`、`stream_id`、`seq`、协议版本、request_id、幂等键、发生时间、接收时间、因果 ID 和关联 ID。
- 使用写事务分配 `seq`，并用唯一约束防止重复事件和重复幂等键。
- 重复事件内容一致时返回原事件；同一事件 ID 内容不一致时拒绝并报告不可变冲突。
- 扩展事件类型和结构化 payload 原样保存。

## TDD 记录

- RED：事件测试在 `memweave.events` 尚不存在时因模块导入失败。
- GREEN：实现数据库和 EventStore 后，顺序、幂等、并发和元数据测试全部通过。

## 验证

```text
G:\Anaconda\envs\smallshrimp\python.exe -m pytest tests -q
16 passed
G:\Anaconda\envs\smallshrimp\python.exe -m compileall -q src
git diff --check
```

## 提交

- `406ea0b feat: add transactional event authority`

## 遗留风险

- 当前只完成事件权威层，没有 Session Projection、Durable Projection 或 Outbox。
- `:memory:` SQLite 多连接场景尚未作为第一阶段目标；当前测试和推荐用法使用文件数据库。
- 事件 payload 的业务 Schema 校验和事件处理器注册将在后续任务实现。
