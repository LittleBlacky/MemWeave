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

## 存储扩展架构修订

根据多数据库协同需求，Task 2 后续修订为“Storage Ports, Migrations, and Event Authority”：

- 引入 SQLAlchemy Core 2.x，避免事件领域逻辑依赖 `sqlite3.Connection`。
- 将核心 DDL 移到 `migrations/0001_core.sql`，通过 `MigrationRunner` 记录并幂等执行版本。
- `SQLiteDatabase` 负责 SQLite WAL、busy timeout 和 `BEGIN IMMEDIATE`；`db.Database` 保留为兼容入口。
- 增加 `RelationalDatabase`、`EventRepository`、`ProjectionBackend`、`VectorIndex`、`GraphStore` 和 `KeywordIndex` 端口。
- 增加 `StorageCoordinator`，支持多个投影后端独立 watermark 和失败隔离，不使用跨数据库两阶段提交。
- `EventStore` 改用 SQLAlchemy Connection 的显式事务接口，原有序号、幂等和不可变语义保持不变。

修订后验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests -q
19 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

修订提交：`60abdb8 feat: add extensible storage ports and event authority`

## 代码审查修订

审查发现上一版虽然引入了 SQLAlchemy，但 `events.py` 仍通过 `exec_driver_sql` 直接拼接 SQLite 风格 SQL，无法充分利用数据库方言抽象。该问题已修正：

- 新增 `storage/schema.py`，以 SQLAlchemy Core `Table` 定义关系表元数据。
- `events.py` 的查询、插入和更新改为 `select`、`insert`、`update` 构造，不再包含运行时 SQL 字符串。
- `ports.py` 不再暴露 SQLAlchemy `Connection` 类型，关系数据库端口保持基础设施无关。
- 保留的 `exec_driver_sql` 仅用于执行外部迁移脚本；SQLite 专属 PRAGMA 和 `BEGIN IMMEDIATE` 保留在 SQLite Adapter 内。
- 增加通用 `SQLAlchemyDatabase` 使用 SQLite URL 执行核心迁移的契约测试。

该修订验证：8 个存储/事件测试通过；随后全量已提交测试仍通过。

最终修订提交：`c6a156d refactor: remove runtime sql from event repository`

## 并发追加审查修订

审查发现通用 `SQLAlchemyDatabase` 使用普通事务时，同一事件流的并发追加可能同时读取相同的 `last_seq`，导致 `stream_heads` 或 `events(stream_id, seq)` 唯一约束冲突。该问题在 SQLite 专用适配器的 `BEGIN IMMEDIATE` 路径之外仍然存在。

修订内容：

- 将单次事件追加封装为独立事务操作；
- 对唯一约束竞争以及 SQLite 锁、数据库死锁和序列化失败进行有限指数退避重试；
- 每次重试重新读取流头，不复用冲突事务中的序号；
- 增加通用 SQLAlchemy SQLite URL 的 20 路并发追加契约测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py::test_generic_sqlalchemy_database_serializes_concurrent_event_appends -q
5 次连续运行通过
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_storage_ports.py -q
21 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`efcea0c fix: retry concurrent event sequence allocation`
