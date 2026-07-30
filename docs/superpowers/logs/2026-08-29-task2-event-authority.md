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

## 迁移执行审查修订

审查发现原迁移器使用 `script.split(";")` 拆分 SQL，无法正确处理字符串、触发器、函数体等包含分号的 SQL，也无法保证同一迁移文件在不同数据库方言下可执行。

修订内容：

- 删除文本迁移 `migrations/0001_core.sql`，改为版本化 Python migration `migrations/0001_core.py`；
- `MigrationRunner` 加载并执行 `upgrade(connection)`，不再自行解析 SQL 文本；
- 核心表和约束由 SQLAlchemy Core `schema.py` 定义，迁移只负责版本化演进；
- 将事件幂等键约束纳入 SQLAlchemy 表定义，避免迁移定义与运行时定义分叉；
- 增加自定义迁移测试，验证值 `remember; this` 中的分号不会导致迁移被截断。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py -q
10 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src migrations
git diff --check
```

本次修订提交：`280cf9e refactor: use sqlalchemy python migrations`

## 迁移资源打包审查修订

审查发现默认迁移路径依赖源码仓库根目录。安装 wheel 后，顶层 `migrations/` 目录可能不存在，导致数据库初始化找不到迁移文件。

修订内容：

- 将迁移放入 `src/memweave/migrations/versions/` 包内；
- `MigrationRunner()` 默认通过 `importlib.resources` 发现包内迁移；
- `migration_dir` 仍可用于外部项目提供自定义 Python migration；
- 数据库适配器不再根据 `__file__` 推导源码目录。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py -q
11 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src migrations
git diff --check
pip wheel . --no-deps --wheel-dir <temporary-directory>
wheel contains memweave/migrations/versions/0001_core.py
installed wheel migration smoke test: ['0001_core']
```

本次修订提交：`6d66a79 fix: load migrations from installed package`

## 事件幂等校验审查修订

审查发现重复 `event_id` 的比较遗漏了 `occurred_at`、`schema_version` 和 `protocol_version`。显式传入不同发生时间时，旧实现会错误返回第一次事件。

修订内容：

- 将固定的 schema/protocol 版本纳入不可变字段比较；
- 调用者显式提供 `occurred_at` 时，重复事件必须使用完全相同的时间；
- 调用者未提供 `occurred_at` 时继续使用原有自动生成时间的幂等语义；
- 增加不同 `occurred_at` 的重复事件回归测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
24 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`cbdc593 fix: validate immutable event timestamps`

## 投影接口语义审查修订

审查发现 `ProjectionBackend.project(event)` 与向量、图、关键词索引的 `upsert/delete` 职责混用，Coordinator 会把原始 `Event` 直接传给不理解事件语义的索引后端。

修订内容：

- 保留 `ProjectionBackend` 作为健康状态和水位能力的公共契约；
- 新增 `EventProjector.apply(event)`，Coordinator 只注册和调度事件投影器；
- `VectorIndex`、`GraphStore`、`KeywordIndex` 独立接收 `MemoryRecord`，不再继承事件投影行为；
- 增加契约测试，确保索引对象不能被注册为事件投影器。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
25 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`84e7374 refactor: split event and index projection ports`

## EventRepository 契约审查修订

审查发现 `EventRepository.append` 使用 `*args/**kwargs`，无法向调用方或第三方适配器表达真实参数契约，静态检查也无法发现参数名和类型错误。

修订内容：

- 用显式签名替换 `*args/**kwargs`；
- 声明流 ID、事件类型、payload、actor、request ID 及所有可选幂等/因果字段；
- 参数顺序和默认值与 `EventStore.append` 保持一致；
- 增加接口签名回归测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
26 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`961a017 fix: define explicit event repository contract`

## StorageCoordinator 能力边界审查修订

审查发现当前 `StorageCoordinator` 只是进程内顺序分发器，没有持久化 checkpoint、失败队列或重启恢复能力，但名称和文档容易让调用方误以为它已经提供可靠投递。

修订内容：

- 将实现类明确命名为 `ProjectionDispatcher`；
- 保留 `StorageCoordinator` 作为兼容别名；
- 在模块和设计文档中明确当前仅提供 best-effort in-process fan-out；
- 明确 Outbox 负责后续的持久化水位、重试、重启恢复和索引重建。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
27 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`b301bab refactor: clarify projection dispatcher boundary`

## SQLite `:memory:` 审查修订

审查发现 SQLite 内存数据库按连接隔离。迁移在主线程连接上执行后，工作线程从连接池获得新连接，会出现 `no such table: events`；`check_same_thread=False` 并不会让不同连接共享内存状态。

修订内容：

- `:memory:` 使用 SQLAlchemy `StaticPool`，让同一进程内线程共享一条连接；
- 仅对内存数据库使用 `RLock` 串行化连接访问，避免同一 DBAPI 连接上的事务交叉；
- 文件 SQLite 继续使用原有连接池和并发行为；
- 增加主线程迁移、8 线程并发追加的回归测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
28 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`c7e2642 fix: share sqlite memory database across threads`

## 投影水位查询审查修订

审查发现 `ProjectionDispatcher.watermarks()` 使用字典推导式，任一后端的水位查询异常都会中断整个查询，导致其它后端的健康水位无法返回。

修订内容：

- 改为逐后端查询并隔离异常；
- 返回所有成功后端的水位；
- 将失败后端及错误信息写入 `errors()`；
- 增加水位查询故障隔离回归测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
29 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`56f530f fix: isolate projection watermark failures`
