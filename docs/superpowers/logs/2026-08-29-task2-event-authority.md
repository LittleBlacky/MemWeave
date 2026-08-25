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

## checkpoint 并发更新审查修订

审查发现 `RelationalProjectionCheckpointStore.save_max()` 采用“先读后插入/更新”，并发初始化时可能触发唯一约束异常；并发更新时低序号也可能覆盖高水位。

修订内容：

- 使用 `last_seq < seq` 条件更新，保证水位不会回退；
- 无记录时执行插入，遇到唯一约束竞争重新读取当前值；
- 对数据库锁、死锁、序列化失败和唯一冲突执行有限指数退避；
- 增加 8 线程并发初始化 checkpoint 的回归测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py -q
34 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`d06274a fix: make projection checkpoints concurrency safe`

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

## 投影 checkpoint 持久化审查修订

审查发现投影水位只保存在后端对象内存中，进程重启后无法判断某个 `(projection, stream_id)` 已经处理到哪里，重复投影或恢复处理没有可靠依据。

修订内容：

- 新增 `ProjectionCheckpointStore` 端口；
- 新增 `RelationalProjectionCheckpointStore`，复用 `projection_watermarks` 表；
- `ProjectionDispatcher` 成功应用事件后保存最大水位；
- 重建 dispatcher 后，已达到 checkpoint 的事件会被跳过；
- checkpoint 具有单调性，旧序号不能回退新水位；
- 明确 Outbox 仍负责持久化任务、失败重试和 worker 重启恢复。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
30 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`70c6cc1 feat: persist projection checkpoints`

## 投影健康查询审查修订

审查发现投影协调层没有统一的健康查询接口，调用方无法获得各后端可用性；单个后端检查异常时也不应阻断其它后端状态返回。

修订内容：

- 新增 `ProjectionDispatcher.health()`；
- 逐后端执行健康检查；
- 健康检查异常的后端返回 `False` 并记录错误，其它后端状态照常返回；
- 增加健康查询故障隔离回归测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_events.py tests/test_storage_ports.py tests/test_models.py tests/test_protocol.py -q
32 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`e322cde feat: add projection health reporting`

## ProjectionRuntime 重启恢复审查修订

审查发现乱序 gap 事件只保存在 Dispatcher 内存中，进程重启后 pending 丢失，后续事件无法补齐 checkpoint。为保持 Dispatcher 单一职责，新增独立 `ProjectionRuntime`：

- 通过 `EventReplaySource` 从 EventStore 回放 checkpoint 之后的已提交事件；
- `RECOVERING` 状态缓存实时事件，恢复成功后按序排空并切换 `READY`；
- 回放异常进入 `FAILED`，拒绝绕过 Runtime 继续发布；
- Adapter 只依赖 Runtime 的生命周期 API，不复制恢复逻辑。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
39 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`0943ab1 feat: add projection runtime recovery`

## 多 stream watermark 语义审查修订

审查发现事件 `seq` 和持久化 checkpoint 都按 `stream_id` 隔离，但 `ProjectionBackend.watermark()` 只有无参数的单一整数，无法同时准确表达多个 stream 的投影进度。

修订内容：

- 将投影后端契约改为 `watermark(stream_id)`；
- 将 `ProjectionDispatcher.watermarks()` 改为 `watermarks(stream_id)`；
- 每个 stream 独立报告投影水位，避免不同 stream 之间互相覆盖或误判；
- 增加多 stream 水位隔离回归测试；
- 全局 offset 不纳入 Task 2，后续若需要必须作为独立日志游标建模。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py -q
35 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`d12c155 fix: scope projection watermarks per stream`

## Dispatcher 并发投影审查修订

审查发现 `ProjectionDispatcher.project()` 共享 pending 缓存但没有并发保护。两个线程同时处理同一 backend 和 stream 时可能重复调用 `apply()`、竞争删除 pending 事件或产生不一致的水位结果。

修订内容：

- 增加 `(backend_name, stream_id)` 级别的独立可重入锁；
- 将该 key 的 pending、事件应用和 checkpoint 提交串行化；
- 不同 backend 或不同 stream 仍可并行投影；
- 明确后端注册应在开始投影前完成；
- 增加阻塞 handler 的并发回归测试，验证同一 key 不会同时进入 `apply()`。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py -q
36 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

本次修订提交：`51d68a5 fix: serialize projection dispatch per stream`

## 乱序事件 checkpoint 审查修订

审查发现 Dispatcher 以“已见到的最大序号”推进 checkpoint。若先收到 `seq=3`，checkpoint 会跳到 3，之后到达的 `seq=1/2` 会被误判为已处理，造成事件永久漏投影。

修订内容：

- 启用持久化 checkpoint 时，将水位定义为连续处理序号；
- 序号存在间隙时暂存事件，不推进 checkpoint；
- 缺口补齐后按连续序号依次调用投影器并 drain 暂存事件；
- 未配置 checkpoint 的 best-effort 进程内分发保持原有语义，不提供跨重启乱序恢复保证；
- 增加乱序 `seq=3 → 1 → 2` 回归测试，验证投影顺序和连续水位。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py -q
33 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

## Projection 恢复起点优化

问题：`ProjectionRuntime.recover()` 每次都从 `seq=0` 读取事件。长会话重启时会重复扫描已完成投影的历史，恢复耗时随整个事件流增长。

决策：新增 `ProjectionDispatcher.replay_from(stream_id)`，读取所有已注册投影在该 stream 的 checkpoint 并取最小值。最慢投影决定回放起点，既避免无谓的全量扫描，又保证任何投影不会因从最大水位开始而漏事件；未配置 checkpoint 或没有 backend 时返回 `0`。

TDD：RED 测试验证恢复起点错误地为 `0`；GREEN 实现接口并让 Runtime 使用该起点后通过。新增覆盖最慢 checkpoint、无 checkpoint 和空 stream 校验的回归测试。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
42 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`e253b5b fix: resume projection recovery from checkpoints`

## 幂等投影故障窗口修复

问题：投影 `apply()` 成功后 checkpoint 保存失败时，重试会再次执行同一事件。该行为对非幂等后端可能产生重复记忆或重复外部副作用。

决策：复用已有 `ProjectionBackend.watermark(stream_id)` 契约，将其定义为后端已实际应用的连续水位。启用 checkpoint 的 Dispatcher 在调用 `apply()` 前先检查后端水位；若候选事件序号已经被后端应用，则跳过副作用，仅补写 checkpoint。需要跨进程/重启去重的后端必须持久化自身水位或按 event_id 实现幂等 upsert；Task 2 不宣称任意外部副作用 exactly-once。

TDD：新增故障注入测试，第一次 checkpoint 写入失败，第二次投影只恢复 checkpoint 且 backend 事件列表仍只有一条。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
43 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`10ee955 fix: suppress duplicate projection side effects`

## 公共接口输入校验修复

问题：EventStore、ProjectionDispatcher 和 checkpoint store 对非法类型、空标识符和非法序号的校验不一致，可能向调用方暴露 `AttributeError` 或静默接受错误参数。

决策：统一公开入口错误语义：非字符串/非预期对象类型抛 `TypeError`，空字符串和负序号抛 `ValueError`。补充 EventStore 的 stream/payload/seq、Dispatcher 的 event/backend name，以及 checkpoint store 的 projection/stream/seq 校验。

TDD：先新增非法参数测试并确认现有实现失败，再加入最小校验实现；聚焦测试 3 passed，Task 2 相关回归测试 34 passed。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
46 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`04cf4f2 fix: validate storage and projection inputs`

## MigrationRunner 异常边界修复

问题：`MigrationRunner.applied()` 捕获所有异常并返回空列表，数据库连接失败或 SQL 错误会被误报为“没有已应用迁移”。

决策：仅对 SQLAlchemy 操作错误中明确表示 `schema_migrations` 表不存在的情况返回 `[]`；其它 `OperationalError` 继续抛出，非 SQLAlchemy 异常也不再被吞掉。

TDD：新增缺表正常路径和连接异常传播测试；先确认异常测试失败，再实现窄化捕获。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_recovery.py -q
36 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`e9b904f fix: preserve unexpected migration errors`

## Pending gap 缓存上限与清理

问题：启用 checkpoint 时，乱序事件会暂存在 per-stream pending 字典；如果缺失序号永久不到达，缓存会无限增长并长期占用进程内存。

决策：`ProjectionDispatcher` 新增可配置的 `max_pending_events`（默认 10,000）。达到上限后拒绝新的缺口事件并通过 `errors()` 报告 overflow，避免无界内存增长；新增 `clear_pending(stream_id)` 显式清理缓存。清理前必须先从 EventStore 重放权威事件，清理只影响内存缓存，不删除事实事件。

TDD：先新增容量上限、overflow 报告和显式清理测试并确认构造函数缺少参数而失败，再实现最小限制与清理接口。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
50 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`b2d0f1e fix: bound projection gap buffers`

## Projection 错误状态按 stream 隔离

问题：`ProjectionDispatcher.project()` 每次调用都会重置全局 `_errors`，并且错误只按 backend 名称索引。多 stream 并发或连续投影时，一个 stream 的成功调用可能清除另一个 stream 的失败信息。

决策：错误状态改为按 `stream_id -> backend -> message` 保存，并使用独立锁保护；`errors()` 返回分组快照，`errors(stream_id)` 返回单 stream 视图。健康检查使用 `__system__` scope，水位查询按对应 stream 记录。成功重试只清除对应 stream/backend 条目。

TDD：新增跨 stream 错误保留测试，并更新健康、水位和 pending overflow 测试断言；先确认旧实现失败，再完成最小实现。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
51 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`0307db0 fix: isolate projection errors by stream`

## Projection 错误状态恢复清理

问题：水位或健康检查失败后，后续检查成功不会清除旧错误，`errors()` 会长期返回已经恢复的故障。

决策：`watermarks(stream_id)` 成功时清除对应 stream/backend 错误；`health()` 成功时清除 `__system__`/backend 错误。清理仍使用错误状态锁，不影响其它 scope 或 backend。

TDD：新增水位失败后恢复、健康检查失败后恢复测试；旧实现 RED，增加成功分支清理后 GREEN。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
58 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`4233491 fix: clear recovered projection errors`

## 事件类型与幂等键输入校验

问题：`EventStore.append()` 未校验 `event_type` 和 `idempotency_key`；`event_type=None` 会暴露数据库 NOT NULL 异常，空事件类型会被写入，非字符串幂等键会依赖数据库隐式类型转换。

决策：追加事务前校验 `event_type` 必须是 `EventType` 或非空字符串；`idempotency_key` 必须是 `None` 或非空字符串。非法类型抛 `TypeError`，空字符串抛 `ValueError`。

TDD：新增四个非法字段测试；旧实现 RED，增加前置校验后 GREEN。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
59 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`4a85432 fix: validate event type and idempotency key`

## Pending 满载时允许补缺事件

问题：pending 达到容量上限后，当前实现无差别拒绝新事件；当真正缺失的 `checkpoint + 1` 到达时也会被拒绝，导致 gap 无法自行填补，只能清空后全量重放。

决策：容量限制只作用于新的非连续缺口事件。当前期待的 `checkpoint + 1` 始终允许进入 pending 并触发 drain，保证满载缓存仍可恢复；异常情况下缓存最多短暂超过配置值一个补缺事件，随后按序排空。

TDD：新增满载后依次补齐 `seq=1`、`seq=2` 的回归测试；旧实现 RED，放宽补缺事件条件后 GREEN。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
53 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`cfc15d9 fix: allow gap filling at pending capacity`

## 进程级 Pending 总量上限

问题：per-stream `max_pending_events` 只能限制单个 stream；大量异常 stream 同时出现永久 gap 时，所有 pending 字典合计仍可能耗尽进程内存。

决策：新增 `max_pending_events_total`（默认 100,000），用受锁保护的全局计数限制所有 backend/stream 的 pending 条目总量。达到总量上限时拒绝新的非连续缺口事件；当前 `checkpoint + 1` 的补缺事件仍允许进入，以保证恢复路径不会被限流锁死。`clear_pending()` 同步维护总计数。

TDD：新增多 stream 总量上限和清理后重新接收测试，以及非法总量配置测试；旧实现 RED，加入全局计数与限制后 GREEN。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
55 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`9727c40 fix: cap pending events process-wide`

## 并发迁移初始化修复

问题：多个 Worker 同时调用 `SQLAlchemyDatabase.apply_migrations()` 时，`checkfirst=True` 的检查与建表不是原子操作；并发初始化会出现 `table schema_migrations already exists`，导致部分实例启动失败。

决策：数据库适配器对整批迁移事务增加有限指数退避重试，覆盖表已存在、数据库锁、死锁和序列化冲突。失败事务整体回滚后重新读取已提交迁移版本，非可重试异常继续传播；SQLite 适配器同步暴露相同的重试配置。

TDD：新增 8 线程并发 `apply_migrations()` 测试；旧实现出现建表竞态异常，增加整批重试后仅一个线程执行全部迁移，其余线程返回空列表。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
56 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`6b2f921 fix: retry concurrent database migrations`

## ProjectionRuntime 按 stream 检查错误

问题：错误状态改为按 stream 分组后，`ProjectionRuntime._raise_on_dispatch_error()` 仍查询全局 `errors()`；一个 stream 的失败会误使另一个健康 stream 的恢复进入 `FAILED`。

决策：Runtime 恢复阶段改为调用 `errors(stream_id)`，只检查当前正在回放的 stream。其它 stream 的失败继续保留并可独立处理。

TDD：新增“failed stream 不影响 healthy stream recovery”回归测试；旧实现失败，改为按 stream 查询后通过。

验证：

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_storage_ports.py tests/test_events.py tests/test_models.py tests/test_protocol.py tests/test_recovery.py -q
52 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

代码提交：`efa05c5 fix: scope recovery errors to stream`
