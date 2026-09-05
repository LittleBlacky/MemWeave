# 可扩展 Agent 记忆系统设计

## 1. 背景与目标

本设计面向个人助理、项目/研发 Agent、企业流程 Agent 等混合场景。系统需要同时支持会话连续性、跨会话记忆、历史经验沉淀，并能在长上下文、异步处理和多租户共享场景下保持可解释的一致性。

### 已确认的需求

- 先实现单机可运行版本，但数据模型和接口按多租户扩展设计。
- 同时提供统一核心接口、SDK 和独立记忆服务。
- 当前会话内要求读后写一致；跨会话允许最终一致。
- 作用域支持 session、user、project、team、tenant、global，并可按记忆类型配置共享策略。
- 输入来源覆盖对话、工具调用、用户显式操作和外部系统；每类来源可配置可信度与更新权限。
- 用户可查看、修改、删除记忆；支持项目/团队批量管理、审计和禁止记忆策略。
- 明确记住/修改/删除同步处理；普通事实和偏好自动提取；历史经验在任务完成或阶段性结束后异步归纳。
- 采用系统前置基础召回 + Agent 受控工具深挖的混合召回。
- 延迟按记忆类型配置：当前工作状态和明确变更低延迟，复杂提取和经验归纳异步。
- 第一版以文本和结构化数据为核心，原始事件预留文件/多模态对象引用。

## 2. 总体架构

采用事件溯源的分层混合架构。原始事件是唯一不可变事实源，其他存储都是可重建的投影或索引。

```text
用户 / Agent / 工具 / 外部系统
            |
            v
不可变事件日志（带 stream_id、seq、event_id）
            |
            +--> Session Projection：会话工作记忆
            +--> Durable Projection：长期事实与经验权威状态
            +--> Search Projection：向量/关键词/图索引

Agent 集成：Middleware + Context Provider + 受控 Tools/MCP
```

第一版使用 SQLite 作为关系型权威存储的本地实现；生产部署可使用 PostgreSQL 或其他关系数据库。会话、长期记忆、向量、关键词和图数据可以同时落在不同后端，但必须通过统一存储端口和投影协调器接入。队列先支持进程内/本地实现，接口保持可替换为 Redis Streams 或 Kafka。

记忆层采用多数据库协同，而不是单一数据库替换：

```text
事件日志/权威记忆：SQLite、PostgreSQL 或 MySQL
会话热点状态：关系库、Redis 或其他 KV Store
语义检索：Qdrant、Milvus、pgvector 等 Vector Index
关系检索：Neo4j、NebulaGraph、AGE 等 Graph Store
关键词检索：关系库 FTS、OpenSearch 等 Keyword Index
大对象：S3、MinIO 或本地对象存储
```

事件日志和关系型长期记忆是权威数据；向量、图、关键词和热点状态是可重建投影。一次写入先提交权威事务，再通过 Outbox 并行更新多个派生后端，不使用跨数据库两阶段提交。每个投影独立维护 watermark、重试状态和健康信息。

## 3. 记忆对象模型

### 3.1 记忆与事件分离

消息、Agent 输出、工具调用及结果首先形成不可变事件。记忆是从事件生成的、可检索的结构化状态对象，不是原始消息的简单拷贝。

```json
{
  "id": "mem_xxx",
  "kind": "decision",
  "scope": "project",
  "scope_id": "project_123",
  "key": "database.engine",
  "value": "PostgreSQL",
  "status": "active",
  "confidence": 0.96,
  "source": {
    "type": "user_conversation",
    "event_ids": ["evt_137"]
  },
  "valid_time": {"from": "2026-08-28T10:00:00Z", "to": null},
  "version": 2,
  "created_at": "...",
  "updated_at": "..."
}
```

### 3.2 内置记忆种类

| kind | 用途 | 示例 |
| --- | --- | --- |
| `working` | 当前会话正在使用的状态 | 当前目标、临时变量、未完成待办 |
| `profile` | 用户或组织的稳定属性 | 语言偏好、时区、团队 |
| `fact` | 可验证事实 | 项目使用 PostgreSQL |
| `decision` | 已确认方案和选择 | API 必须向后兼容 |
| `experience` | 从完整任务归纳的经验 | 某类超时应先检查连接回收 |
| `procedure` | 可复用步骤或技能 | 发布服务的标准流程 |

种类是注册表，可通过插件增加 `preference`、`policy`、`relationship` 等类型；Agent 主循环不因新增类型而修改。

### 3.3 作用域、状态和来源

作用域独立于种类：`session`、`user`、`project`、`team`、`tenant`、`global`。每条记录包含 `scope`、`scope_id`、`visibility_policy`，服务端从认证上下文注入身份和权限。

生命周期状态至少包括：

```text
candidate → active → superseded
                    ├→ retracted
                    └→ expired
session_only
needs_confirmation
```

事实和经验分开建模。事实回答“是什么”，经验回答“过去如何解决”；经验必须携带适用条件和证据，不能覆盖当前用户的新指令。

## 4. 事件与投影模型

### 4.1 不可变事件

每个输入、输出和工具结果先写入事件流：

```json
{
  "event_id": "evt_137",
  "stream_id": "tenant:t1/project:p1/session:s1",
  "seq": 137,
  "type": "user_message",
  "payload": {"text": "还是改用 PostgreSQL 吧"},
  "occurred_at": "...",
  "actor": "user:u123"
}
```

事件追加接口必须接收请求携带的 `protocol_version`，不能在存储层固定写入某个版本。持久化时将结构化的 `ProtocolVersion(major, minor)` 规范化为 `"major.minor"` 字符串；重复 `event_id` 或幂等键重放时，协议版本也属于不可变内容的一部分，版本不一致必须报告冲突。

同一 `stream_id` 内 `seq` 严格递增，`event_id` 全局唯一，事件追加后不可修改。

### 4.2 投影和存储平面

- **Session Projection**：同步维护当前会话工作记忆和最近上下文。
- **Durable Projection**：长期事实、经验和版本状态的权威投影。
- **Search Projection**：向量、关键词和图索引，属于可重建派生数据，可以同时写入多个后端。

长期权威存储只接受已经通过策略确认的 `active` 记录。`candidate` 和
`needs_confirmation` 保留在候选/审核流程中，不得在写入长期版本链时替代已有的
`active` 版本；`superseded`、`retracted` 和 `expired` 只能由对应的生命周期操作产生。
长期记忆的 `value` 使用严格 JSON 原生值，以保证数据库重启前后类型稳定；UUID、时间
对象和其它自定义类型必须由上层先规范化为字符串或 JSON 结构，不能由存储层隐式转换。

投影不等于数据库。一个投影可以使用多个数据库，一个数据库也可以承载多个投影；Core 通过存储端口和 `StorageCoordinator` 管理这种组合。

### 4.3 水位、版本和幂等

每个作用域记录 `session_watermark`、`durable_watermark`、`index_watermark`。长期记忆更新使用来源事件的 `source_seq`，而不是异步任务完成时间；更大的序号才能覆盖旧版本。每个任务携带 `event_id`、`idempotency_key`、`source_seq` 和尝试次数，重复投递只允许第一次生效。版本表按版本保存多行，因此同一作用域内的 `memory_id` 绑定关系由独立身份注册表维护；`memory_id` 不能跨 key 重用，同一 key 也不能更换 `memory_id`，历史冲突必须在迁移时显式失败。长期记忆写入还单独维护 `durable_memory_writes`，用 `(write_stream_id, write_event_id)` 绑定实际写入版本，并保存操作类型和完整请求指纹；`MemorySource.event_ids` 仅表示证据集合，允许跨版本复用，不能用于写入幂等判断。指纹不完整的历史写入身份必须拒绝重放。

事件投影的持久化 checkpoint 表示连续处理水位，而不是已见到的最大序号。启用 checkpoint 的 Dispatcher 遇到序号间隙时暂存事件，不推进水位；缺口补齐后按序投影并连续推进。未配置 checkpoint 的进程内 best-effort 分发不提供跨重启的乱序恢复保证。

严格 checkpoint store 除了保存和读取单个事件 receipt，还必须能够判断 receipt 是否连续覆盖
`1..N`。ProjectionRuntime 在恢复时先以 `min(checkpoint, backend watermark)` 作为有效水位读取
权威事件前缀，并逐条校验 receipt 的 `event_id` 和 fingerprint；receipt 缺失或身份不一致时保持
FAILED 并要求重放，即使事件流当前没有 `N+1` 的新事件也不能直接标记 READY。严格 receipt store
必须同时实现基础 checkpoint 的读取和单调保存能力。

同一 `(projection, stream_id)` 的 checkpoint 更新必须单调且并发安全：使用数据库条件更新保证较小序号不能覆盖较大水位，首次插入冲突或暂时性锁错误仅做有限重试。

进程内 `ProjectionDispatcher` 对每个 `(backend, stream_id)` 使用独立锁，串行化该 stream 的 pending 缓存、事件应用和 checkpoint 提交；不同后端或不同 stream 不共享此锁。后端注册应在开始投影前完成。

## 5. 记忆生命周期

### 5.1 显式操作（同步）

识别“记住、保存、修改、改成、忘掉、删除、撤销”等明确意图，同步生成结构化 `remember/upsert/forget` 操作，立即更新会话状态和长期权威表；向量和图索引异步更新。

### 5.2 普通事实和偏好（异步候选）

每轮结束后由规则或轻量模型提取候选，不直接标记为有效。策略引擎根据稳定性、未来复用价值、敏感级别、来源可信度和现有冲突决定：`promote`、`session_only`、`ask_confirmation` 或 `discard`。

候选在审核完成前不进入长期权威版本链；只有 `promote` 决策才将候选规范化为
`active` 记录并通过长期存储写入。

### 5.3 历史经验（任务完成后异步）

在任务完成、用户确认成功、阶段性检查点或会话空闲时，对完整事件情节进行归纳：目标、尝试、工具结果、最终结果、教训和适用条件。中间推测不直接晋升，经验保存证据事件范围。

### 5.4 更新、删除和敏感策略

更新产生新版本，旧版本标记 `superseded`；删除使用 `retracted` 或 `expired`，保留审计链，再按合规要求物理清理。敏感类别可配置为禁止保存、必须确认或短 TTL 严格权限。

## 6. 召回与 Agent 集成

### 6.1 前置基础召回

每轮先由 Memory Router 接收当前消息、任务目标、会话摘要、工作记忆、截断状态和策略配置，输出固定结构：是否召回、查询、记忆种类、作用域、Top-K 和 Token 预算。硬规则先行，模糊场景才调用轻量模型。

召回顺序为：

```text
会话工作记忆 → 长期权威事实表 → 向量索引 → 图/经验索引 → 必要时原始事件
```

结果经权限过滤、版本过滤、重排序、去重、压缩后注入上下文。当前用户明确陈述和更大的 `source_seq` 优先。

### 6.2 受控工具深挖

提供 `memory.search`、`memory.get`、`memory.remember`、`memory.forget`，通过工具网关执行。网关限制权限、查询次数、超时、Top-K、Token 预算并记录审计日志。`tenant_id`、`user_id`、`project_id` 等身份字段由服务端注入，Agent 不可自行传入。

### 6.3 长上下文处理

长上下文不直接等于需要召回。系统先压缩早期消息并检测信息缺口；若异步处理水位落后于上下文裁剪位置，则保留相关原始事件、提高处理并发、对关键事件同步补处理或启用短暂读取屏障。

## 7. 一致性、队列与恢复

一轮请求内的权威事务至少包含：写入原始事件、更新会话投影、记录显式操作、写入 outbox。事务提交后才向客户端返回，后台 Worker 从 outbox 并行投递到向量、图、关键词、KV 和对象存储；不要求这些派生后端参与同一个分布式事务。

读取支持 `eventual`、`session_consistent`、`durable_consistent` 三种模式，默认 `session_consistent`。等待长期水位超时后，使用会话投影并标记 `durable_lag=true`，不无限阻塞 Agent。

任务状态为 `pending → processing → applied`，失败可进入 `retryable` 或 `dead_letter`。权威表保留后，索引失败可通过重建恢复。并发更新采用乐观锁和版本比较；无法判定冲突时进入 `needs_confirmation`，不静默覆盖。

## 8. SDK、服务和插件边界

核心只依赖抽象接口：

```text
EventStore / SessionStore / DurableMemoryStore
VectorIndex / GraphStore / KeywordIndex / BlobStore
StorageCoordinator / RecallProvider / Extractor / Resolver / PolicyEngine / JobQueue
```

SDK 与独立服务共享领域模型，核心 API 包括：`append_event`、`recall`、`remember`、`forget`、`get`、`list`、`confirm`、`rebuild_index`。Agent 集成提供 Middleware、Context Provider 和 Tools/MCP 三种方式，默认启用 Middleware + Context Provider。

插件边界包括 MemoryKind、Extractor、Recall、Policy、Source Adapter、Index Adapter 和 Lifecycle Hook。插件不能绕过事件日志、权限、版本和审计。不同场景通过策略配置实现，不复制核心代码。

### 8.5 多数据库协同和存储扩展

`StorageCoordinator` 负责将一条权威记忆分发到多个存储后端，但不把这些后端混为一个事务。其最小能力包括：注册/注销后端、按记忆种类和作用域路由、写入 Outbox、读取各后端 watermark、报告健康状态，以及在索引损坏时从权威表重建。

Task 2 当前提供的 `ProjectionDispatcher` 负责进程内事件 fan-out，并可通过关系型 checkpoint store 持久化每个 `(projection, stream_id)` 的连续事件水位；`watermark(stream_id)` 和 `watermarks(stream_id)` 均按 stream 查询，不使用跨 stream 的全局整数。投影后端的 `watermark(stream_id)` 必须表示其已实际应用的连续水位；Dispatcher 在推进 checkpoint 前会用它抑制“投影副作用已成功但 checkpoint 保存失败”后的重复 apply。需要跨进程/重启去重时，后端必须持久化该水位或使用自身的 event_id 幂等 upsert。`StorageCoordinator` 作为兼容名称保留。Outbox 写入、失败重试、重启恢复和索引重建由后续任务实现，不能把当前分发器当作可靠投递组件。

Task 2 的公开存储和投影入口统一校验参数：非字符串标识符或非预期对象类型抛出 `TypeError`，空字符串或负序号抛出 `ValueError`。调用方不得依赖底层数据库或 Python 属性访问产生的 `AttributeError` 来判断请求错误。

事件追加还必须校验 `event_type`（`EventType` 或非空字符串）和 `idempotency_key`（`None` 或非空字符串），非法值在进入数据库事务前拒绝。

迁移状态读取只在 `schema_migrations` 表尚不存在时返回空列表；连接中断、权限错误、锁冲突或其它数据库异常必须向调用方传播，不能伪装成“尚未执行迁移”。

数据库启动并发执行迁移时，适配器对“表已存在”、数据库锁、死锁和序列化冲突重试整个迁移事务，并在达到有限次数后传播异常；单次迁移失败必须整体回滚，不能继续执行后续版本。

Dispatcher 的 per-stream gap 缓存必须有容量上限。达到 `max_pending_events` 后不再接收新的非连续缺口事件，但始终允许当前 `checkpoint + 1` 的补缺事件进入，以便缓存能够恢复并排空；overflow 通过 `errors()` 报告。除 per-stream 上限外，Dispatcher 还必须配置进程级 `max_pending_events_total`，限制所有 backend/stream pending 条目的总量。调用方应先从 EventStore 重放权威事件，再调用 `clear_pending(stream_id)` 清理旧缓存。该机制限制进程内存增长，不改变事件日志的事实源语义。

投影错误状态按 `(stream_id, backend)` 隔离并受锁保护；`errors()` 返回按 stream 分组的快照，`errors(stream_id)` 返回单个 stream 的 backend 错误。一次 stream 的成功投影不得清除其它 stream 的错误，成功重试只清除对应 backend/stream 条目。

错误状态必须随健康检查结果收敛：`watermarks(stream_id)` 或 `health()` 对同一 backend/scope 后续查询成功时清除旧错误，避免监控和 Runtime 读取到过期故障。

使用持久化 checkpoint 的应用必须通过独立的 `ProjectionRuntime` 管理恢复生命周期。Runtime 在 `RECOVERING` 状态从 `EventReplaySource` 回放 EventStore 中 checkpoint 之后的事件，并缓存期间到达的实时事件；回放和缓存排空完成后切换为 `READY`，失败则进入 `FAILED` 并拒绝继续投影。Adapter 不应直接绕过 Runtime 调用 Dispatcher；Dispatcher 本身仍只负责进程内事件 fan-out。

Runtime 的恢复 API 只依赖 `list_after(stream_id, seq)` 和 `last_seq(stream_id)` 两个抽象方法。恢复期间 `publish(event)` 进入 per-stream 缓冲，恢复成功后按序排空；应用启动契约必须先调用 `recover(stream_id)`，再把实时事件交给 `publish`。恢复起点由 Dispatcher 按该 stream 所有已注册投影的最小 checkpoint 计算（最慢投影优先），没有持久化 checkpoint 时从 `0` 开始，避免重复扫描完整历史的同时不漏投影事件。

推荐的职责边界如下：

| 存储端口 | 默认角色 | 一致性 | 可否作为唯一真相 |
| --- | --- | --- | --- |
| `EventStore` | 原始事件 | 提交后不可变 | 是 |
| `DurableMemoryStore` | 长期记忆、版本、tombstone | durable/session consistent | 是（记忆状态） |
| `SessionStore` | 当前会话工作状态 | session consistent | 仅当前会话 |
| `VectorIndex` | 语义发现 | 最终一致 | 否 |
| `GraphStore` | 关系和多跳发现 | 最终一致 | 否 |
| `KeywordIndex` | 精确/全文发现 | 最终一致 | 否 |
| `BlobStore` | 大文本和文件引用 | 最终一致或外部权威 | 由来源策略决定 |

一次记忆写入的标准流程是：

```text
权威事件 + Durable Memory + Outbox（同一事务）
                         ↓
       Vector / Graph / Keyword / KV / Blob 并行投影
```

召回必须在合并向量或图结果后回查作用域、版本和 tombstone；任何派生后端不可用时，系统仍可从 Session Projection 和 Durable Projection 返回降级结果。

### 8.1 框架无关 Memory Protocol

MemWeave 不假设 Agent 使用某一种运行时、模型供应商或编排框架。所有接入方先转换为统一的 `Memory Protocol` 事件和请求，再由 Memory Core 处理。协议只描述记忆相关语义，不接管 Agent 的规划、工具执行和模型循环。

标准事件至少包括：

```text
turn.started       一轮开始，携带 session、任务目标和当前上下文摘要
context.requested  Agent 即将组装模型上下文，请求基础召回
model.input        发往模型的消息或裁剪后的上下文
model.output       模型输出（包括工具调用意图）
tool.called        工具调用及参数摘要
tool.completed     工具结果及成功/失败状态
turn.completed     一轮完成，可触发异步提取
episode.completed  任务或阶段完成，可触发经验归纳
memory.command     显式 remember/update/forget/confirm 操作
```

协议请求中的身份、租户和作用域由 Adapter 从宿主 Agent 的可信运行时注入；客户端消息中同名字段一律视为不可信数据。每个请求带 `protocol_version`、`request_id`、`session_id`、`causation_id` 和能力声明，响应带 `watermarks`、`consistency`、`degraded` 和可审计的 `decision_id`。

### 8.2 Capability Negotiation

Adapter 启动时向 Core 声明能力：是否能在 turn 前后执行 Hook、是否能拦截模型请求、是否支持原生工具注册、是否能接收上下文补丁、是否能报告工具和 Episode 生命周期。Core 根据能力返回接入等级和保证集合；缺失能力不得被假设存在。

### 8.3 Agent Adapter 接入等级

```text
L1 Native Middleware
  before_turn/context_provider/after_turn Hook 可用。
  Core 自动完成基础召回、显式写入和 turn 结束事件。
  可提供 session_consistent 的最强保证。

L2 API Proxy
  只能拦截模型请求与响应，不修改 Agent 内部 loop。
  Core 自动注入上下文并记录模型事件；显式工具语义可能不完整。
  保证取决于代理覆盖范围，作为后续阶段实现。

L3 Tools/MCP
  Agent 通过 memory.search/get/remember/update/forget 工具主动调用。
  兼容性最高，但不保证 Agent 会主动调用；系统只保证工具调用本身的权限、幂等和版本语义。
```

每个 Adapter 必须实现以下契约：

```text
capabilities() -> CapabilitySet
start_turn(input: TurnInput) -> TurnHandle
provide_context(handle, ContextEnvelope) -> ContextAck
record_event(handle, ProtocolEvent) -> EventAck
finish_turn(handle, TurnOutcome) -> FinishAck
```

L1 Adapter 还必须实现显式命令短路：检测到用户明确记住/修改/删除时，在模型继续生成前调用 Core 的同步命令接口。L3 Adapter 的工具 schema 由 Core 生成，Adapter 不得自行放宽参数、作用域或 Token 限制。所有等级都必须透传 `request_id` 和幂等键，并在降级时报告缺失事件和一致性级别。

### 8.4 接入保证矩阵

| 能力 | L1 Middleware | L2 Proxy | L3 Tools/MCP |
| --- | --- | --- | --- |
| 自动基础召回 | 是 | 是（代理覆盖时） | 否，需 Agent 调用 |
| 显式记忆同步可见 | 是 | 部分，取决于拦截点 | 是，工具返回后 |
| 自动记录完整工具链 | 是 | 代理可见范围内 | 仅记录通过记忆工具的调用 |
| session_consistent | 是 | 需宿主配合 | 工具调用范围内 |
| Agent 无需修改 | 否，需 Hook | 是 | 是 |

## 9. 失败模式与可观测性

需要记录每次召回和写入的触发原因、规则/Router 决策、memory_id、source_seq、水位、延迟、超时、降级和工具调用次数。核心指标包括召回命中率、陈旧读取率、durable lag、提取成功率、索引延迟和记忆冲突率。

关键降级规则：Router 超时使用规则默认结果；向量索引不可用时查询权威表和会话状态；长期服务不可用时使用当前上下文和工作记忆；超预算只保留高优先级记忆；冲突显式标注。

## 10. 分阶段实现边界

本节是实施路线概览，用于划定阶段范围；具体的一致性、安全、作用域和验收约束见后续章节。

### 第一阶段：最小可靠闭环

1. 不可变事件日志和 seq。
2. 最近消息窗口与 Session Projection。
3. 显式记忆 CRUD 同步生效。
4. 长期事实权威表，包含来源、版本和状态。
5. Outbox 与可重试异步任务。
6. 基础规则召回和 Token 预算。
7. 框架无关 Memory Protocol 数据模型和版本校验。
8. L1 参考 Middleware/Context Provider，自动基础召回、显式同步写入和 turn 生命周期事件。
9. L3 Tools/MCP Adapter，提供受控 search/get/remember/update/forget 工具。
10. 本地 SDK 与最小 HTTP 接口；L2 Proxy 只定义契约，不在本阶段实现。

### 第二阶段：自动化提取与检索增强

1. 普通事实/偏好候选提取和晋升策略。
2. 向量索引适配器与混合重排序。
3. L2 API Proxy Adapter 和更多宿主框架适配器。
4. 向量/图检索及混合重排序。
5. 用户确认、审计和敏感策略。

### 第三阶段：经验和多租户能力

1. 历史经验/程序性记忆归纳。
2. 图索引插件和多跳召回。
3. 多租户权限、团队共享和外部 Source Adapter。
4. 水位监控、积压保护和索引重建运维能力。

## 11. 设计原则总结

```text
原始历史保证不丢
会话投影保证当前连续
长期权威表保证版本和一致性
向量/图索引负责发现相关内容
经验流水线负责事后归纳
Agent 只通过受控接口参与
```

## 12. 受控自进化设计

系统支持三层自进化，但三层的权限和验证要求不同。

### 12.1 第一层：记忆内容进化

这是默认支持的能力。系统根据新事件、任务结果和用户反馈自动完成记忆的新增、更新、失效和经验归纳：

```text
对话/任务结果 → 候选记忆 → 验证与冲突处理 → 新版本记忆
```

事实、偏好、决策和经验均保留来源证据、置信度、适用范围和版本；模型推断不能作为高风险记忆的唯一证据。

### 12.2 第二层：召回与提取策略进化

系统收集召回命中、用户纠正、任务成功率、陈旧读取率、冲突率和记忆使用频次，由 `Evaluator` 评估策略效果，`Policy Optimizer` 生成策略候选。候选策略必须经过离线回放、影子运行或灰度验证后才能激活。

策略版本保存于 `Policy Registry`，支持按 Agent、项目、租户配置和回滚。系统可以调整召回阈值、记忆类型权重、晋升条件、确认要求和检索器组合，但不能绕过权限、审计、版本和敏感信息规则。

### 12.3 第三层：能力进化

修改 Prompt、工具描述、提取器、召回算法、模型或生产代码属于能力进化，不由记忆内核直接执行。它应通过独立的优化流水线完成：

```text
生成候选 → 沙箱测试 → 基准评估 → 安全检查 → 人工/自动审批 → 灰度发布 → 可回滚
```

记忆系统只能提供事件、反馈和评估数据，不能直接获得修改生产代码或模型的权限。

### 12.4 防止错误自强化

必须防止“错误推断被反复召回后置信度越来越高”的闭环。约束包括：

- 用户明确事实优先于模型推断。
- 经验必须关联任务结果、工具输出或用户确认。
- 重复召回本身不能无限提升置信度。
- 重大策略变化需要评估或确认。
- 记忆、策略和能力版本均可审计、回滚。

因此，本系统的自进化定义为：**可观测、可评估、可回滚的受控进化**，而非 Agent 自由修改自身。

## 13. 跨会话并发与版本语义

seq 只保证单个事件流内的顺序，不能直接比较来自不同会话的项目级更新。系统使用三种版本维度：

~~~text
session_seq：同一会话内的事件顺序
memory_version：同一 memory_key 的版本
scope_revision：项目/团队/租户共享状态的并发修订号
~~~

同一个 memory_key 的更新和删除都使用乐观锁：提交必须携带读取到的 memory_version；版本不匹配则拒绝操作（更新可重新读取并运行 Resolver，删除不得静默重试）。跨会话同时修改时，不按请求完成时间判断新旧，而按来源可信度、用户确认、有效时间和冲突策略处理。无法安全合并时，生成 conflict 记录并进入 needs_confirmation。
长期更新和删除还必须携带真实的来源位置 `source_stream_id + source_seq`；存储层不自动推测来源序号，缺失来源元数据的操作必须先回到事件编排层补齐。同一 stream 内按 `source_seq` 判断先后，跨 stream 不直接比较序号，而由 `expected_version`/CAS 和 Resolver 处理。
同一 `source_event_id` 的重放必须携带相同的来源 stream、目标版本和来源序号；参数变化的重放视为冲突，不能仅凭事件 ID 返回历史结果。
`MemorySource.event_ids` 表示可跨版本复用的证据集合，不承担写入幂等身份；创建、更新和删除操作应通过显式 `source_event_id` 标识本次写入事件。

## 14. 事务与跨系统一致性边界

同一权威数据库内的以下操作使用一个本地事务：

~~~text
追加原始事件
更新 Session Projection
写入显式记忆操作
写入 Outbox
~~~

外部系统、队列、向量库和图数据库不参与该事务。跨系统交付采用 Outbox + 至少一次投递 + 幂等消费，系统承诺最终一致，不承诺跨系统 exactly-once。

幂等消费由每个 Worker 的 `consumer_id` 和任务 `idempotency_key` 共同确定。Worker 在调用 handler 前持久化消费 receipt；已标记为 `applied` 的 receipt 在重投递时直接跳过，handler 失败则释放 receipt 以便重试，处理中断后由租约过期恢复。receipt 只能避免已确认完成的重复调用，无法覆盖“外部副作用已发生但 receipt 尚未提交”的崩溃窗口，因此 handler 仍必须使用相同幂等键实现自身去重，或将副作用与 receipt 放进同一权威事务。

外部来源接入必须记录 source_event_id 和同步游标；重复同步通过幂等键消除，源系统暂时不可用时不阻塞当前会话。

## 15. 权威来源与冲突策略

每个 kind/key/scope 可以配置 canonical_source 和允许写入的来源集合：

~~~text
用户偏好        canonical_source = explicit_user
员工部门        canonical_source = hr_system
项目技术决策    canonical_source = project_owner_or_user_confirmation
排障经验        canonical_source = validated_episode
~~~

来源优先级默认如下：

~~~text
用户明确操作 > 权威业务系统 > 工具结果 > 多轮提取 > 单轮推断
~~~

低优先级来源不能静默覆盖 canonical source。冲突记录同时保存各版本、来源和证据；召回时只注入有效版本，并在需要时向 Agent 暴露“存在冲突”的提示。

## 16. 默认提取与晋升策略

第一版采用以下默认策略，所有阈值均可由租户/项目策略覆盖，但不得关闭权限、审计和敏感信息规则：

| 输入情形 | 默认结果 |
| --- | --- |
| 明确“记住/保存” | 同步写入 active |
| 明确修改或撤销 | 同步生成新版本/撤销事件 |
| 项目硬约束或已确认决策 | 高置信度候选；用户确认后长期激活 |
| 稳定偏好重复出现至少 2 次 | 候选晋升为长期记忆 |
| 单轮普通事实 | 默认 session_only，置信度达到 0.85 才进入候选 |
| 单轮模型推断 | 不自动晋升 |
| 敏感类别 | 默认禁止；允许时必须确认并设置 TTL |

候选对象必须包含 evidence_event_ids、confidence、sensitivity、future_value 和 stability。置信度不能仅因被重复召回而提升。

## 17. Episode 生命周期与历史经验

历史经验以显式 Episode 作为边界，不直接从任意单轮消息生成：

~~~text
episode_started → episode_progressed → episode_completed
                                  ├→ episode_abandoned
                                  └→ episode_timed_out
~~~

上层 Agent 或工作流负责在任务开始、完成、放弃时发出 Episode 事件；没有显式完成事件时，只能生成低置信度候选。经验提取器读取 Episode 内完整事件、工具结果、错误和用户反馈，输出目标、尝试、结果、教训和适用条件。

只有满足以下条件之一才允许激活经验：

~~~text
用户确认成功
工具/测试结果验证
同类任务多次得到相同结论
~~~

## 18. 长上下文保留与 Pending Overlay

当异步处理水位落后于上下文裁剪位置时，系统为未提交事件建立 pending_overlay：

~~~text
pending_overlay = {stream_id, from_seq, to_seq, event_refs, expires_at}
~~~

Overlay 保留尚未完成语义解析的关键原文或摘要，并在每轮召回阶段参与合并。只有当长期权威投影达到对应水位且索引任务完成或可重建时，才能清理 Overlay。

对明确记住、修改、删除和安全策略事件，Overlay 不得被普通滑动窗口裁剪；对普通低价值事件，可按 TTL 清理。

## 19. 删除权、审计与派生索引清理

系统区分“不可变审计事件”和“用户内容删除”：

~~~text
记忆逻辑撤销       → active 变为 retracted
派生索引清理       → 删除向量/图/缓存中的内容
合规物理删除       → 对事件 payload 执行加密擦除或受控重写
审计保留           → 只保留删除动作、主体、时间和对象 ID，不保留敏感原文
~~~

删除操作必须产生可追踪的 tombstone，并传播到所有投影和索引。删除任务幂等、可重试；在删除完成前，召回层先通过 tombstone 屏蔽相关内容。

## 20. API、安全与版本约定

所有 SDK、HTTP 和 MCP 接口带有显式 API 版本，例如 /v1/recall。响应包含 consistency、watermarks、degraded 和 request_id。

权限上下文由服务端认证层注入，至少包括：

~~~text
tenant_id / user_id / agent_id / project_id / roles
~~~

记忆读取遵循最小权限；跨作用域访问必须有策略授权。日志中禁止写入原始敏感内容，管理操作需要审计主体和原因。

## 21. 作用域继承与合并规则

作用域默认形成有序优先级，但不是所有记忆都必须使用同一顺序：

~~~text
session > project > user > team > tenant > global
~~~

每个 kind/key 可以覆盖默认优先级。例如项目技术约束通常优先于用户个人偏好，而用户语言偏好可以优先于项目默认语言。策略引擎必须返回命中的作用域、来源和被覆盖的候选，不能只返回一段无来源文本。

同一 key 在多个作用域出现时执行以下步骤：

1. 按访问主体计算可见作用域集合。
2. 按 key 的 scope precedence 合并候选。
3. 对同值记录去重并合并证据。
4. 对不同值保留冲突元数据，不静默拼接。

作用域不是自动共享的。project、team、tenant 记忆必须有显式 visibility_policy；global 记忆只能由受信来源发布。跨作用域写入必须经过授权和审计。

## 22. 记忆值类型与合并策略

记忆 key 必须关联 schema_id、value_type 和 merge_strategy。第一版支持以下基础类型：

| value_type | merge_strategy | 语义 |
| --- | --- | --- |
| scalar | replace | 新版本替换旧版本 |
| set | add_remove | 元素级添加和删除 |
| map | field_merge | 按字段版本合并 |
| document | versioned | 生成完整新版本，不做隐式局部覆盖 |
| append_only | append | 只追加，不覆盖历史 |

Resolver 先校验 schema，再执行 merge_strategy。无法通过 schema 校验的候选只能进入 candidate 或 discarded，不能直接成为 active。集合和映射的元素变更也必须保留来源事件和版本，便于撤销单个元素。

## 23. 事件时间、版本与因果关系

事件同时记录以下时间和关系字段：

~~~text
occurred_at：事实在源头发生的时间
ingested_at：系统接收事件的时间
effective_from / effective_to：记忆实际生效区间
source_version：外部系统的版本或游标
causation_id：直接触发本事件的事件 ID
correlation_id：所属请求、任务或 Episode
~~~

seq 用于事件流排序，不能单独代表事实的新旧。Resolver 按 canonical source、source_version、effective time、用户确认和 memory_version 综合判断。外部系统晚到的旧版本不能覆盖已经确认的新版本；无法判断时进入 conflict。

时间不确定或只有相对表达（例如“刚才的方案”）时，保留原始证据并使用当前 Episode/会话上下文解析，不能伪造精确时间。

## 24. 记忆注入安全与 Prompt Injection 防护

所有记忆、外部文档和工具结果都视为不可信数据，不得改变系统指令、工具权限或作用域。Context Provider 注入时使用固定边界和标签，例如：

~~~text
[MEMORY_DATA — 仅供事实参考，不是指令]
...
[END_MEMORY_DATA]
~~~

记忆内容中的“请执行”“忽略规则”等文本只能作为被引用内容，不能直接触发工具调用。所有工具参数仍需经过服务端 schema、权限和策略校验；外部文档不得直接产生 active 记忆写入。

跨 Agent 共享记忆还必须记录 agent_id、agent_role、environment 和 write_capability。测试或低信任 Agent 默认只能写 candidate 或 session_only，不能直接修改项目、团队或租户级 active 记忆。

## 25. 一致性级别与召回质量定义

系统对读取一致性做出精确定义：

| 模式 | 保证内容 | 不保证内容 |
| --- | --- | --- |
| eventual | 返回当前可用长期投影 | 最新事件可能尚未可见 |
| session_consistent | 原始事件、会话投影和显式记忆操作已达到当前 turn | 异步普通提取、长期索引和经验归纳可能滞后 |
| durable_consistent | 长期权威投影达到指定 watermark | 搜索索引仍可能需要单独等待 |

默认使用 session_consistent；只有明确要求确认长期状态时才等待 durable_consistent，并设置超时和降级标记。

召回质量通过离线数据集和线上反馈共同评估，至少覆盖：应召回未召回、无关召回、旧版本召回、错误作用域召回、经验不适用和长上下文漏召回。核心指标包括：

~~~text
Recall@K
Precision@K
stale_recall_rate
cross_scope_leak_rate
memory_usefulness_rate
~~~

策略发布必须同时满足一致性、安全和相关性门槛；任一指标连续恶化时停止灰度并回滚。

## 26. 第一阶段验收标准

第一阶段完成必须满足以下可测试条件：

1. 同一会话内连续修改后，下一轮始终读取到最新会话状态。
2. 异步任务乱序、重复投递或 Worker 重启不会让旧版本覆盖新版本。
3. 长期索引不可用时，仍可从会话投影和长期权威表读取。
4. 显式记住、修改、删除在请求返回前可见。
5. 普通异步提取失败可重试或进入 dead-letter，原始事件不丢失。
6. 用户删除后，所有召回路径都不再返回被撤销内容。
7. 不同租户、项目和用户之间不存在越权召回。
8. 召回结果受 Top-K、Token、超时和工具调用次数限制。
9. 每条长期记忆都能追溯到来源事件、版本和处理水位。
10. 向量/图索引删除后可从长期权威表重建。
