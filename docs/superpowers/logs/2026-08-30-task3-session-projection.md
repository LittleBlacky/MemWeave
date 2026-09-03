# Task 3 开发日志：Session Projection 和显式命令策略

日期：2026-08-30

## 当前目标

在 Task 2 事件权威层之上实现会话工作记忆投影和显式记忆命令解析。显式命令
同步生效；普通事实提取、长期记忆和搜索索引不在本阶段实现。

## 第一阶段实现

- 新增 `SessionState`，保存 `session_id`、`last_seq`、最近消息和活动记忆。
- 新增 `SessionStore`，使用关系数据库持久化会话快照，支持数据库重启后读取。
- `apply_event()` 按事件序号拒绝旧事件覆盖新会话状态，并限制最近消息数量。
- `upsert_active()` 按 memory key 和 `source_seq` 更新 session-scoped 活动记忆。
- 新增 `ParseContext` 和 `ExplicitOperationParser`，支持中文/英文 remember、
  update、forget，模糊文本返回空操作。
- 会话 stream 支持从 `session:<id>` 以及带前缀的复合 stream ID 提取 session ID。

## TDD 记录

- RED：`tests/test_session_consistency.py` 因 `memweave.session` 和
  `memweave.policy` 不存在而无法收集。
- GREEN：实现两个模块后，现有四个 Session Consistency 测试全部通过。

## 验证

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_session_consistency.py -q
4 passed
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

## 当前边界

- 会话表由 `0004_session_states` 版本化 migration provision；SessionStore 不再隐式
  建表。自定义数据库适配器需要先执行统一 migration runner。
- 显式 parser 当前只生成结构化操作，不负责执行操作或写入长期权威表。
- Outbox、Durable Memory Store、Recall 和 Adapter 由后续 Task 实现。

## 可注册 CommandSpec/ParserRule

- 将固定正则命令改为声明式 `CommandSpec` 和编译后的 `ParserRule`；
- `ExplicitOperationParser.register()` 支持运行时注册新别名和 assignment/key 语法；
- 默认中文/英文 remember、update、forget 规则保持不变；
- 重复别名和不支持的语法会在注册时拒绝；
- 通过自定义“保存”命令的注册与解析回归验证。

验证：`tests/test_session_consistency.py` 的 4 个原有测试通过；另行验证自定义
“保存”命令注册后可正确解析。

## 显式操作同步投影

日期：2026-08-31

- 新增 `SessionStore.apply_operation()`，将可信边界传入的
  `MemoryOperation` 同步应用到当前 session 的活动记忆；
- `REMEMBER`/`UPDATE` 生成 session-only working record，`FORGET` 从当前活动投影移除；
- `source_seq` 用于拒绝旧写入，`expected_version` 用于 compare-and-swap 更新；
- 使用 `source_event_id` 识别重复投递；同一序号不同内容抛出 `StaleWriteError`，不静默覆盖；
- 操作仅接受 session scope，身份和最终来源序号仍由可信适配器传入，不能从用户文本获得。

TDD 验证：新增 `tests/test_session_operations.py`，覆盖同步 remember/update/forget、
版本冲突、重复投递和同序号内容冲突；Task 3 相关测试共 8 passed。

当前边界：该方法仍只更新会话投影；长期权威记录、墓碑、Outbox 同事务编排分别由
Task 4/5/7 负责。

## 事件先落库与命令重放

日期：2026-08-31

- 新增 `SessionCommandCoordinator.append_explicit()`，统一执行“EventStore 先追加，
  SessionStore 后投影”的显式命令流程；
- `memory.command` 事件 payload 保存结构化 `MemoryOperation`，SessionStore 可从事件
  自主重建操作，不依赖调用进程中的临时对象；
- `SessionStore.apply_event()` 在同一个本地事务中完成命令操作、最近事件和 watermark
  更新；操作校验失败时事务整体回滚；
- 投影失败不会删除已提交事件，调用方可以从 EventStore 读取事件并重放；重复重放由
  session watermark 和事件来源实现幂等；
- coordinator 在追加前校验 operation scope 与 stream session 一致，防止写入幽灵 session。

TDD 验证：新增 `tests/test_session_coordinator.py`，覆盖 EventStore 失败、投影失败后
重放、重复命令、作用域不匹配和非法命令回滚；Task 3 相关测试 14 passed，全量测试
98 passed。

当前边界：本实现选择事件先落库和可恢复投影，不引入跨数据库分布式事务；Outbox、
长期权威记忆和自然语言提取仍由后续 Task 负责。

## source_seq 与 session watermark 约束

日期：2026-08-31

- 明确 `SessionState.last_seq` 是事件投影水位，`MemoryRecord.source_seq` 是记忆来源
  事件序号，两者不是同一个版本字段；
- 低层 `SessionStore.apply_operation()` 现在要求 source event 已先投影，拒绝
  `source_seq > last_seq` 的绕过式写入；
- `memory.command` 由 `apply_event()` 在推进 watermark 的同一事务内应用，因而不受
  该低层入口限制；
- 新增测试验证未投影源事件不能直接创建 session memory，正常事件先投影后操作仍可用。

TDD 验证：Task 3 相关测试 15 passed。

## 严格收敛 Task 3 事件水位

日期：2026-08-31

- `SessionStore` 只接受 `last_seq + 1` 的下一个事件，不再提供宽松模式开关；
- 缺口事件不得推进 session watermark，也不得写入最近事件或活动记忆；
- 乱序缓存和恢复职责明确归属 `ProjectionDispatcher/ProjectionRuntime`，
  SessionStore 不再作为缺口缓存；
- 乱序旧适配器必须迁移到 `ProjectionDispatcher/ProjectionRuntime`，不能绕过严格
  SessionStore 契约。

## 缺口恢复与读取屏障

日期：2026-08-31

- 新增 `SessionProjectionBackend`，将严格 SessionStore 接入 Task 2 的 Dispatcher；
  Dispatcher 负责 pending gap，SessionStore 只应用连续事件；
- 新增 `SessionReadBarrier` 和 `SessionReadResult`；读取发现 session 水位落后于
  EventStore 目标水位时，主动调用 `ProjectionRuntime.recover()` 尝试补齐；
- 恢复失败不把旧状态伪装成最新状态，结果带有 `lagging=True`、`degraded=True`
  和错误信息，供 Adapter 决定重试或降级；
- 正常恢复后返回 `requested_seq == applied_seq`，保证读取到的状态已追平目标事件。

TDD 验证：新增 `tests/test_session_read_barrier.py`，覆盖 Dispatcher 缺口缓存、读取
自动恢复和恢复无法覆盖目标时的降级标记。

## Runtime 解耦与异步边界

日期：2026-08-31

- `SessionReadBarrier` 不再访问具体 Runtime 的 `event_source` 或 `recover()`，只依赖
  `ProjectionCatchup.target_seq()` / `catch_up()` 协议；
- `ProjectionRuntime` 实现该协议，保留 Task 2 的 stream 缓冲、checkpoint 和重放能力；
- `SessionProjectionBackend` 继续作为轻量适配器注册到 Dispatcher；
- 长期记忆提取不新增第二套事件投影队列，后续使用现有 Outbox/Worker 的独立 topic，
  与同步会话投影分离。

TDD 验证：`tests/test_session_read_barrier.py` 与 `tests/test_recovery.py` 共 17 passed。

## UPDATE 版本来源

日期：2026-08-31

- Parser 不再把 `UPDATE` 的 `expected_version` 固定为 1；缺省值表示由执行事务
  读取当前 session 版本；
- SessionStore 在事务内绑定当前版本并递增，连续多次自然更新不再误报版本冲突；
- 调用方显式提供 `expected_version` 时仍执行严格 CAS，旧版本继续抛出
  `StaleWriteError`；
- `MemoryOperation` 允许 UPDATE 缺省版本，保留执行层的版本解析职责。

TDD 验证：新增 Parser 连续两次 UPDATE 测试，Task 3 相关测试 21 passed。

## 统一 Session 写入入口语义

日期：2026-08-31

- `upsert_active()` 与 `apply_operation()` 现在共享同一个内部状态变更函数；
- 直接 upsert 同样要求 `source_seq <= session.last_seq`，不能绕过源事件投影；
- 同一 key、同一 source_seq、同一来源事件且内容相同视为重复投递；
- 同序号不同内容抛出 `StaleWriteError`，旧序号保持 no-op；
- 保留 `upsert_active()` 兼容签名，但不再保留与操作入口不同的冲突语义。

TDD 验证：新增直接 upsert 的水位、重复和冲突测试，Task 3 相关测试 22 passed。

## Tenant/session 身份隔离

日期：2026-08-31

- `SessionStore` 增加可选 `tenant_id` 命名空间；多租户实例将 session 快照键隔离为
  `<tenant_id>:session:<session_id>`；
- 配置租户后只接受对应的 `tenant:<tenant_id>/.../session:<session_id>` stream，跨租户
  事件在投影入口拒绝；
- `stream_id_for_session()` 和 `SessionReadBarrier` 会生成匹配租户的规范 stream；
- 未配置 tenant_id 的实例保留旧的全局命名空间，仅用于兼容单租户/迁移场景；多租户
  生产路径必须显式配置 tenant_id。

TDD 验证：新增同名 session 的双租户隔离和 foreign stream 拒绝测试。

## 纳入版本化 schema migration

日期：2026-08-31

- 将 `session_states` 表定义统一移动到 `storage.schema`，避免 SessionStore 持有第二份
  表元数据；
- 新增 `0004_session_states` migration，数据库初始化统一创建会话投影表；
- 移除 SessionStore 初始化时的隐式建表，旧数据库中已存在的表由 migration 的
  `checkfirst` 安全接管，不修改既有数据；
- 更新 migration discovery/应用测试，保证新库和并发启动仍保持幂等。

TDD 验证：Task 3 相关存储、会话协调器、读屏障测试 61 passed。

## 会话快照 JSON 边界

日期：2026-08-31

- 最近事件 payload 在进入 `SessionState` 前经过确定性的 JSON round-trip，UUID 和
  datetime 等与 EventStore 相同的可序列化值统一转换为字符串；
- 不可序列化值在本地事务提交前失败，不会留下半更新的 session snapshot；
- 重启后读取的 recent message 与本次投影返回值保持一致，避免直接投影适配器绕过
  EventStore 时出现不同的序列化语义。

TDD 验证：会话操作、协调器和读屏障测试 21 passed。

## FORGET 的版本校验

日期：2026-08-31

- 修复显式 `FORGET` 忽略 `expected_version` 的问题；当删除命令携带版本时，必须
  与当前 session memory 版本匹配，否则抛出 `StaleWriteError`；
- 未携带版本的删除仍保持幂等兼容行为，删除不存在的记忆不会制造新状态；
- 防止延迟或并发的旧删除命令覆盖较新的 UPDATE。

TDD 验证：新增 stale FORGET 回归测试；会话操作与协调器测试 18 passed。

## 同一 session 并发显式命令串行化

日期：2026-08-31

- 修复 Coordinator 直接并发调用 `SessionStore.apply_event()` 导致的竞态：seq=2
  可能在 seq=1 投影完成前先执行并收到 sequence gap；
- `SessionStore` 提供按规范 session stream 归一化的 `command_lock()`，由
  `SessionCommandCoordinator` 包住 EventStore append 与 SessionStore apply；
- 锁归属 SessionStore，因此共享同一投影实例的多个 Coordinator 也能复用同一把锁；
- 该锁解决单进程内的并发排序，跨进程仍需由持久化 ProjectionRuntime/队列保证顺序，
  不宣称分布式 exactly-once。

TDD 验证：新增同一 session 双线程显式命令测试；Task 3 会话相关测试 23 passed。

## 跨进程命令租约与版本单调性

日期：2026-08-31

- `upsert_active()` 现在要求新写入的 `MemoryRecord.version` 严格大于当前版本，
  防止 source_seq 前进但 memory version 回退；旧版本写入抛出 `StaleWriteError`。
- 新增 `session_command_leases` 表（migration `0005_session_command_leases`），以
  `(tenant/session, stream)` 的持久化键协调不同进程的命令；租约包含过期时间和递增
  `fencing_token`。
- Coordinator 在事件追加后、投影前验证租约 token；租约过期或被抢占时拒绝旧进程的
  投影，事件保留在 EventStore，后续由恢复重放。
- 释放租约只把 `lease_until` 置为过期而不删除记录，确保 fencing token 不回退。

TDD 验证：版本回退与独立 SessionStore 实例租约测试加入；相关测试 62 passed，
恢复、事件、协议、Outbox/Worker 回归测试 47 passed。

补充修复：`apply_event()` 对 lease 使用 `SELECT ... FOR UPDATE`（SQLite 下由
`BEGIN IMMEDIATE` 提供等价写事务串行化），并校验 lease 的 session 身份；Coordinator
始终向具体 SessionStore 传递 fencing token，不再通过反射兼容分支绕过租约校验。

租约抢占更新进一步采用旧 fencing token 和旧过期时间条件的 CAS；并发抢占失败的一方
重新轮询，不会把两个 owner 错误地分配成同一个新 token。

补充：`SessionLease` 绑定完整的 tenant/session storage key，拒绝跨租户复用同名
session 的租约；Coordinator owner_id 使用进程号加随机 UUID，避免实例标识碰撞。

最终 TDD 验证：Task 3/Task 2 相关测试 79 passed，核心回归集合 110 passed。

## 未配置租户时拒绝 tenant stream

日期：2026-09-01

- 未配置 `tenant_id` 的 SessionStore 只用于全局/单租户 `session:<id>` stream；
- 对 `tenant:<tenant_id>/session:<id>` 输入直接拒绝，防止不同租户的同名 session
  被错误折叠到全局 `session_id`；
- 多租户路径必须显式创建 tenant-scoped SessionStore。

## 统一可扩展 stream ID 语法

日期：2026-09-01

- 对齐设计协议和 Task 2 既有示例，scope segment 使用 `/` 分隔，字段和值使用 `:`：
  `tenant:t1/session:s1`、`tenant:t1/project:p1/session:s1`；
- tenant-scoped SessionStore 校验首段 tenant 与末段 session，中间 scope segment 保留
  给 project/team 等扩展；
- unscoped SessionStore 只接受严格的 `session:<id>`，不再从任意复合字符串中截取
  session 后缀；
- 内部 storage session key 保持不变，已有 session snapshot 和 lease 无需迁移。

## 租约数据库错误分类

日期：2026-09-01

- lease insert 的唯一键竞争继续作为并发抢占重试；
- OperationalError 只对数据库锁、死锁和序列化冲突执行有限重试；
- 缺表、SQL 不兼容、权限等永久错误立即保留原异常抛出，不再等待默认 30 秒后
  伪装成 lease timeout。

## 目标水位不可用时的读取降级

日期：2026-09-01

- `SessionReadBarrier` 现在同时处理 `target_seq()` 权威水位查询失败；
- EventStore 暂时不可用时仍返回本地 SessionState，`requested_seq` 回落为当前
  `applied_seq`，并设置 `degraded=True` 和错误信息；
- 此时 `lagging=False` 仅表示没有已知缺口，不能解释为已确认追平权威事件流。

## Session projection 健康检查

日期：2026-09-01

- `SessionProjectionBackend.health()` 不再无条件返回 True；
- 健康检查通过只读查询验证数据库连接和 `session_states` 表可用；
- 异常交由 ProjectionDispatcher 统一转换为 `False` 并保留具体错误，检查过程不执行
  migration 或隐式修复。

## 确定性记忆身份与完整重建

日期：2026-09-01

- 修复 REMEMBER 重放时由 `MemoryRecord` 默认工厂生成随机 ID 的问题；否则后续按
  `memory_id` 写入的 FORGET 在空库重建后无法命中新记录，已删除记忆会重新出现。
- 新记忆 ID 由权威 `source_event_id` 确定性派生，旧的 `memory.command` 事件无需迁移；
  操作显式携带 `memory_id` 时继续尊重该身份。
- 同一 key 的后续 REMEMBER/UPDATE 保留已有 memory ID 和 `created_at`，并使用事件
  `occurred_at` 作为 `updated_at`，确保相同事件流生成相同会话快照。
- 新增 REMEMBER、按 ID FORGET、空库完整重放回归测试，同时比较重建前后的完整
  `MemoryRecord` 并验证删除结果不会复活。

TDD 验证：Task 3 会话操作、协调器和读取屏障测试 33 passed。

## 限制调用方覆盖会话记忆身份

日期：2026-09-01

- `MemoryOperation` 现在拒绝 REMEMBER/UPDATE 携带 `memory_id`；新会话记忆的身份仅由
  权威事件 ID 确定性派生，UPDATE 则沿用当前记忆的身份。
- `memory_id` 仅作为 FORGET 的可选定位条件，避免两个不同 key 被调用方赋予同一 ID，
  导致按 ID 删除依赖内部列表顺序而只删除其中一个。
- 新增调用方为 REMEMBER 指定 ID 的拒绝测试，确认事件与会话投影均不会写入。

TDD 验证：模型与 Task 3 会话测试 42 passed。

## 扩展 scope stream 的投影隔离

日期：2026-09-01

- 修复 tenant-scoped SessionStore 接受 `project:<id>` 等中间 scope segment、但快照水位
  仅按 `(tenant_id, session_id)` 存储的问题；不同 project 的同名 session 都从 seq=1
  开始，第二条事件此前会被静默误判为已投影。
- 非 canonical 的扩展 session stream 现在使用完整 stream 派生的稳定内部存储键，快照、
  进程内命令锁和跨进程 lease 由此一起隔离；canonical stream 保持原有存储键，已有数据
  无需迁移。
- `SessionStore.get()`、读屏障和投影水位读取均支持完整 `stream_id`。省略该参数仍读取
  canonical stream；使用 project 等扩展 scope 的调用方必须传入完整 stream，避免同名
  `session_id` 的读取歧义。
- 迁移后发现旧版本写入的 `stream:<hash>` 快照或租约没有完整身份时，按 legacy 执行：
  读取/投影直接报告需要从 EventStore 重放，不把无法归属的旧数据静默暴露给新 project。
- `0006_session_stream_identity` 使用跨 SQLite/PostgreSQL 方言可执行的显式 `ALTER TABLE`
  增列，并新增旧 `0005` 数据库升级回归测试；全新数据库仍由版本化 schema 直接创建新列。
- 新增同一 tenant、不同 project、同名 session 的独立投影与读屏障回归测试。

TDD 验证：Task 3 会话操作、协调器和读取屏障测试 36 passed。

## 兼容历史 memory.command 事件

日期：2026-09-01

- 新写入仍由 `MemoryOperation` 拒绝 REMEMBER/UPDATE 的调用方 `memory_id`，但投影重放
  对升级前已落库的这类事件执行兼容解码，并保留其历史 ID，避免升级后重放失败。
- 兼容路径只存在于事件投影内部，不改变公开命令契约；新记忆仍由当前事件 ID 派生稳定
  身份，历史事件则使用其已持久化身份。
- 历史事件若与当前 active memory 产生 ID 冲突，会抛出错误并回滚整个投影事务，水位不
  推进，等待人工诊断或从权威事件流修复。
- 新增旧格式 REMEMBER 重放和历史 ID 冲突回归测试。

TDD 验证：历史事件兼容与 Task 3 会话测试通过。

## Legacy 扩展投影的可执行恢复

日期：2026-09-01

- 修复 legacy 扩展快照虽然报告 `replay required`，但 `apply_event()` 重放第一条事件时
  仍读取同一旧行并再次失败，导致恢复永久卡死的问题。
- `0007_session_stream_recovery` 会根据权威 `events.stream_id` 找出存在 project 等扩展
  scope 的 session，清理无法确认归属的旧 hash/canonical 快照、旧 lease，以及这些完整
  stream 在所有 projection 名称下的 checkpoint，强制恢复从 seq=1 开始。
- 没有扩展事件的普通 canonical session 不会被清理；迁移不猜测旧内容属于哪个 project。
- 作为滚动升级防御，若迁移后仍遇到无完整身份的 legacy hash 行，只有 seq=1 重放可以
  原子覆盖为新格式；普通读取和 seq>1 继续拒绝。
- 新增旧库选择性清理、自定义 projection checkpoint 和 seq=1 重建回归测试。
- 恢复清理使用独立 `0007`，不修改已经发布的 `0006`，确保已执行过 stream identity
  migration 的数据库也会得到恢复修复。
- 补充修复 `0006` 与 `0007` 之间的升级窗口：canonical 快照即使已被新代码回填非空
  `stream_id`，其内容仍可能包含旧 project 数据。因此，只要权威事件日志确认存在扩展
  stream，`0007` 就无条件清理对应 canonical 快照和租约，再分别重放 canonical/project
  stream；没有扩展事件的普通 canonical session 继续保留。

TDD 验证：旧库恢复定向测试通过。

## Lease 释放异常不掩盖命令结果

日期：2026-09-02

- 修复事件与 SessionStore 投影已成功后，`command_lease()` 的 finally 释放失败覆盖成功
  返回值，导致调用方误判失败并可能在无 idempotency key 时重复提交的问题。
- 命令主体成功时，释放错误记录到日志和 `SessionStore.lease_release_errors()`，命令仍返回
  已提交事件与投影状态；租约随后通过过期时间自然失效。
- 命令主体失败时保留原始异常，并通过 exception note 附加 lease 释放失败信息，不再用
  清理异常替换真正的投影错误。
- 后续成功释放同一 stream 的 lease 会清除对应诊断。
- 新增成功命令释放失败、投影失败同时释放失败，以及后续成功释放清除诊断三类故障
  注入测试。

TDD 验证：协调器定向测试 20 passed，Task 3 相关回归 99 passed，排除用户未跟踪
`tests/test_session_consistency.py` 后的完整仓库测试 129 passed；`compileall` 与
`git diff --check` 通过。

## 过期 Lease 释放的 fencing 诊断保护

日期：2026-09-02

- 修复旧 fencing token 释放时条件更新影响行数为 0，却被误判为成功并清除较新租约诊断
  的问题。
- Lease 释放现在检查条件更新的 `rowcount`；旧 token 只记录告警，不会覆盖或清除较新
  token 的释放错误。
- 内部诊断携带 fencing token，公开的 `lease_release_errors()` 仍返回 stream 到错误消息
  的稳定格式。
- 新增旧租约晚到释放不影响新租约诊断的并发时序测试。

TDD 验证：协调器定向测试 21 passed，Task 3 相关回归 100 passed，排除用户未跟踪
`tests/test_session_consistency.py` 后的完整仓库测试 130 passed；`compileall` 与
`git diff --check` 通过。

## 拒绝冲突的会话事件序号

日期：2026-09-03

- 新增 `session_event_receipts`，在会话投影事务中记录每个已应用序号对应的
  `event_id` 和不可变事件指纹；同一 `(session, seq)` 重放相同事件保持幂等，内容不同
  时抛出 `ProjectionConflictError`，不再静默返回旧快照。
- `memory.command` 的事件收据与会话状态在同一事务提交，冲突或操作失败都不会推进
  会话水位，也不会产生重复 active memory。
- 新增 `0008_session_event_receipts` 迁移。升级旧库时仅为快照 `last_seq` 以内且事件表字段
  完整的历史事件回填收据；无法安全判断归属或字段不完整时跳过，交由权威事件流重放。
- 收据缺失但快照已越过该序号时 fail closed，返回明确的 `replay required`，避免损坏状态
  被当成成功读取。
- `ProjectionConflictError` 同时继承领域错误和 `ValueError`，兼容既有调用方的校验捕获。

TDD 验证：Task 3 会话、协调器、读取屏障和迁移定向测试 91 passed；用户未跟踪的
`tests/test_session_consistency.py` 仍有既有的乱序投影与 parser 期望失败，未修改。

## 校验通用投影 checkpoint 的事件身份

日期：2026-09-03

- 修复 `ProjectionDispatcher` 在 `event.seq <= checkpoint` 快速路径中只比较序号、
  静默跳过不同事件的问题。
- 新增 `projection_event_receipts`，由关系型 checkpoint store 持久化每个 projection、
  stream、seq 对应的 `event_id` 和事件指纹；已完成序号只有 receipt 完全一致时才可跳过。
- receipt 缺失时 fail closed 并报告 `replay required`；receipt 冲突时报告明确的投影
  冲突，不调用后端重复执行。
- 新增 `0009_projection_event_receipts` 迁移，为已有 checkpoint 覆盖的权威事件回填 receipt，
  对字段不完整的旧事件表安全跳过，并对迁移重试保持幂等。
- 保留第三方旧 checkpoint 实现的兼容路径；实现严格跨进程校验的适配器应提供可选的
  `get_receipt()` / `save_receipt()` 能力。

TDD 验证：Dispatcher、Recovery、Session Task 3 定向测试通过；`compileall` 与
`git diff --check` 待本轮收尾验证。

## 收敛 checkpoint 的严格 receipt 契约

日期：2026-09-03

- 修复第三方 checkpoint 实现缺少 receipt 能力时仍静默按序号跳过事件的问题。
- `ProjectionDispatcher` 启用持久化 checkpoint 时，现在要求实现
  `ProjectionCheckpointReceiptStore`；不完整实现会在初始化阶段明确拒绝。
- 内置关系型 checkpoint store 在投影成功后写入 receipt，快速路径直接校验 receipt，
  不再使用隐式弱一致性兼容分支。
- 更新 fake checkpoint 测试和公共 storage 导出，明确第三方适配器必须同时实现
  `get_receipt()` / `save_receipt()` 才能使用严格 Task 3 语义。

TDD 验证：checkpoint、Dispatcher、Recovery 和 Session Task 3 回归测试通过。
