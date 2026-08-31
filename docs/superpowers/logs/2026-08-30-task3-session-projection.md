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

- 会话表暂由 `SessionStore` 幂等创建，尚未加入版本化迁移；后续需要在不破坏
  Task 2 迁移契约的前提下补充专用 migration。
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
- 配置租户后只接受对应的 `tenant:<tenant_id>:session:<session_id>` stream，跨租户
  事件在投影入口拒绝；
- `stream_id_for_session()` 和 `SessionReadBarrier` 会生成匹配租户的规范 stream；
- 未配置 tenant_id 的实例保留旧的全局命名空间，仅用于兼容单租户/迁移场景；多租户
  生产路径必须显式配置 tenant_id。

TDD 验证：新增同名 session 的双租户隔离和 foreign stream 拒绝测试。
