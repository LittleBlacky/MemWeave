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
