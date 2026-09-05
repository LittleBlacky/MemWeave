# Task 4：长期记忆权威、版本与 tombstone

日期：2026-09-05

## 目标

建立独立于 SessionStore 的长期记忆权威表，保存同一作用域和 key 的完整版本链，
让删除通过 tombstone 屏蔽旧值，并为异步乱序和并发更新提供明确的拒绝语义。

## 实现

- 新增 `DurableMemoryStore`，提供 `create`、`update`、`forget`、`get_active` 和
  `list_versions` 接口。
- 新增 `durable_memories` 迁移。旧版本只标记为 `superseded`，删除追加
  `retracted` 版本，不物理覆盖历史。
- `expected_version` 使用 CAS；`source_seq` 必须严格递增，旧来源不能覆盖新版本。
- 同一记录或同一来源事件重试返回已落库版本；同一来源事件携带不同内容时抛出
  `StaleWriteError`，避免重复记忆和静默冲突。
- 同一 memory key 的 `memory_id` 在版本链中保持稳定；`session_only` 记录拒绝进入
  长期权威表。
- `DurableMemoryStore.create()` 现在只接受 `active` 初始记录；candidate、
  needs_confirmation 以及其它生命周期状态不会遮蔽已有 active 记忆，候选晋升留给
  Task 7 的策略流程。
- 并发更新和删除改用带 `version`、`key`、`active` 状态条件的 CAS 更新；条件更新
  影响行数为零时返回 `StaleWriteError`，不插入新版本。唯一版本约束产生的
  `IntegrityError` 也统一转换为 `StaleWriteError`，调用方不再依赖底层数据库异常类型。
- `forget()` 同时收到 `key` 和 `memory_id` 时现在比较完整记忆身份；即使历史数据中
  出现同 key 的不同 ID，也会拒绝请求，不会静默删除错误版本。
- 长期更新现在强制携带 `expected_version`；缺少版本的更新在事务前拒绝，避免调用方
  基于旧读取结果静默覆盖后来提交的版本。会话投影的兼容行为不改变。
- 长期删除同样强制携带 `expected_version`；旧删除请求不会再根据最新版本直接生成
  tombstone，避免撤销并发写入的新记忆。
- `create()` 现在要求版本链连续：首次写入必须是 v1，后续写入必须是当前版本加一；
  跳号写入会被拒绝，避免审计和恢复无法区分缺失版本与合法跳跃。
- `value` 现在只接受严格 JSON 原生值并递归校验；不再把 UUID、datetime 或其它对象
  隐式转成字符串，重启读取可保持稳定的值类型。
- 长期更新和删除现在强制携带真实 `source_seq`；缺失来源序号时拒绝写入，不再按当前
  版本自动生成“最新”序号，避免旧异步操作伪装成新来源。
- source event 重放现在同时校验目标版本和来源序号；参数发生变化的重试不会被当成
  幂等请求直接返回，删除 tombstone 重放也必须基于被删除前的版本。
- 版本表允许同一 `memory_id` 在同一 key 下重复出现（每个版本一行），因此没有在
  `durable_memories` 上直接创建 `memory_id` 唯一索引；新增独立的
  `durable_memory_identities` 注册表，保证一个作用域内的 `memory_id` 只能绑定一个
  key。新版本写入前校验绑定关系，历史迁移发现同一身份绑定多个 key 时整体失败，
  不自动选择或删除冲突记录。
- 仅携带 `memory_id` 的删除重放现在先通过身份注册表解析真实 key，再匹配原始
  `source_event_id`；已存在 tombstone 只有在 `expected_version` 和 `source_seq`
  都一致时才幂等返回，参数变化会明确拒绝，避免删除命令冲突被静默吞掉。
- 身份注册表现在同时约束两个方向：同一作用域内 `memory_id` 不能跨 key 重用，
  同一 key 也不能在版本链中更换 `memory_id`。迁移回填发现任一方向存在历史冲突
  都会失败；写入路径也在插入前显式拒绝，避免只依赖数据库唯一约束异常。

## 验证

```text
G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest tests/test_durable_versions.py -q
10 passed

G:\\Anaconda\\envs\\smallshrimp\\python.exe -m pytest -q
171 passed

G:\\Anaconda\\envs\\smallshrimp\\python.exe -m compileall -q src
git diff --check
```

## 边界

本 Task 不负责 Outbox、向量/图索引、自然语言提取和召回。跨数据库事务、索引投影
和长期记忆的自动晋升留到后续任务；本表是可独立读取和重建派生索引的长期状态权威。
