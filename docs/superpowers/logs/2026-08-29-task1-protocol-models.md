# Task 1 开发日志：项目骨架、领域模型和 Memory Protocol

日期：2026-08-29

## 目标

建立正式的 `memweave` Python 包，定义记忆领域模型和框架无关的 Memory Protocol，不实现数据库、投影、召回或 Agent Adapter。

## 实际变更

- 创建 `pyproject.toml`，项目包名为 `memweave`，Python 最低版本为 3.11。
- 创建 `src/memweave/__init__.py`、`models.py`、`protocol.py`、`errors.py` 和 `clock.py`。
- 定义 `MemoryRecord`、`MemoryOperation`、`Event`、`AuthContext`、`RecallRequest`、`RecallResult` 和 `Watermarks`。
- 定义 `ProtocolVersion`、`RequestEnvelope`、`CapabilitySet`、`TurnInput`、`ContextEnvelope`、`ProtocolEvent` 和 `TurnOutcome`。
- 事件类型使用字符串字段，允许第三方 Agent 注册或发送扩展事件类型；内置 `EventType` 仅作为常用值集合。
- 增加显式操作、置信度、作用域、来源、协议元数据和幂等键校验。

## TDD 记录

- RED：模型和协议测试在包不存在时因 `ModuleNotFoundError: No module named 'memweave'` 失败。
- GREEN：实现最小模型后，10 个测试通过。
- 复核后补充扩展事件类型和 `ConsistencyMode` 测试；最终 12 个测试通过。

## 验证

```text
G:\Anaconda\envs\smallshrimp\python.exe -m pytest tests -q
12 passed
G:\Anaconda\envs\smallshrimp\python.exe -m compileall -q src
git diff --check
```

## 提交

- `06695c8 feat: define memweave protocol and domain models`
- `139aaf8 fix: keep protocol events extensible`

## 遗留风险

- 当前模型只负责结构和输入校验，不验证身份是否来自可信服务边界。
- 协议版本兼容策略和事件 Schema Registry 尚未实现。
- 事件尚未持久化，后续由 Task 2 的 EventStore 负责。
