# MemWeave

An event-sourced, evolvable memory fabric for agents.

MemWeave 是面向 Agent 的可扩展记忆、经验和技能基础设施。它将原始对话、工具调用、外部业务事件和用户操作记录为不可变事件，再投影为会话工作记忆、长期事实、任务经验和可检索关系；技能由 MemWeave 保存和治理，由 Agent Runtime 执行。

## License

MemWeave is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.

## 项目目标

- 在当前会话内提供稳定的读后写一致性。
- 在跨会话场景下提供可追溯、可版本化的长期记忆。
- 支持个人、项目、团队、租户和全局等多级作用域。
- 通过统一核心接口同时支持本地 SDK、独立服务和 MCP/HTTP 接入。
- 采用系统基础召回与 Agent 受控工具深挖的混合召回方式。
- 支持明确记忆操作的同步处理、普通事实的异步提取和历史经验的事后归纳。
- 通过事件、反馈和评估支持可观测、可评估、可回滚的受控自进化。

## 非目标

- 第一阶段不实现自动修改生产代码或模型的能力。
- 第一阶段不实现 Episode、Experience、Skill、Workflow 或 Prediction 的完整运行时能力。
- 第一阶段不要求绑定某一种向量数据库、图数据库或消息队列。
- 第一阶段不把搜索索引作为记忆真相；权威状态必须可独立读取和重建索引。
- 第一阶段不追求一次性覆盖所有外部数据源和多模态解析器。

## 核心原则

~~~text
原始历史保证不丢
会话投影保证当前连续
长期权威表保证版本和一致性
向量/图索引负责发现相关内容
经验流水线负责事后归纳
Agent 只通过受控接口参与
~~~

## 文档

- [系统设计规格](docs/superpowers/specs/2026-08-29-memory-system-design.md)
- [总体路线计划](docs/superpowers/plans/2026-09-memory-system-roadmap.md)
- [开发规范](CONTRIBUTING.md)

## 开发环境

项目使用 `uv` 管理 Python 虚拟环境和依赖。需要先安装 `uv`，然后在仓库根目录执行：

```powershell
uv sync
uv run pytest -q
```

`uv sync` 会根据 `.python-version` 使用 Python 3.11，创建本地 `.venv` 并按 `uv.lock` 安装锁定依赖。日常命令通过 `uv run` 执行，避免依赖当前 shell 是否激活了虚拟环境。

常用检查命令：

```powershell
uv run python -m compileall -q src
uv run pytest tests/test_events.py tests/test_storage_ports.py -q
```

## 计划路线

1. 可靠记忆底座：事件日志、会话投影、显式记忆 CRUD、长期权威表、Outbox 和基础召回。
2. 自动化记忆与语义召回：事实/偏好候选提取、关系建立、向量索引、混合检索和治理策略。
3. 记忆关联、经验和 Skill：Episode、经验归纳、失败模式、Skill/Workflow 注册和复用。
4. 预测与受控进化：反馈评估、用户意图预测、召回/提取/工具策略灰度和能力优化流水线。

当前首个 commit 只建立项目目标和开发约束，不包含运行时代码。
