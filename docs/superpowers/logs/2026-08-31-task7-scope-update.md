# Task 7 范围同步日志：自然语言记忆提取

日期：2026-08-31

## 背景

Task 3 已将显式记忆命令抽象为可注册的 `CommandSpec/ParserRule`。评审中确认，
不能要求普通用户始终使用 `记住 key = value`；自然语言事实和项目约束也应进入记忆链路。

## 范围决策

- 将自然语言记忆候选提取纳入 Task 7 的 L1 编排范围；
- Task 7 只实现可替换的 `MemoryExtractor` 和 `MemoryPolicy` 接口及规则基线；
- 具体托管 LLM 提供商、向量/图索引和自动策略进化仍不在本阶段；
- 显式命令继续走同步、确定性的 Parser；普通自然语言提取在回合结束后异步入队，
  不阻塞下一轮 Agent 执行；
- 所有隐式候选必须经过作用域、置信度、敏感数据和 create/update/delete 意图校验后才能落库。

## 计划变更

已更新 `docs/superpowers/plans/2026-08-29-memory-system-phase1-plan.md`：

- Task 7 增加 `src/memweave/extraction.py` 和 `tests/test_extraction.py`；
- 增加 `MemoryCandidate`、`MemoryExtractor`、`MemoryPolicy` 接口；
- 增加自然语言约束、歧义文本、作用域规范化和策略拒绝测试；
- 明确提取延迟/重试不能影响会话工作记忆和下一轮读取。

## 当前状态

本次只同步任务边界和验收标准，尚未实现 Task 7 代码。后续按 TDD 拆分为提取器、
策略门和 Middleware/Kernel 编排三个小问题逐项实现。
