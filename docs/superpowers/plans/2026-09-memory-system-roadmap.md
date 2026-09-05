# MemWeave 总体路线计划

## 1. 总体目标

MemWeave 为任意 Agent 提供一个框架无关的记忆、经验和技能基础设施。它记录不可变事件，维护可验证的会话和长期状态，并在后续阶段将任务轨迹归纳为可复用经验和 Skill。

MemWeave 不接管 Agent 的规划循环，不直接执行任意 Skill，也不直接修改 Prompt、工具、模型或生产代码。

## 2. 分层边界

```text
Agent Runtime
  └─ 规划、工具调用和 Skill 执行

MemWeave
  ├─ Event / Evidence
  ├─ Working Memory
  ├─ Durable Memory
  ├─ Relation / Episode / Experience
  ├─ Skill Registry
  ├─ Recall / Governance / Audit
  └─ Outbox / Projection / Recovery

External Control Plane
  └─ 高风险策略、Prompt、工具、模型和代码的评估、发布、灰度和回滚
```

对象之间不是无条件转换关系：

```text
Event → Memory Candidate → Memory
Event → Episode → Experience → Skill Candidate → Validated Skill
当前状态 → Prediction（临时推断，不是事实记忆）
```

每个派生对象都必须保留来源、作用域、版本、状态、生成器和验证结果。

## 3. 四阶段路线

### 阶段一：可靠记忆底座

**目标：** 让已有 Agent 能可靠记录、读取和修改记忆。

**交付：**

- 不可变事件日志和按 stream 的连续水位；
- Session 工作记忆和显式命令同步投影；
- 长期事实权威表、版本、CAS、tombstone；
- Outbox、重试、幂等、租约和重放；
- 基础 session-first 召回和 Token 预算；
- Memory Protocol、L1 Middleware、L3 Tools/MCP 和最小 HTTP 接入。

**不包含：** 托管 LLM 自动提取、真实向量/图厂商、Episode 经验归纳、Skill 执行和预测。

**完成门槛：** 原始事件不丢失，显式写入立即可见，重复/乱序/重启可恢复，作用域和删除语义可验证。

### 阶段二：自动化记忆与语义召回

**目标：** 从普通自然语言中提取受治理的事实候选，并按语义召回相关记忆。

**交付：**

- CandidateStore、提取运行记录和候选生命周期；
- 规则及 LLM 可替换提取器；
- 规范化、Resolver、冲突和晋升策略；
- 关系基础模型和证据关联；
- 向量/关键词混合检索及权威回查；
- 用户确认、敏感信息治理和质量评测；
- L2 Proxy 和更多宿主适配器。

**完成门槛：** 自动记忆可追溯、可去重、可冲突处理，索引不可用时可降级，候选不会绕过权限和版本约束。

详见[阶段二计划](2026-09-memory-system-phase2-plan.md)。

### 阶段三：记忆关联、经验和 Skill

**目标：** 从完整任务中形成可验证、可复用的经验和工作流程。

**交付：**

- Episode 生命周期和任务结果反馈；
- Experience、失败模式和适用条件归纳；
- Skill/Workflow Registry、版本、验证、复用和废弃；
- 图索引、多跳关系查询和经验召回；
- 多租户共享、外部 Source Adapter 和积压运维。

**完成门槛：** 经验必须关联完整 Episode 和验证结果；Skill 必须经过回放或测试；旧版本可追溯、可撤销、可回滚。

### 阶段四：预测与受控进化

**目标：** 利用记忆、经验和反馈改善召回、流程和下一步建议。

**交付：**

- 用户意图、下一步工作和风险预测；
- 召回、提取、工具和 Skill 策略评估；
- 离线回放、影子运行、灰度和回滚；
- Evaluator、Policy Registry、Experiment Registry；
- 面向 Prompt、工具、模型和代码的外部能力发布接口。

**完成门槛：** 预测只作为建议或预取信号；策略变化有对照实验、审计和回滚；错误经验不会被自动自我强化。

## 4. 阶段依赖

```text
阶段一 可靠事件和权威状态
   ↓
阶段二 候选提取和语义召回
   ↓
阶段三 Episode、经验和 Skill
   ↓
阶段四 预测、评估和受控进化
```

后一个阶段不得绕过前一个阶段的事件、权限、版本、审计和删除不变量。阶段三可以先使用阶段二的规则召回，阶段四可以先使用阶段三的离线评估，不要求一次性完成所有外部基础设施。

## 5. 跨阶段验收指标

所有阶段都持续关注：

- 事件完整率和投影恢复率；
- stale read、重复写入和冲突率；
- 跨作用域泄漏率；
- 召回 Precision@K、Recall@K 和 Token 成本；
- 自动提取准确率和错误记忆率；
- 经验/Skill 复用成功率；
- 预测命中率、任务成功率、延迟和降级率。

## 6. 当前状态

阶段一的事件、会话、长期权威和 Outbox 基础已在持续实现；阶段一剩余工作按现有 Phase 1 Plan 收尾。阶段二已经建立独立实现计划，阶段三和阶段四在对应阶段开始前分别拆成独立执行计划。

## 7. 计划维护规则

- 总体路线只描述阶段目标、边界、依赖和门槛；具体代码步骤放入阶段 Plan。
- 修改事件、记忆对象、作用域、一致性或权限语义时，先更新设计规格或新增 ADR。
- 新增 Experience、Skill 或 Prediction 能力时，不把它们塞入 `MemoryRecord`。
- 每个阶段都必须有独立开发日志、定向测试和可回滚的提交边界。
