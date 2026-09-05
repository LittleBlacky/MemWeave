# 2026-09-06 计划边界调整日志

## Scope

根据对 MemWeave 定位的复核，收敛阶段一边界，并补充记忆网络与阶段二路线；本次不修改运行时代码。

## Decisions

- “记忆网络”作为总体逻辑架构，“经验网络”作为后续派生层。
- `Memory`、`Episode`、`Experience`、`Skill`、`Prediction` 和 `Policy` 分开建模。
- 阶段一只交付可靠记忆底座；Task 7 仅保留可替换提取接口和规则基线。
- 阶段二负责候选提取、候选生命周期、Resolver、关系基础模型和语义召回。
- Skill 由 MemWeave 保存、检索和治理，由 Agent Runtime 执行；高风险能力发布由外部控制平面负责。
- `CLAUDE.md` 保持为 `AGENTS.md` 的委托文件，不重复添加规则。

## Changed Files

- `AGENTS.md`
- `README.md`
- `docs/superpowers/specs/2026-08-29-memory-system-design.md`
- `docs/superpowers/plans/2026-08-29-memory-system-phase1-plan.md`
- `docs/superpowers/plans/2026-09-memory-system-phase2-plan.md`

## Verification

- 文档交叉搜索阶段、对象和 Skill 边界。
- `git diff --check`。

## Known Risks

- 阶段一现有运行时代码和 Task 6 未提交改动保持不变；候选、Episode、Skill 数据模型尚未实现。
- 阶段二的具体 LLM 和第三方索引供应商仍需后续 ADR 和契约测试。
