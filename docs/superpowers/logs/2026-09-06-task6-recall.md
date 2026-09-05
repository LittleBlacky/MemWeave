# Task 6 开发日志：基础召回

## 范围

实现确定性的基础 Recall Service，不引入向量数据库、第三方索引、LLM Router 或 Agent Adapter。

## 已完成

- 新增 `RecallService` 与可替换的 `RecallProvider` 接口。
- 会话工作记忆优先于长期记忆；同一 key 按作用域优先级合并。
- 长期记忆只读取每个作用域的最新版本，自动屏蔽 superseded/retracted/expired 等状态。
- 召回结果执行作用域、kind、去重、关键词排序、Top-K 和 token 预算过滤。
- Durable 读取失败时保留会话结果并返回 `degraded=True`。
- `DurableMemoryStore.list_active()` 通过权威版本链返回某作用域的 active 记录。
- 不把记忆记录的 `source_seq` 冒充 Durable Projection watermark；当前 durable 水位保持未知，等待后续明确的水位接口。
- Provider/派生索引命中必须与当前权威记录的 memory_id、作用域、key 和 version 一致；旧版本和 tombstone 命中会被过滤。

## 验证

```text
python -m pytest tests/test_recall.py -q
6 passed; full suite 217 passed
```

## 边界

当前关键词匹配是确定性的基础实现，不代表语义检索质量；向量/图索引与混合重排留到后续阶段。
