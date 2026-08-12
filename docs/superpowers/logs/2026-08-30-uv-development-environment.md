# 开发环境变更日志：uv 虚拟环境管理

日期：2026-08-30

## 目标

统一 MemWeave 的 Python 虚拟环境、依赖解析和开发命令，避免依赖系统 Python 或未锁定的环境状态。

## 实际变更

- 在 `pyproject.toml` 增加 `dev` dependency group，包含 pytest。
- 增加 `.python-version`，固定开发 Python 为 3.11。
- 生成并提交 `uv.lock`，锁定运行时和开发依赖。
- 在 README 和 CONTRIBUTING 中统一记录 `uv sync`、`uv run pytest` 和工具命令。
- 忽略 `*.egg-info/` 构建产物。

## 验证

```text
uv --version
uv 0.11.14
uv lock --offline
Resolved 14 packages
uv run --no-sync pytest tests/test_events.py tests/test_storage_ports.py tests/test_outbox.py tests/test_worker.py -q
32 passed
uv run --no-sync python -m compileall -q src
通过
git diff --check
通过
```

## 环境说明

当前环境对依赖下载存在网络限制，普通 `uv sync` 未能完成下载；验证使用 `.venv` 和 `--no-sync`，并复用了本机已有包。正常网络环境下应直接执行 `uv sync`，不依赖系统包。

## 已知边界

- 全量测试仍包含尚未实现的 Task 3 红测试，不能作为本次开发环境变更的绿色验收标准。
