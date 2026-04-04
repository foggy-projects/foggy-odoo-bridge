# P0-community 模型注册中心消费与公开同步 — Implementation Plan

## 基本信息

- 目标版本：`v1.1`
- 上游需求：`docs/v1.1/P0-community-model-registry-consumer-需求.md`
- 代码清单：`docs/v1.1/P0-community-model-registry-consumer-code-inventory.md`
- 仓库：`foggy-odoo-bridge`

## 前置条件

- `foggy-model-registry` Stage 1 已完成
- `foggy-odoo-bridge-pro` Stage 2 已完成（model-manifest.json 明确了 community / pro 边界）
- registry 已发布 community bundle
- Java / Python 消费侧已完成 Stage 3（非强制，但建议先跑通消费流程）

## 实施步骤

### Step 1. 创建 sync 脚本

在 `scripts/sync-community-models.sh` 创建同步入口：

1. 硬编码 `--edition community`，不允许传 pro
2. 从 registry 拉取 community bundle（本地或 HTTP）
3. 解包到 `foggy_mcp/setup/foggy-models/`
4. 写/更新 `models.lock.json`

支持参数：
- `--registry <url-or-path>`（默认 `../foggy-model-registry/data`）
- `--channel <stable|beta>`（默认 `stable`）

验收：运行后模型目录只包含 community 模型，lock 文件已更新。

### Step 2. 创建 pro 内容防泄漏检查

在 `scripts/check-no-pro-content.sh` 创建检查脚本：

1. 扫描 `foggy_mcp/setup/foggy-models/model/` 和 `query/` 目录
2. 检查是否存在已知 pro-only 模型文件（`OdooMrpProductionModel.tm`、`OdooProjectTaskModel.tm` 及对应 QM）
3. 存在时退出非零

验收：
- 当前 community 仓不含 pro 模型时通过
- 手动放入 pro 模型后失败

### Step 3. 创建 lock 文件初始版本

手动运行一次 sync，生成首个 `models.lock.json`，提交到 git。

验收：lock 文件存在且内容完整。

### Step 4. 创建 CI 漂移校验脚本

在 `scripts/check-model-drift.sh` 创建校验入口（与 Java/Python 仓相同逻辑）。

验收：未修改时通过，手改后失败。

### Step 5. 标记模型目录为 generated

在 `foggy_mcp/setup/foggy-models/` 下添加 `GENERATED.md`：

```
本目录由 foggy-model-registry community bundle 同步生成，禁止手工修改。
使用 scripts/sync-community-models.sh 更新。
```

验收：文件存在。

### Step 6. 定义公开仓提交流程

在 sync 后的工作流中确认：

1. 运行 `sync-community-models.sh`
2. 运行 `check-no-pro-content.sh` 确认无 pro 内容
3. `git diff` review 模型变更
4. 提交 lock 文件 + 模型变更
5. 禁止在 commit 钩子中自动 sync

验收：流程文档化，开发者可按步骤操作。

## 不做的事

- 不拉取 pro bundle
- 不修改 Odoo 插件加载逻辑
- 不在 commit 钩子中自动同步
- 不做 CI 中自动 pull

## 预估工作量

| Step | 预估 | 说明 |
|------|------|------|
| 1. sync 脚本 | 20 min | shell 脚本，固定 community |
| 2. pro 防泄漏 | 15 min | 文件名检查 |
| 3. 初始 lock | 5 min | 运行一次 sync |
| 4. 漂移校验 | 15 min | checksum 比对 |
| 5. GENERATED 标记 | 5 min | 文档 |
| 6. 提交流程文档 | 10 min | 流程说明 |
| **合计** | **~1h 10min** | |
