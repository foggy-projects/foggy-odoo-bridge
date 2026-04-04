# P0-community 模型注册中心消费与公开同步 — Execution Prompt

## 基本信息

- 目标版本：`v1.1`
- 上游文档：
  - 需求：`docs/v1.1/P0-community-model-registry-consumer-需求.md`
  - 代码清单：`docs/v1.1/P0-community-model-registry-consumer-code-inventory.md`
  - 实施计划：`docs/v1.1/P0-community-model-registry-consumer-implementation-plan.md`
- 进度报告模板：`docs/v1.1/P0-community-model-registry-consumer-progress.md`

## 开工提示词

你现在负责在 `foggy-odoo-bridge` 仓库中实现 community 模型的 registry 消费与公开同步。

### 你需要先读的文档

1. `docs/v1.1/P0-community-model-registry-consumer-需求.md` — 目标和验收标准
2. `docs/v1.1/P0-community-model-registry-consumer-code-inventory.md` — 代码触点
3. `docs/v1.1/P0-community-model-registry-consumer-implementation-plan.md` — 步骤和顺序

### 你需要做的事

按 implementation plan 的 Step 1-6 顺序执行：

1. **创建 `scripts/sync-community-models.sh`**：从 registry 拉取 community bundle
   - 硬编码 `--edition community`，不允许传 pro
   - 解包到 `foggy_mcp/setup/foggy-models/`
   - 写 `models.lock.json`

2. **创建 `scripts/check-no-pro-content.sh`**：确认不包含 pro 模型
   - 检查已知 pro-only 文件：`OdooMrpProductionModel.tm`、`OdooProjectTaskModel.tm` 及对应 QM
   - 存在时退出非零

3. **生成初始 lock 文件**：运行一次 sync，提交 `models.lock.json`

4. **创建 `scripts/check-model-drift.sh`**：漂移校验（与 Java/Python 仓相同逻辑）

5. **添加 GENERATED 标记**：在模型目录下添加 `GENERATED.md`

6. **文档化提交流程**：明确 sync → check-no-pro → review diff → commit 的标准流程

### 你不需要做的事

- 不拉取 pro bundle
- 不修改 Odoo 插件加载逻辑
- 不在 commit 钩子中自动 sync
- 不做 CI 自动 pull

### 关键约束

**本仓是公开仓库，绝对不能包含 pro 模型内容。** `check-no-pro-content.sh` 是安全底线。

### 验收方式

```bash
# 1. 同步 community bundle
bash scripts/sync-community-models.sh \
  --registry ../foggy-model-registry/data \
  --channel stable

# 2. 确认 lock 文件
cat foggy_mcp/setup/foggy-models/models.lock.json

# 3. pro 内容检查 — 应通过
bash scripts/check-no-pro-content.sh

# 4. 手动放入 pro 模型 → 应失败
touch foggy_mcp/setup/foggy-models/model/OdooMrpProductionModel.tm
bash scripts/check-no-pro-content.sh
# 期望：exit 1
rm foggy_mcp/setup/foggy-models/model/OdooMrpProductionModel.tm

# 5. 漂移校验
bash scripts/check-model-drift.sh
```

### 执行完成后

创建 `docs/v1.1/P0-community-model-registry-consumer-progress.md`，按模板格式填写完成状态。
