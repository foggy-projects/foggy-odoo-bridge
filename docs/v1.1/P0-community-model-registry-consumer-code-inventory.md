# P0-community 模型注册中心消费与公开同步 — Code Inventory

## 基本信息

- 目标版本：`v1.1`
- 上游需求：`docs/v1.1/P0-community-model-registry-consumer-需求.md`
- 仓库：`foggy-odoo-bridge`

## 当前 Odoo 模型位置

社区版 Odoo TM/QM 当前位于 `foggy_mcp/setup/foggy-models/`，包含 12 个 community 模型（无 mrp / project），随 Odoo 插件分发。

## Code Inventory

### Odoo 模型目录（现有）

- repo: `foggy-odoo-bridge`
- path: `foggy_mcp/setup/foggy-models/`
- role: 当前 community TM/QM 存放位置（12 TM + 12 QM）
- expected change: `update`
- notes: 后续由 sync 脚本从 registry community bundle 同步；需标记为 generated

### sync 脚本

- repo: `foggy-odoo-bridge`
- path: `scripts/sync-community-models.sh`
- role: 从 registry 拉取 community bundle 并同步到模型目录
- expected change: `create`
- notes: 只拉取 community edition；禁止拉取 pro bundle；拉取后写 lock 文件

### lock 文件

- repo: `foggy-odoo-bridge`
- path: `foggy_mcp/setup/foggy-models/models.lock.json`
- role: 锁定当前消费的 community bundle 版本和 checksum
- expected change: `create`
- notes: 提交到 git；公开仓的提交内容可通过 lock 复现

### CI 漂移校验脚本

- repo: `foggy-odoo-bridge`
- path: `scripts/check-model-drift.sh`
- role: CI 阶段校验模型目录与 lock 文件一致性
- expected change: `create`
- notes: 与 Java/Python 仓相同逻辑

### pro 内容防泄漏检查

- repo: `foggy-odoo-bridge`
- path: `scripts/check-no-pro-content.sh`
- role: CI 阶段确认不包含 pro 模型
- expected change: `create`
- notes: 检查模型目录中不存在 `OdooMrpProductionModel` / `OdooProjectTaskModel` 等 pro-only 模型

### embedded backend（现有）

- repo: `foggy-odoo-bridge`
- path: `foggy_mcp/services/embedded_backend.py`（或等效入口）
- role: 社区版内嵌模型加载
- expected change: `read-only-analysis`
- notes: 确认加载路径与 sync 后目录结构兼容
