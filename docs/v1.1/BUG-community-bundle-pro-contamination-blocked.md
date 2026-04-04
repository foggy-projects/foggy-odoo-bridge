# BUG: community bundle 包含 pro 模型导致同步被阻塞

## 基本信息

- 版本：`v1.1`
- 优先级：P0（阻塞 community consumer 实施）
- 状态：`blocked`（等待上游修复）
- 分类：bug（跨仓，消费侧视角）
- 关联仓库：
  - `foggy-model-registry`（根因，见 `foggy-model-registry/docs/v1.0/BUG-community-bundle-contains-pro-models.md`）
  - `foggy-odoo-bridge`（本文档）

## 背景

foggy-odoo-bridge v1.1 community consumer 实施（`P0-community-model-registry-consumer-implementation-plan.md`）的 Step 1 运行 `scripts/sync-community-models.sh` 时，脚本内置的 pro 内容守卫检测到拉取的 community bundle 中包含 pro-only 模型，正确拦截并退出。

## 问题描述

`foggy-model-registry` 发布的 `foggy.odoo.community@1.1.0` bundle 包含 30 个模型，其中混入：

- `OdooMrpProductionModel.tm` / `OdooMrpProductionQueryModel.qm`
- `OdooProjectTaskModel.tm` / `OdooProjectTaskQueryModel.qm`

`sync-community-models.sh` 的 pro 守卫在 staging 阶段检测到这些文件后拒绝写入，因此：

- `models.lock.json` 无法通过 sync 生成（当前为手动创建的 `1.1.0-local` 临时版本）
- `check-model-drift.sh` 可基于手动 lock 正常工作
- `check-no-pro-content.sh` 在当前模型目录（纯 community）上正常通过

## 当前临时措施

手动生成 `foggy_mcp/setup/foggy-models/models.lock.json`，标记 `version: "1.1.0-local"`、`checksum: "manual"`，`content_checksum` 基于当前 12 TM + 12 QM + 2 fsscript 计算。漂移校验和 pro 防泄漏脚本均可正常工作。

## 目标结果

上游 `foggy-model-registry` 修复 community bundle 后：

1. 运行 `scripts/sync-community-models.sh` 端到端通过
2. `models.lock.json` 更新为正式版本（非 `local`）
3. 提交正式 lock 文件 + 模型变更到 git

## 验收标准

1. `sync-community-models.sh` 运行成功，无 pro 内容拦截
2. `check-no-pro-content.sh` 通过
3. `check-model-drift.sh` 通过
4. `models.lock.json` 中 `version` 不含 `local` 标记

## 约束

- 不绕过 pro 内容守卫
- 不手动从 bundle 中删除 pro 模型再同步
- 等待上游正式修复

## 进度追踪

### 开发进度

- [x] `sync-community-models.sh` 已创建，功能正常
- [x] `check-no-pro-content.sh` 已创建并验证
- [x] `check-model-drift.sh` 已创建并验证
- [x] `GENERATED.md` 已创建
- [x] 临时 `models.lock.json` 已手动生成
- [ ] 上游修复后运行正式 sync（blocked）
- [ ] 提交正式 lock 文件（blocked）

### 测试进度

- [x] `check-no-pro-content.sh` 正反例验证通过
- [x] `check-model-drift.sh` 正反例验证通过
- [ ] 端到端 sync 验证（blocked on upstream）

### 经验进度

- N/A（基础设施脚本，无用户面操作）
