# P0-community 模型注册中心消费与公开同步-需求

## 基本信息

- 目标版本：`v1.1`
- 需求等级：`P0`
- 状态：`待处理`
- 责任项目：`foggy-odoo-bridge`

## 背景

社区版仓库后续不再作为 Odoo TM/QM 的权威来源，而是作为 `community` 公开消费与公开发布面。

随着 `foggy-model-registry` 引入，本仓需要从“手工持有模型副本”切换到“显式同步 community bundle 并写 lock”。

## 问题定义

如果社区版继续直接维护模型副本，会持续存在：

- 与 authority 漂移
- 与 `pro` 版本差异难以追溯
- 公开仓误带不该公开内容的风险

## 目标

- 本仓只消费 `community` bundle
- 公开仓提交流程基于显式同步和 lock 文件
- 本仓不保留任何 `pro` 模型内容

## 任务拆分

### 1. community 同步入口

- 增加从 `foggy-model-registry` 拉取 community bundle 的脚本或流程
- 拉取后生成或更新 lock 文件

### 2. 公开仓边界

- 不接触 `pro` bundle
- 不提交 `pro` 相关模型
- 不从 `foggy-odoo-bridge-pro` 目录直接拷贝模型

### 3. 提交流程

- 显式同步
- review diff
- 提交 bundle 对应内容和 lock 文件
- 禁止在 commit 钩子中自动拉取可变 `latest`

## 验收标准

- community 仓能从 registry 拉取固定版本 community bundle
- 提交内容可通过 lock 文件复现
- 本仓不再手工维护权威模型源码
- 本仓不包含任何 `pro` 模型内容

## 非目标

- 本条不负责 registry 服务实现
- 本条不负责 key 校验逻辑
- 本条不负责 authority 仓拆分
