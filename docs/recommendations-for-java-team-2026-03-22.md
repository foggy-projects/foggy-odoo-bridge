# Odoo Bridge 团队 → Java 团队：三引擎对比测试建议

**日期**: 2026-03-22
**背景**: 三引擎对比测试全部 15/15 通过（内嵌 Python / Java 网关 / Python 网关）

---

## 1. `selfAndDescendantsOf` 修复已验证通过 ✅

之前 T01 (res_partner) 因 `selfAndDescendantsOf` 在 `$or` 内不支持导致 0 行。本次 rebuild 后验证通过，返回 61 行。感谢修复！

---

## 2. FSScript `@service` 导入规范化

### 现状

TM 文件中 `dicts.fsscript` 使用 `import { registerDict } from '@jdbcModelDictService'`。Python FSScript 引擎不识别 `@` 前缀的 SPI 服务导入，需要在外部 monkey-patch 一个 mock。

### 建议

- 在 FSScript 文档中明确 **SPI 服务清单及其导出接口**，方便 Python 侧做准确的 mock
- 或在 FSScript 引擎规范中约定：`@` 前缀模块在找不到 SPI 提供者时 fallback 为 no-op stub（而非抛异常）
- 当前已知的 SPI 服务：`@jdbcModelDictService`（导出 `registerDict` 函数）

---

## 3. Namespace 传递方式文档化

### 现状

Java 引擎通过 `@EnableFoggyFramework(bundleName = "odoo", namespace = "odoo")` 注册 Odoo 模型到 `odoo` namespace。调用 MCP 端点时必须通过 **`X-NS: odoo` HTTP 请求头**传递 namespace，否则返回：

```
查询执行失败: 资源OdooResPartnerQueryModel.qm (在默认命名空间中)不存在
```

### 建议

- 在 MCP 端点文档（`/mcp/analyst/rpc`、`/mcp/business/rpc`）中显式说明 `X-NS` header 的用法
- 考虑在 `dataset.get_metadata` 的响应中标注各模型所属的 namespace
- 或提供一个 `namespaces/list` API 让客户端发现可用 namespace

Python 网关不需要 namespace（模型直接注册到全局），这个差异也建议在文档中注明。

---

## 4. Docker 镜像更新

### 现状

- Docker Hub 上 `foggysource/foggy-odoo-mcp:v8.1.8-beta` 拉取失败（403 Forbidden）
- 本地 `foggysource/foggy-dataset-mcp:latest` 是旧版本，没有 `selfAndDescendantsOf` 修复
- `docker/odoo/Dockerfile` 存在但对应镜像未构建发布

### 建议

- CI/CD 流程在 merge 后自动构建 `docker/odoo/Dockerfile` 并推送到 Docker Hub
- 建议镜像标签与 Maven 版本一致（如 `foggysource/foggy-odoo-mcp:8.1.8-beta`）
- Odoo Bridge 的 `docker-compose.yml` 可以直接引用最新镜像，无需手动 build

---

## 5. 数据源 API 一致性

### 现状

- `foggy-mcp-launcher` 启动后数据源从 `~/.foggy/datasources/odoo.json` 自动恢复 ✅
- 但 `/api/v1/datasource` REST API 在某些启动配置下不可用（404）
- `foggy-dataset-mcp` 模块单独运行时也没有这个 API

### 建议

- 确保所有启动方式（JAR / Docker / spring-boot:run）都暴露 `/api/v1/datasource` API
- 或在文档中明确哪些 profile 下可用、哪些不可用

---

## 联系

如有疑问，请联系 Odoo Bridge 团队。
