# Java MCP Server 启动管理

一键 build 并启动 Java MCP Server（foggy-data-mcp-bridge），用于 Java 网关模式测试。

## 触发条件

当用户需要启动、重启、停止 Java MCP Server，或使用 `/java-mcp` 时触发。

## 操作步骤

### 1. 构建

```bash
cd D:/foggy-projects/foggy-data-mcp/foggy-data-mcp-bridge
mvn clean package -pl foggy-mcp-launcher -am -DskipTests
```

JAR 文件：`foggy-mcp-launcher/target/foggy-mcp-launcher-8.1.9.beta.jar`

### 2. 停止旧进程

```powershell
Get-NetTCPConnection -LocalPort 7108 -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
```

### 3. 启动

```bash
cd D:/foggy-projects/foggy-data-mcp/foggy-data-mcp-bridge
java -jar foggy-mcp-launcher/target/foggy-mcp-launcher-8.1.9.beta.jar \
  --spring.profiles.active=lite,odoo \
  --server.port=7108 \
  --spring.datasource.url=jdbc:postgresql://localhost:5432/odoo_demo \
  --spring.datasource.username=odoo \
  --spring.datasource.password=odoo \
  --foggy.auth.token=
```

**关键参数**：
- `--spring.profiles.active=lite,odoo` — 必须包含 `odoo` profile 才加载 Odoo 模型
- `--foggy.auth.token=` — 空值表示不启用认证（开发环境）

### 4. 验证

```bash
# 健康检查
curl http://localhost:7108/actuator/health

# 查询测试（注意 X-NS header！）
curl -X POST http://localhost:7108/mcp/analyst/rpc \
  -H 'Content-Type: application/json' \
  -H 'X-NS: odoo' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"dataset.query_model","arguments":{"model":"OdooResCompanyQueryModel","payload":{"columns":["name"],"limit":5}}}}'
```

## 重要提醒

- **Namespace**: 所有查询请求必须带 `X-NS: odoo` header，否则返回 `资源不存在（在默认命名空间中）`
- **数据源持久化**: 首次通过 API 配置的数据源会保存到 `~/.foggy/datasources/odoo.json`，之后自动恢复
- **入口模块**: `foggy-mcp-launcher`（不是 `foggy-dataset-mcp`）
- **Odoo 侧 FoggyClient**: 自动通过 `X-NS` header 传递 namespace（配置 `foggy_mcp.namespace`）
