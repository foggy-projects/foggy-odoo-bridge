# Python MCP Server 启动管理

一键启动 Python 独立 MCP Server，通过 TM/QM 文件加载 Odoo 模型。

## 触发条件

当用户需要启动 Python 独立 MCP Server、测试 Python 网关模式，或使用 `/python-mcp` 时触发。

## 操作步骤

### 1. 停止旧进程

```powershell
Get-NetTCPConnection -LocalPort 8066 -ErrorAction SilentlyContinue | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
```

### 2. 启动

```bash
cd D:/foggy-projects/foggy-data-mcp/foggy-odoo-bridge
python scripts/start_python_mcp.py --port 8066
```

**自定义参数**：
```bash
python scripts/start_python_mcp.py \
  --port 8066 \
  --db-host localhost --db-port 5432 \
  --db-user odoo --db-password odoo --db-name odoo_demo \
  --model-dir /path/to/custom/templates
```

### 3. 验证

```bash
# 健康检查
curl http://localhost:8066/health

# 列出模型（应有 18 个：9 TM + 9 QM）
curl http://localhost:8066/api/v1/models

# 查询测试（无需 namespace header）
curl -X POST http://localhost:8066/api/v1/query/OdooResCompanyQueryModel \
  -H 'Content-Type: application/json' \
  -d '{"columns": ["name"], "limit": 5}'
```

## 重要提醒

- **无需 namespace**: Python 引擎没有 bundle/namespace 概念，模型直接注册到全局（与 Java 不同）
- **SPI Mock**: 启动脚本自动 monkey-patch `FileModuleLoader` 处理 `@jdbcModelDictService` 等 Java SPI 导入
- **TM/QM 文件**: 默认从 Java 项目复用 `foggy-data-mcp-bridge/addons/foggy-odoo-bridge-java/src/main/resources/foggy/templates/odoo/`
- **数据源名**: TM 文件中 `dataSourceName: 'odoo'`，启动时 `DataSourceConfig(name='odoo', ...)` 必须匹配
- **依赖**: 使用 `foggy-data-mcp-bridge-python/src` 完整源码（含 `foggy.mcp` 模块），非 vendored 子集
