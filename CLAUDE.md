# Foggy Odoo Bridge - Claude Memory

> **开源项目，请勿上传私有 key、账号密码、token 等敏感信息。**

## 项目概览

Odoo 17 插件，让 AI 助手（Claude、Cursor 等）通过 MCP 协议查询 Odoo 业务数据。

**GitHub**: https://github.com/foggy-projects/foggy-odoo-bridge

## 架构：双引擎模式

```
AI Client ──MCP──→ Odoo foggy_mcp 插件
                   ├── 内嵌模式（默认）：Python 引擎进程内运行，零外部依赖
                   └── 网关模式：HTTP 转发到外部 Java/Python 服务器
```

### 引擎抽象层

```
mcp_controller.py → EngineBackend（抽象接口）
                     ├── GatewayBackend  → FoggyClient（HTTP 转发）
                     └── EmbeddedBackend → vendored foggy-python（进程内）
```

配置：`foggy_mcp.engine_mode` = `'embedded'`（默认） | `'gateway'`

## 项目结构

```
foggy-odoo-bridge/
├── foggy_mcp/                    ← Odoo 17 插件主目录
│   ├── __init__.py               ← sys.path 注入 lib/（vendored foggy）
│   ├── __manifest__.py           ← Odoo 模块清单
│   ├── lib/foggy/                ← vendored foggy-python 核心引擎
│   │   ├── core/                 ←   工具类
│   │   ├── dataset/              ←   数据库抽象层（MySQL/PG/MSSQL/SQLite）
│   │   ├── dataset_model/        ←   语义查询引擎（TM/QM 加载 + SQL 构建）
│   │   ├── mcp_spi/              ←   SPI 接口（DatasetAccessor）
│   │   └── bean_copy/            ←   Bean 转换
│   ├── setup/foggy-models/       ← TM/QM 模型文件（内嵌模式的权威源）
│   │   ├── model/*.tm            ←   9 个 Table Model 定义
│   │   └── query/*.qm            ←   9 个 Query Model 定义
│   ├── controllers/
│   │   ├── mcp_controller.py     ← MCP JSON-RPC 端点（认证+权限+路由）
│   │   └── chat_controller.py    ← AI 对话控制器
│   ├── services/
│   │   ├── engine_backend.py     ← EngineBackend 抽象接口
│   │   ├── gateway_backend.py    ← 网关模式（包装 FoggyClient）
│   │   ├── embedded_backend.py   ← 内嵌模式（包装 foggy-python）
│   │   ├── engine_factory.py     ← 工厂（根据配置创建后端）
│   │   ├── foggy_client.py       ← HTTP 客户端（网关模式用）
│   │   ├── tool_registry.py      ← 工具注册+权限过滤
│   │   ├── field_mapping_registry.py ← 字段映射（动态权限解析）
│   │   ├── permission_bridge.py  ← ir.rule → DSL slice 转换
│   │   └── llm_service.py        ← LLM 集成（OpenAI/Anthropic/DeepSeek/Ollama）
│   ├── models/
│   │   ├── foggy_config.py       ← Settings 配置（引擎模式+服务器+LLM）
│   │   ├── foggy_api_key.py      ← API 密钥认证（fmcp_ 前缀）
│   │   ├── foggy_chat.py         ← 对话模型
│   │   └── connection_test_result.py
│   ├── wizard/
│   │   ├── foggy_setup_wizard.py ← 设置向导（动态步骤，按引擎模式）
│   │   └── foggy_setup_wizard_views.xml
│   ├── views/                    ← 全中文 UI
│   ├── security/                 ← ir.model.access + ir.rule
│   ├── setup/                    ← TM/QM 文件（网关模式参考）
│   ├── static/                   ← OWL 聊天组件
│   └── i18n/zh_CN.po
├── docker-compose.yml            ← PG + Odoo 17 + Foggy MCP
├── scripts/
│   ├── upgrade_module.sh         ← 模块升级脚本（4步：stop→start→upgrade→restart）
│   └── upgrade_module.bat
├── tests/
│   ├── test_permission_bridge.py
│   ├── test_field_mapping_registry.py
│   └── e2e/                      ← 端到端测试
├── sql/refresh_closure_tables.sql
└── LICENSE                       ← Apache 2.0
```

## 关联项目

| 项目 | 路径 | 关系 |
|------|------|------|
| foggy-data-mcp-bridge | `../foggy-data-mcp-bridge` | Java 版 Foggy MCP Server（网关模式后端） |
| foggy-data-mcp-bridge-python | `../foggy-data-mcp-bridge-python` | Python 版引擎源码（vendored 到 `lib/`） |

## Vendored foggy-python

`foggy_mcp/lib/foggy/` 是从 `foggy-data-mcp-bridge-python` 精简 vendor 的核心引擎：
- 包含：`core/`, `dataset/`, `dataset_model/`, `mcp_spi/`, `bean_copy/`
- 不包含：`mcp/`（FastAPI 服务器）, `demo/`（示例数据）, `fsscript/`（脚本引擎）
- 大小：714K
- 加载方式：`__init__.py` 中 `sys.path.insert(0, lib_dir)`，pip 版本优先

**更新 vendor**：
```bash
PY_SRC="../foggy-data-mcp-bridge-python/src/foggy"
VENDOR="foggy_mcp/lib/foggy"
rm -rf "$VENDOR" && mkdir -p "$VENDOR"
cp -r "$PY_SRC"/{__init__.py,core,dataset,dataset_model,mcp_spi,bean_copy,fsscript} "$VENDOR/"
find "$VENDOR" -name "__pycache__" -exec rm -rf {} +
```

## Docker 环境

```yaml
# docker-compose.yml（项目根目录）
services:
  postgres:    # foggy-odoo-postgres, port 5432
  odoo:        # foggy-odoo, port 8069, mount ./foggy_mcp:/mnt/extra-addons/foggy_mcp:ro
  foggy-mcp:   # foggy-mcp-server, port 7108（网关模式用）
```

## 开发命令

```bash
# 模块升级（修改代码后执行）
bash scripts/upgrade_module.sh

# 手动升级
docker stop foggy-odoo && docker start foggy-odoo && sleep 5
docker exec foggy-odoo bash -c "odoo -d odoo -u foggy_mcp --stop-after-init"
docker restart foggy-odoo

# 单元测试
python -m pytest tests/test_permission_bridge.py -v

# E2E 测试（需 Odoo + Foggy MCP 运行中）
python -m pytest tests/e2e/ -v

# 健康检查
curl http://localhost:8069/foggy-mcp/health | python3 -m json.tool
```

## 权限桥接

```
ir.model.access → 工具级过滤（tools/list 按用户权限裁剪）
ir.rule → 行级过滤（域解析 → DSL slice → 注入 payload.slice）
失败关闭（fail-closed）：权限计算异常时拒绝访问
```

## 9 个 Odoo 模型

| 模型 | 表名 | 说明 |
|------|------|------|
| OdooSaleOrderModel | sale_order | 销售订单 |
| OdooSaleOrderLineModel | sale_order_line | 销售订单行 |
| OdooPurchaseOrderModel | purchase_order | 采购订单 |
| OdooAccountMoveModel | account_move | 会计分录/发票 |
| OdooStockPickingModel | stock_picking | 库存调拨 |
| OdooHrEmployeeModel | hr_employee | 员工 |
| OdooResPartnerModel | res_partner | 合作伙伴 |
| OdooResCompanyModel | res_company | 公司 |
| OdooCrmLeadModel | crm_lead | CRM 线索/商机 |

**内嵌模式**：`setup/foggy-models/` 下的 TM/QM 文件，由 `load_models_from_directory()` 加载
**网关模式**：由外部引擎（Java 或 Python）自行维护模型定义

## MCP 端点

- `/foggy-mcp/rpc` — MCP JSON-RPC 2.0（认证：Bearer fmcp_xxx 或 Session Cookie）
- `/foggy-mcp/health` — 健康检查（无需认证）

## Setup Wizard 流程

- **网关模式**：欢迎 → 部署 → 连接 → 数据源 → 闭包表 → 完成（6 步）
- **内嵌模式**：欢迎 → 闭包表 → 完成（3 步，跳过服务器部署）

## UI 语言

所有 UI 字符串直接使用中文（非 PO 文件翻译），包括：
- Settings 页面（引擎模式/服务器连接/LLM 配置）
- Setup Wizard（所有步骤）
- API 密钥管理
- 错误提示和状态消息

## 开发约定

- 不需要运行单元测试（`-DskipTests`）
- API 用户面向字符串使用中文
- 代码注释中英文均可
- 修改后运行 `bash scripts/upgrade_module.sh` 验证
- **模型统一使用 TM/QM 定义（阻塞规则）**：
  - 所有数据模型必须使用标准 TM（Table Model）/ QM（Query Model）文件定义，**禁止使用 Python 代码手写模型**
  - **内嵌模式**：模型文件由 Odoo 插件准备，存放在 `foggy_mcp/setup/foggy-models/`（`model/*.tm` + `query/*.qm`），由 `EmbeddedBackend` 通过 `load_models_from_directory()` 加载
  - **网关模式**：模型文件由外部引擎（Java 或 Python）自行维护，Odoo 侧不管模型定义
  - **单一源原则**：TM/QM 文件是模型的唯一权威源。三个引擎（内嵌 Python、Java 网关、Python 网关）共用同一套 TM/QM 定义，确保字段名、数据类型、JOIN 关系完全一致
  - 新增/修改模型时，直接编辑 `setup/foggy-models/` 下的 `.tm`/`.qm` 文件即可，无需修改 Python 代码
- **API 签名一致性原则（阻塞规则）**：内嵌 Python 引擎（`foggy-data-mcp-bridge-python`）与外部 Java 引擎（`foggy-data-mcp-bridge`）的业务方法签名**必须保持一致，以 Java 为标准**。当发现 Python API 参数签名（不管是 Python 内嵌引擎还是 Python 作为独立服务）与 Java 侧不符时：
  1. **立即停止**相关功能的开发工作
  2. 在 `docs/` 下创建签名不一致报告文档（格式参考 `docs/api-signature-mismatch-report-2026-03-21.md`）
  3. 将报告交付给 Python 团队和/或 Java 团队进行修改
  4. **待对方团队修复并更新 vendored 代码后，再继续开发**
  5. Odoo 侧**不做临时 workaround**（不创建 shim/adapter 层绕过签名差异）

## ✅ 已解决的 API 签名问题（2026-03-21）

> 原始报告：[`docs/api-signature-mismatch-report-2026-03-21.md`](docs/api-signature-mismatch-report-2026-03-21.md)
> Python 团队回复：[`foggy-data-mcp-bridge-python/docs/response-to-odoo-api-mismatch-2026-03-21.md`](../foggy-data-mcp-bridge-python/docs/response-to-odoo-api-mismatch-2026-03-21.md)
>
> **根因**：vendored 打包时遗漏 `foggy.mcp.spi` 模块。Python 团队已将类型定义迁入 `foggy.mcp_spi`，消除了循环依赖。
> **解决方式**：更新 vendored 代码 + `embedded_backend.py` 通过 `LocalDatasetAccessor` 调用。
> **状态**：✅ 已解决，内嵌模式正常工作。

## 三引擎对比测试（2026-03-22）

### 测试结果

三引擎 **15/15 全部通过**。详见 `tests/results/comparison-report.md`。

### 引擎启动方式

| 引擎 | 启动方式 | 端口 |
|------|---------|------|
| 内嵌 Python | Odoo 插件内嵌，`engine_mode=embedded` | N/A（进程内） |
| Java 网关 | `java -jar foggy-mcp-launcher-*.jar --spring.profiles.active=lite,odoo --server.port=7108` | 7108 |
| Python 网关 | `python scripts/start_python_mcp.py --port 8066` | 8066 |

### 关键经验

1. **Java 网关 namespace**：Java 引擎使用 bundle 系统，Odoo 模型注册在 `odoo` namespace。请求时必须通过 **`X-NS: odoo` HTTP 请求头**传递 namespace，否则会报 `资源不存在（在默认命名空间中）`。Odoo 侧 `FoggyClient` 通过 `X-NS` header 发送，Python 网关**不需要 namespace**（模型直接注册到全局）。

2. **Java 启动模块**：入口是 `foggy-mcp-launcher`（非 `foggy-dataset-mcp`），包含 Spring Boot main class `McpLauncherApplication`。构建命令：`mvn clean package -pl foggy-mcp-launcher -am -DskipTests`。

3. **Java 数据源持久化**：Java 引擎的数据源配置通过 REST API `POST /api/v1/datasource` 设置后自动持久化到 `~/.foggy/datasources/*.json`，重启自动恢复。

4. **Python TM/QM 加载**：
   - FSScript `@service` 导入需要 mock（`@jdbcModelDictService` 等 Java SPI 服务在 Python 不可用）
   - TM 文件中 `dataSourceName` 通过 `dicts.fsscript` 中的 `ODOO_DATA_SOURCE_NAME='odoo'` 常量定义
   - QM 文件通过 `loadTableModel('OdooSaleOrderModel')` 引用 TM

5. **Vendored 代码同步**：
   ```bash
   PY_SRC="../foggy-data-mcp-bridge-python/src/foggy"
   VENDOR="foggy_mcp/lib/foggy"
   rm -rf "$VENDOR" && mkdir -p "$VENDOR"
   cp -r "$PY_SRC"/{__init__.py,core,dataset,dataset_model,mcp_spi,bean_copy,fsscript} "$VENDOR/"
   find "$VENDOR" -name "__pycache__" -exec rm -rf {} +
   ```

### 测试脚本

- `tests/e2e/test_engine_comparison.py` — pytest 自动化对比测试（通过 Odoo MCP 端点）
- `scripts/start_python_mcp.py` — Python 独立 MCP Server 启动器

## License

Apache License 2.0
