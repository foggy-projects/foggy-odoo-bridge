# Vendored foggy-python 同步

从 foggy-data-mcp-bridge-python 同步 vendored 代码到 Odoo 插件的 `foggy_mcp/lib/foggy/`。

## 触发条件

当用户需要更新 vendored foggy-python 代码、Python 团队有代码更新，或使用 `/vendor-sync` 时触发。

## 操作步骤

### 1. 同步代码

```bash
cd D:/foggy-projects/foggy-data-mcp/foggy-odoo-bridge

PY_SRC="../foggy-data-mcp-bridge-python/src/foggy"
VENDOR="foggy_mcp/lib/foggy"

rm -rf "$VENDOR" && mkdir -p "$VENDOR"
cp -r "$PY_SRC"/{__init__.py,core,dataset,dataset_model,mcp_spi,bean_copy} "$VENDOR/"
find "$VENDOR" -name "__pycache__" -exec rm -rf {} +

echo "Vendored. Size:"
du -sh "$VENDOR"
```

### 2. 验证

```bash
# 检查关键模块存在
python -c "
import sys; sys.path.insert(0, 'foggy_mcp/lib')
from foggy.mcp_spi import LocalDatasetAccessor
from foggy.dataset_model.semantic import SemanticQueryService
print('OK: key modules importable')
"
```

### 3. 升级 Odoo 模块

```bash
bash scripts/upgrade_module.sh
```

## 包含的模块

| 目录 | 说明 |
|------|------|
| `core/` | 核心工具类 |
| `dataset/` | 数据库抽象层 (MySQL/PG/MSSQL/SQLite) |
| `dataset_model/` | 语义查询引擎 (TM/QM/Service) |
| `mcp_spi/` | SPI 接口 (LocalDatasetAccessor, SemanticQueryRequest) |
| `bean_copy/` | Bean 转换 |

## 不包含的模块

| 目录 | 原因 |
|------|------|
| `mcp/` | FastAPI 服务器（Odoo 有自己的 HTTP 层） |
| `demo/` | 示例数据（Odoo 有自己的 demo 数据） |
| `fsscript/` | FSScript 引擎（仅 Python 独立服务需要） |

## 重要提醒

- **pip 版本优先**: `foggy_mcp/__init__.py` 中 `sys.path.insert(0, lib_dir)`，如果 pip 安装了 foggy 包则优先使用 pip 版本
- **同步后务必测试**: 至少运行内嵌模式健康检查和一个基础查询
- **不要手动修改 vendored 代码**: 所有修复应在 `foggy-data-mcp-bridge-python` 源码中完成，然后同步
