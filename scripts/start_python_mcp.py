#!/usr/bin/env python3
"""
启动 Python 独立 MCP Server，通过 TM/QM 文件加载 Odoo 模型。

使用 foggy-python 引擎原生的 FSScript TM/QM 加载能力，
从 Java 项目中复用同一套模型定义文件。

用法：
    python scripts/start_python_mcp.py
    python scripts/start_python_mcp.py --port 8066 --db-host localhost --db-name odoo_demo

TM/QM 文件位置：
    foggy-data-mcp-bridge/addons/foggy-odoo-bridge-java/
      src/main/resources/foggy/templates/odoo/
"""
import sys
import os
import argparse
import logging

# foggy-python 源码（完整版，含 foggy.mcp）优先于 vendored
PY_SRC = os.path.join(os.path.dirname(__file__), '..', '..', 'foggy-data-mcp-bridge-python', 'src')
sys.path.insert(0, os.path.abspath(PY_SRC))

# TM/QM 模型文件目录（Java 项目中的 Odoo 模型定义）
DEFAULT_MODEL_DIR = os.path.normpath(os.path.join(
    os.path.dirname(__file__), '..', '..',
    'foggy-data-mcp-bridge', 'addons', 'foggy-odoo-bridge-java',
    'src', 'main', 'resources', 'foggy', 'templates', 'odoo'
))


def _patch_fsscript_spi_services():
    """Patch FSScript FileModuleLoader to handle Java SPI service imports.

    TM/QM files import from '@jdbcModelDictService' etc. — these are Java-side
    SPI services unavailable in Python. We monkey-patch the loader to return
    no-op stubs for any '@'-prefixed module imports.
    """
    from foggy.fsscript.module_loader import FileModuleLoader

    _original_load = FileModuleLoader.load_module

    def _patched_load(self, module_path, context):
        # '@' prefixed = Java SPI service → return mock with no-op registerDict
        if module_path.startswith('@'):
            mock_exports = {
                'registerDict': lambda d: d,  # registerDict just returns the dict
            }
            logging.getLogger(__name__).debug(
                "SPI mock for %s — returning no-op stub", module_path
            )
            return mock_exports
        return _original_load(self, module_path, context)

    FileModuleLoader.load_module = _patched_load



def main():
    parser = argparse.ArgumentParser(description='Start Python MCP Server with Odoo TM/QM models')
    parser.add_argument('--host', default='0.0.0.0')
    parser.add_argument('--port', type=int, default=8066)
    parser.add_argument('--db-host', default='localhost')
    parser.add_argument('--db-port', type=int, default=5432)
    parser.add_argument('--db-user', default='odoo')
    parser.add_argument('--db-password', default='odoo')
    parser.add_argument('--db-name', default='odoo_demo')
    parser.add_argument('--model-dir', default=DEFAULT_MODEL_DIR,
                        help='TM/QM model directory (default: Java project odoo templates)')
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # Patch FSScript before any TM loading
    _patch_fsscript_spi_services()

    from foggy.mcp.launcher.app import create_app, McpProperties, DataSourceConfig, DataSourceType
    import uvicorn

    model_dir = os.path.abspath(args.model_dir)
    print(f"\n=== Python MCP Server for Odoo ===")
    print(f"  Model dir : {model_dir}")
    print(f"  Database  : {args.db_host}:{args.db_port}/{args.db_name}")
    print(f"  Port      : {args.port}")

    if not os.path.exists(model_dir):
        print(f"\n  ⚠ Model directory not found: {model_dir}")
        print(f"    Please check the path or use --model-dir to specify.")
        sys.exit(1)

    properties = McpProperties(
        host=args.host,
        port=args.port,
        model_directories=[model_dir],
    )

    ds_config = DataSourceConfig(
        name='odoo',
        source_type=DataSourceType.POSTGRESQL,
        host=args.db_host,
        port=args.db_port,
        database=args.db_name,
        username=args.db_user,
        password=args.db_password,
    )

    app = create_app(
        properties=properties,
        data_source_configs=[ds_config],
        load_demo_models=False,
    )

    print(f"\n  Starting on http://{args.host}:{args.port}")
    print(f"  Health: http://localhost:{args.port}/health")
    print(f"  MCP:    http://localhost:{args.port}/mcp/analyst")
    print()

    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == '__main__':
    main()
