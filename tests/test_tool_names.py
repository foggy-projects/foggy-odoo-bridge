"""Unit tests for public MCP-safe tool name mapping."""
import ast
import importlib.util
import os
import re
import sys
from pathlib import Path


_bridge_dir = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), 'foggy_mcp', 'services'
)
_spec = importlib.util.spec_from_file_location(
    'foggy_mcp.services.tool_names',
    os.path.join(_bridge_dir, 'tool_names.py'),
)
_tool_names = importlib.util.module_from_spec(_spec)
sys.modules['foggy_mcp.services.tool_names'] = _tool_names
_spec.loader.exec_module(_tool_names)


def test_public_tool_names_match_strict_mcp_regex():
    pattern = re.compile(r'^[a-zA-Z0-9_-]{1,64}$')

    for name in _tool_names.PUBLIC_TOOL_NAMES:
        assert pattern.fullmatch(name), name
    assert 'dataset__get_metadata' not in _tool_names.PUBLIC_TOOL_NAMES
    assert 'dataset.get_metadata' not in _tool_names.ENGINE_TOOL_NAMES


def test_engine_tool_names_translate_to_public_names():
    assert _tool_names.to_public_tool_name('dataset.query_model') == 'dataset__query_model'
    assert _tool_names.to_public_tool_name('dataset.list_models') == 'dataset__list_models'
    assert (
        _tool_names.to_public_tool_name('dataset.describe_model_internal')
        == 'dataset__describe_model_internal'
    )


def test_public_tool_names_translate_to_engine_names():
    assert _tool_names.to_engine_tool_name('dataset__query_model') == 'dataset.query_model'
    assert _tool_names.to_engine_tool_name('dataset__list_models') == 'dataset.list_models'


def test_descriptions_use_public_tool_names():
    text = 'Call dataset.query_model after dataset.list_models.'

    assert _tool_names.replace_tool_name_mentions(text) == (
        'Call dataset__query_model after dataset__list_models.'
    )


def test_public_tool_definition_source_is_english_only():
    backend_source = Path(_bridge_dir, 'embedded_backend.py').read_text(encoding='utf-8')
    backend_tree = ast.parse(backend_source)
    tool_node = next(
        node
        for class_node in backend_tree.body
        if isinstance(class_node, ast.ClassDef) and class_node.name == 'EmbeddedBackend'
        for node in class_node.body
        if isinstance(node, ast.FunctionDef) and node.name == '_build_tool_definitions'
    )
    tool_definition_source = ast.get_source_segment(backend_source, tool_node)

    assert not re.search(r'[\u4e00-\u9fff]', tool_definition_source)
