# -*- coding: utf-8 -*-
"""Public MCP tool names exposed by the Odoo bridge.

Strict MCP clients accept only ``^[a-zA-Z0-9_-]{1,64}$`` tool names. The
embedded/gateway engines still use the historical dot names internally, so the
Odoo bridge translates at the public MCP boundary.
"""

TOOL_QUERY_MODEL = "dataset__query_model"
TOOL_LIST_MODELS = "dataset__list_models"
TOOL_GET_METADATA = "dataset__get_metadata"
TOOL_DESCRIBE_MODEL = "dataset__describe_model_internal"

ENGINE_TOOL_QUERY_MODEL = "dataset.query_model"
ENGINE_TOOL_LIST_MODELS = "dataset.list_models"
ENGINE_TOOL_GET_METADATA = "dataset.get_metadata"
ENGINE_TOOL_DESCRIBE_MODEL = "dataset.describe_model_internal"

PUBLIC_TO_ENGINE_TOOL_NAMES = {
    TOOL_QUERY_MODEL: ENGINE_TOOL_QUERY_MODEL,
    TOOL_LIST_MODELS: ENGINE_TOOL_LIST_MODELS,
    TOOL_GET_METADATA: ENGINE_TOOL_GET_METADATA,
    TOOL_DESCRIBE_MODEL: ENGINE_TOOL_DESCRIBE_MODEL,
}

ENGINE_TO_PUBLIC_TOOL_NAMES = {
    engine_name: public_name
    for public_name, engine_name in PUBLIC_TO_ENGINE_TOOL_NAMES.items()
}

PUBLIC_TOOL_NAMES = frozenset(PUBLIC_TO_ENGINE_TOOL_NAMES)
ENGINE_TOOL_NAMES = frozenset(ENGINE_TO_PUBLIC_TOOL_NAMES)


def to_public_tool_name(name):
    """Return the public MCP-safe name for an engine/internal tool name."""
    return ENGINE_TO_PUBLIC_TOOL_NAMES.get(name, name)


def to_engine_tool_name(name):
    """Return the engine/internal name for a public MCP tool name."""
    return PUBLIC_TO_ENGINE_TOOL_NAMES.get(name, name)


def replace_tool_name_mentions(text):
    """Replace dot-name mentions in tool descriptions with public names."""
    if not isinstance(text, str) or not text:
        return text
    for engine_name, public_name in ENGINE_TO_PUBLIC_TOOL_NAMES.items():
        text = text.replace(engine_name, public_name)
    return text
