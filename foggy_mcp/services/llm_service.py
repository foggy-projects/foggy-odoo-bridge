# -*- coding: utf-8 -*-
"""
LLM Service — orchestrates AI chat with Foggy MCP tool calling.

Flow:
    1. Build system prompt with available models
    2. Call LLM via openai / anthropic SDK (no litellm dependency)
    3. If LLM returns tool_use → execute through Foggy (with permission injection)
    4. Feed tool results back to LLM
    5. Return final assistant response

Supported providers:
    - OpenAI (gpt-4o, gpt-4o-mini, etc.)
    - OpenAI-compatible (DeepSeek, Ollama, vLLM, etc. — via base_url)
    - Anthropic (claude-3-5-sonnet, claude-3-haiku, etc.)
"""
import json
import logging

_logger = logging.getLogger(__name__)

# Maximum tool calling rounds to prevent infinite loops
# Most queries complete in 1-3 rounds; complex multi-step analysis may need more.
# Configurable via ir.config_parameter: foggy_mcp.llm_max_tool_rounds
DEFAULT_MAX_TOOL_ROUNDS = 20

# ── System prompt template ──────────────────────────────────────

SYSTEM_PROMPT_TEMPLATE = """你是 Foggy AI，一个嵌入 Odoo ERP 的业务数据分析助手。
你通过工具调用帮助用户分析业务数据。

当前时间：{current_time}

## ⚠️ 核心规则（必须严格遵守）
1. **禁止编造数据**：你只能基于工具调用的真实返回结果回答问题。绝对不能凭猜测或假设编造数据、字段名、统计结果。
2. **不确定时先查询**：如果你不确定某个模型有哪些字段，必须先调用 `dataset__describe_model_internal` 或 `dataset__get_metadata` 获取字段信息，再构造查询。
3. **如实报告**：如果查询返回空结果或出错，如实告知用户，不要编造替代数据。
4. **使用中文回复**：除非用户使用其他语言，否则使用中文回复。

## 可用数据模型
{model_descriptions}

## 查询流程
1. 如果不确定模型有哪些字段 → 先调用 `dataset__describe_model_internal` 获取字段定义
2. 根据字段定义构造查询 → 调用 `dataset__query_model` 执行查询
3. 基于真实查询结果 → 整理回复

## 查询参数格式（dataset__query_model）
```json
{{
  "model": "模型名称",
  "payload": {{
    "columns": ["field1", "dimension$caption", "sum(measure) as total"],
    "slice": [{{"field": "fieldName", "op": "=", "value": "..."}}],
    "orderBy": [{{"field": "total", "dir": "desc"}}],
    "limit": 20
  }}
}}
```

## 字段使用规则
- 维度关联字段：`dimension$id`（查询/过滤用）、`dimension$caption`（展示用）
- 内联聚合：`sum(amount) as total`、`count(id) as cnt` — 系统自动生成 groupBy
- orderBy 可以引用 columns 中定义的别名（如 `total`）或原始字段名
- 日期格式：ISO "2025-01-01"

## 回复格式
- 用清晰的 markdown 表格展示数据
- 提供简要的分析和洞察
- 明确说明数据来源（哪个模型、什么条件）
"""


def _get_llm_config(env):
    """Read LLM configuration from Odoo settings."""
    get = env['ir.config_parameter'].sudo().get_param
    provider = get('foggy_mcp.llm_provider', 'openai')
    api_key = get('foggy_mcp.llm_api_key', '')
    model = get('foggy_mcp.llm_model', 'gpt-4o-mini')
    base_url = get('foggy_mcp.llm_base_url', '')
    temperature = float(get('foggy_mcp.llm_temperature', '0.3'))

    return {
        'provider': provider,
        'api_key': api_key,
        'model': model,
        'base_url': base_url or None,
        'temperature': temperature,
    }


    # Human-readable model descriptions for the system prompt (Chinese)
_MODEL_DESCRIPTIONS = {
    'OdooSaleOrderQueryModel': '销售订单 — 订单号、客户、金额、状态、销售员、团队',
    'OdooSaleOrderLineQueryModel': '销售订单行 — 产品明细、数量、单价、行小计',
    'OdooPurchaseOrderQueryModel': '采购订单 — 供应商、采购金额、状态',
    'OdooAccountMoveQueryModel': '发票与账单 — 会计分录、付款状态、金额',
    'OdooStockPickingQueryModel': '库存调拨 — 仓库移动、拣货状态',
    'OdooHrEmployeeQueryModel': '员工 — 姓名、部门、职位、工作地点、联系方式',
    'OdooResPartnerQueryModel': '联系人/合作伙伴 — 客户、供应商、地址',
    'OdooCrmLeadQueryModel': 'CRM 线索/商机 — 管道阶段、预期收入、概率、销售员',
    'OdooAccountPaymentQueryModel': '付款记录 — 收付款金额、类型、合作伙伴、币种、对账状态',
    'OdooAccountMoveLineQueryModel': '会计分录行 — 借贷方金额、科目、日记账、产品明细',
    'OdooProductTemplateQueryModel': '产品目录 — 产品名称、类型、分类、价格、计量单位',
}


def _build_system_prompt(env, uid):
    """Build system prompt with available model information."""
    from .tool_registry import MODEL_MAPPING

    # Get model descriptions from accessible models
    model_descriptions = []
    try:
        user_env = env(user=uid)
        for odoo_model, qm_name in MODEL_MAPPING.items():
            try:
                if odoo_model in user_env and user_env['ir.model.access'].check(
                    odoo_model, 'read', raise_exception=False
                ):
                    desc = _MODEL_DESCRIPTIONS.get(qm_name, '')
                    model_descriptions.append(f"- **{qm_name}**: {desc}" if desc else f"- **{qm_name}**")
            except Exception:
                pass

        if not model_descriptions:
            model_descriptions.append("(No accessible models — check user permissions)")
    except Exception as e:
        _logger.warning("Failed to build model descriptions: %s", e)
        model_descriptions.append("(Model list unavailable — check Foggy MCP Server connection)")

    from datetime import datetime
    prompt = SYSTEM_PROMPT_TEMPLATE.format(
        current_time=datetime.now().strftime('%Y-%m-%d %H:%M (%A)'),
        model_descriptions='\n'.join(model_descriptions),
    )

    # Inject admin-defined business context & custom rules
    custom_prompt = env['ir.config_parameter'].sudo().get_param(
        'foggy_mcp.llm_custom_prompt', '')
    if custom_prompt and custom_prompt.strip():
        prompt += f"\n\n## 管理员自定义规则\n{custom_prompt.strip()}\n"

    return prompt


def _build_openai_tools(env, uid):
    """Convert Foggy MCP tools to OpenAI function calling format.

    Uses the unified engine backend (embedded or gateway) via mcp_controller,
    instead of creating a separate FoggyClient.

    Returns:
        tuple: (tools_list, name_map_dict)
    """
    from ..controllers.mcp_controller import _get_engine_backend, _get_tool_registry

    try:
        registry = _get_tool_registry(env)
        foggy_tools = registry.get_tools_for_user(env, uid)
    except Exception as e:
        _logger.error("Failed to load tools for LLM: %s", e)
        return [], {}

    # Explicit name mapping: OpenAI function names can't have dots
    _TOOL_NAME_MAP = {
        'dataset.query_model': 'dataset__query_model',
        'dataset.get_metadata': 'dataset__get_metadata',
        'dataset.describe_model_internal': 'dataset__describe_model_internal',
    }

    tools = []
    for tool in foggy_tools:
        name = tool.get('name', '')
        if name not in _TOOL_NAME_MAP:
            continue

        fn_def = {
            'type': 'function',
            'function': {
                'name': _TOOL_NAME_MAP[name],
                'description': tool.get('description', ''),
                'parameters': tool.get('inputSchema', {'type': 'object', 'properties': {}}),
            }
        }
        tools.append(fn_def)

    return tools, _TOOL_NAME_MAP


def _execute_tool_call(env, uid, tool_name, arguments, reverse_name_map=None):
    """Execute a tool call through Foggy MCP Server with permission injection."""
    from .foggy_client import FoggyClient
    from .permission_bridge import compute_permission_slices
    from .tool_registry import QM_TO_ODOO_MODEL

    # Restore original tool name using reverse map
    if reverse_name_map and tool_name in reverse_name_map:
        original_name = reverse_name_map[tool_name]
    else:
        original_name = tool_name  # fallback

    # For query_model, inject permission slices
    if original_name == 'dataset.query_model' and 'payload' in arguments:
        model_name = arguments.get('model', '')
        odoo_model = QM_TO_ODOO_MODEL.get(model_name, '')
        if odoo_model:
            try:
                slices = compute_permission_slices(env, uid, model_name)
                if slices:
                    payload = arguments.get('payload', {})
                    if isinstance(payload, str):
                        payload = json.loads(payload)
                    existing = payload.get('slice', [])
                    payload['slice'] = existing + slices
                    arguments['payload'] = payload
            except Exception as e:
                _logger.error("Permission injection failed for chat: %s", e)
                return {'error': f'Permission check failed: {e}'}

    # Call through unified engine backend (embedded or gateway)
    try:
        from ..controllers.mcp_controller import _get_engine_backend
        backend = _get_engine_backend(env)
        result = backend.call_tools_call(original_name, arguments)
        return result
    except Exception as e:
        _logger.error("Tool call failed: %s — %s", original_name, e)
        return {'error': str(e)}


# ── Error message helpers ──────────────────────────────────────

_INSTALL_GUIDE_URL = (
    'https://github.com/foggy-projects/foggy-odoo-bridge/blob/main/'
    'INSTALL_GUIDE.md#optional-enable-built-in-ai-chat'
)


def _missing_package_error(package):
    """Build a user-friendly error message for missing LLM SDK package."""
    return (
        f'**AI Chat 需要安装 `{package}` 包**\n\n'
        f'**方法一**：使用项目自带的 Dockerfile 构建（推荐）：\n'
        f'```\n'
        f'docker compose build odoo\n'
        f'docker compose up -d\n'
        f'```\n'
        f'**方法二**：修改 `docker-compose.yml`，在 command 中加入 pip install'
        f'（每次启动自动安装，无需构建镜像）：\n'
        f'```\n'
        f'command: >\n'
        f'  bash -c "pip install openai anthropic &&\n'
        f'  exec odoo --database=odoo_demo ..."\n'
        f'```\n'
        f'**方法三**：快速临时安装（重启后失效）：\n'
        f'```\n'
        f'docker exec foggy-odoo pip install openai anthropic\n'
        f'docker restart foggy-odoo\n'
        f'```\n'
        f'详细说明：[安装指南 → 启用 AI Chat]({_INSTALL_GUIDE_URL})'
    )


# ── Provider-specific LLM call implementations ────────────────


def _call_openai(config, messages, tools):
    """Call OpenAI or OpenAI-compatible API (DeepSeek, Ollama, vLLM, etc.).

    Returns:
        dict: {'message': assistant_message_dict, 'error': str or None}
    """
    try:
        from openai import OpenAI
    except ImportError:
        return {'message': None, 'error': _missing_package_error('openai')}

    client_kwargs = {'api_key': config['api_key']}
    if config['base_url']:
        client_kwargs['base_url'] = config['base_url']

    client = OpenAI(**client_kwargs)

    call_kwargs = {
        'model': config['model'],
        'messages': messages,
        'temperature': config['temperature'],
    }
    if tools:
        call_kwargs['tools'] = tools
        call_kwargs['tool_choice'] = 'auto'

    response = client.chat.completions.create(**call_kwargs)

    # Guard against non-standard API responses (some OpenAI-compatible
    # endpoints return a raw string or dict instead of a ChatCompletion object)
    if isinstance(response, str):
        _logger.warning("LLM endpoint returned a raw string instead of ChatCompletion object. "
                        "base_url=%s, model=%s, response_preview=%s",
                        config.get('base_url'), config['model'], response[:200])
        return {'message': {'role': 'assistant', 'content': response}, 'error': None}

    if not hasattr(response, 'choices') or not response.choices:
        _logger.error("LLM endpoint returned unexpected response type: %s", type(response))
        return {'message': None,
                'error': f'LLM 端点返回了非标准响应格式（{type(response).__name__}）。'
                         f'请检查 API Base URL 和模型名称是否正确。'}

    choice = response.choices[0]
    msg = choice.message

    # Normalize to dict for unified processing
    result = {
        'role': 'assistant',
        'content': msg.content or '',
    }
    if msg.tool_calls:
        result['tool_calls'] = [
            {
                'id': tc.id,
                'type': 'function',
                'function': {
                    'name': tc.function.name,
                    'arguments': tc.function.arguments,
                }
            }
            for tc in msg.tool_calls
        ]
    return {'message': result, 'error': None}


def _call_anthropic(config, messages, tools):
    """Call Anthropic Claude API.

    Anthropic has a different API format:
    - system prompt is a top-level parameter, not in messages
    - tool_use / tool_result are content block types
    - tool results go in 'user' role messages with tool_result blocks

    Returns:
        dict: {'message': assistant_message_dict (OpenAI-normalized), 'error': str or None}
    """
    try:
        from anthropic import Anthropic
    except ImportError:
        return {'message': None, 'error': _missing_package_error('anthropic')}

    client_kwargs = {'api_key': config['api_key']}
    if config['base_url']:
        client_kwargs['base_url'] = config['base_url']

    client = Anthropic(**client_kwargs)

    # Extract system prompt from messages (Anthropic uses top-level system param)
    system_prompt = ''
    anthropic_messages = []
    for msg in messages:
        if msg.get('role') == 'system':
            system_prompt = msg.get('content', '')
        else:
            anthropic_messages.append(_to_anthropic_message(msg))

    # Build Anthropic tools format
    anthropic_tools = []
    if tools:
        for tool in tools:
            fn = tool.get('function', {})
            anthropic_tools.append({
                'name': fn.get('name', ''),
                'description': fn.get('description', ''),
                'input_schema': fn.get('parameters', {'type': 'object', 'properties': {}}),
            })

    call_kwargs = {
        'model': config['model'],
        'messages': anthropic_messages,
        'max_tokens': 4096,
        'temperature': config['temperature'],
    }
    if system_prompt:
        call_kwargs['system'] = system_prompt
    if anthropic_tools:
        call_kwargs['tools'] = anthropic_tools

    response = client.messages.create(**call_kwargs)

    # Normalize Anthropic response to OpenAI format
    return {'message': _from_anthropic_response(response), 'error': None}


def _to_anthropic_message(msg):
    """Convert an OpenAI-format message to Anthropic format.

    Handles:
    - Regular text messages (user/assistant)
    - Assistant messages with tool_calls → content blocks with tool_use
    - Tool result messages → user messages with tool_result content blocks
    """
    role = msg.get('role', 'user')

    # Tool result message → Anthropic tool_result content block
    if role == 'tool':
        return {
            'role': 'user',
            'content': [{
                'type': 'tool_result',
                'tool_use_id': msg.get('tool_call_id', ''),
                'content': msg.get('content', ''),
            }],
        }

    # Assistant message with tool_calls → Anthropic content blocks
    if role == 'assistant' and msg.get('tool_calls'):
        content_blocks = []
        text = msg.get('content', '')
        if text:
            content_blocks.append({'type': 'text', 'text': text})
        for tc in msg['tool_calls']:
            fn = tc.get('function', {})
            args_str = fn.get('arguments', '{}')
            try:
                args = json.loads(args_str) if isinstance(args_str, str) else args_str
            except json.JSONDecodeError:
                args = {}
            content_blocks.append({
                'type': 'tool_use',
                'id': tc.get('id', ''),
                'name': fn.get('name', ''),
                'input': args,
            })
        return {'role': 'assistant', 'content': content_blocks}

    # Regular text message
    return {'role': role, 'content': msg.get('content', '')}


def _from_anthropic_response(response):
    """Convert Anthropic response to OpenAI-normalized dict.

    Returns a dict with keys: role, content, tool_calls (optional)
    """
    result = {'role': 'assistant', 'content': ''}
    tool_calls = []

    for block in response.content:
        if block.type == 'text':
            result['content'] += block.text
        elif block.type == 'tool_use':
            tool_calls.append({
                'id': block.id,
                'type': 'function',
                'function': {
                    'name': block.name,
                    'arguments': json.dumps(block.input, ensure_ascii=False),
                },
            })

    if tool_calls:
        result['tool_calls'] = tool_calls

    return result


def _call_llm(config, messages, tools):
    """Dispatch to the correct provider.

    Returns:
        dict: {'message': assistant_message_dict, 'error': str or None}
    """
    provider = config['provider']
    if provider == 'anthropic':
        return _call_anthropic(config, messages, tools)
    else:
        # openai, deepseek, ollama, custom — all OpenAI-compatible
        return _call_openai(config, messages, tools)


# ── Main chat function ─────────────────────────────────────────


def chat(env, uid, session_id, user_message):
    """
    Main chat function — send user message, get AI response.

    Args:
        env: Odoo environment
        uid: User ID
        session_id: foggy.chat.session ID
        user_message: User's text message

    Returns:
        dict with 'content' (assistant reply) and 'error' (if any)
    """
    config = _get_llm_config(env)
    if not config['api_key']:
        return {
            'content': '',
            'error': (
                '**AI Chat 尚未配置 LLM**\n\n'
                '请前往 **Settings → Foggy MCP → AI Chat Configuration** 填写：\n'
                '- **Provider**：OpenAI-compatible 或 Anthropic\n'
                '- **API Key**：你的 API Key\n'
                '- **Model**：如 `gpt-4o-mini` 或 `claude-3-5-haiku-20241022`\n\n'
                '详细步骤：[安装说明 → 启用 AI Chat]'
                '(https://github.com/foggy-projects/foggy-odoo-bridge/blob/main/INSTALL_GUIDE.md'
                '#optional-enable-built-in-ai-chat)'
            ),
        }

    Session = env['foggy.chat.session'].sudo()
    Message = env['foggy.chat.message'].sudo()

    # Get or create session
    session = Session.browse(session_id) if session_id else None
    if not session or not session.exists():
        session = Session.create({
            'user_id': uid,
            'name': user_message[:50] + ('...' if len(user_message) > 50 else ''),
        })

    # Save user message
    Message.create({
        'session_id': session.id,
        'role': 'user',
        'content': user_message,
    })

    # Build conversation history
    system_prompt = _build_system_prompt(env, uid)
    messages = [{'role': 'system', 'content': system_prompt}]

    # Load last N messages from session (keep context manageable)
    history = Message.search([
        ('session_id', '=', session.id),
        ('role', 'in', ['user', 'assistant']),
    ], order='create_date asc, id asc', limit=20)

    for msg in history:
        messages.append({
            'role': msg.role,
            'content': msg.content or '',
        })

    # Build tools and name mapping
    tools, tool_name_map = _build_openai_tools(env, uid)
    reverse_name_map = {v: k for k, v in tool_name_map.items()}

    # LLM call with tool calling loop
    max_rounds = int(env['ir.config_parameter'].sudo().get_param(
        'foggy_mcp.llm_max_tool_rounds', str(DEFAULT_MAX_TOOL_ROUNDS)))

    try:
        for round_idx in range(max_rounds):
            llm_result = _call_llm(config, messages, tools)

            if llm_result['error']:
                return {
                    'session_id': session.id,
                    'content': '',
                    'error': llm_result['error'],
                }

            assistant_msg = llm_result['message']

            # Check for tool calls
            if assistant_msg.get('tool_calls'):
                # Add assistant message with tool calls to history
                messages.append(assistant_msg)

                for tool_call in assistant_msg['tool_calls']:
                    fn_name = tool_call['function']['name']
                    try:
                        fn_args = json.loads(tool_call['function']['arguments'])
                    except json.JSONDecodeError:
                        fn_args = {}

                    _logger.info("Chat round %d tool call: %s(%s)", round_idx, fn_name, json.dumps(fn_args, ensure_ascii=False)[:200])

                    result = _execute_tool_call(env, uid, fn_name, fn_args, reverse_name_map)
                    result_str = json.dumps(result, ensure_ascii=False, default=str)
                    _logger.info("Chat round %d tool result length: %d, preview: %s", round_idx, len(result_str), result_str[:300])

                    # Add tool result to conversation
                    messages.append({
                        'role': 'tool',
                        'tool_call_id': tool_call['id'],
                        'content': result_str[:8000],  # Truncate if too long
                    })

                # Continue loop for LLM to process tool results
                continue

            # No tool calls — we have the final response
            final_content = assistant_msg.get('content', '')

            # Save assistant response
            Message.create({
                'session_id': session.id,
                'role': 'assistant',
                'content': final_content,
            })

            return {
                'session_id': session.id,
                'content': final_content,
                'error': None,
            }

        # Max rounds exceeded
        return {
            'session_id': session.id,
            'content': 'I reached the maximum number of tool calls. Please try a simpler question.',
            'error': 'max_tool_rounds_exceeded',
        }

    except Exception as e:
        _logger.exception("LLM chat error")
        error_msg = str(e)
        # Common error messages for better UX
        if 'api_key' in error_msg.lower() or 'authentication' in error_msg.lower():
            error_msg = 'Invalid LLM API key. Please check Settings → Foggy MCP → AI Chat.'
        elif 'rate_limit' in error_msg.lower():
            error_msg = 'LLM rate limit reached. Please wait a moment and try again.'

        return {
            'session_id': session.id if session else None,
            'content': '',
            'error': error_msg,
        }
