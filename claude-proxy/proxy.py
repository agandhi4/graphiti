"""
Claude Code → Anthropic API proxy.

Accepts Anthropic Messages API requests on localhost:11437,
routes them through a persistent Claude Code session (warm subprocess),
and returns responses in Anthropic Messages API format.

Requests are queued FIFO and processed one at a time through the single
Claude session. The claude subprocess has zero file system permissions
and runs in an empty sandbox directory to prevent any file scanning.

Self-healing: if a request hangs beyond REQUEST_TIMEOUT_SECONDS, the
Claude subprocess is force-killed, the session is recreated, and the
request is retried once. This prevents a single hung request from
blocking the entire queue.

Dependencies: claude-agent-sdk (lazy-imported to allow FastAPI startup
even if the SDK has issues — the first request will surface the error).
"""

import asyncio
import json
import logging
import os
import signal
import time
import uuid

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('claude-proxy')

# -- Configuration --

CLAUDE_PATH = os.environ.get('CLAUDE_PATH', '/Users/aakash/.local/bin/claude')
SANDBOX_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'sandbox')
DEFAULT_MODEL = 'haiku'
SESSION_RESET_INTERVAL = 25   # recreate Claude session every N requests to prevent context pollution
REQUEST_TIMEOUT_SECONDS = 60  # kill and retry if a single LLM call takes longer than this
CLOSE_TIMEOUT_SECONDS = 5     # give __aexit__ this long before force-killing

# Rough chars-to-tokens ratio. Claude tokenizer averages ~4 chars per token for English.
CHARS_PER_TOKEN = 4

# API pricing per 1M tokens (for cost estimation — we're not actually paying these).
# These are "what it WOULD cost" comparisons to inform whether switching off the
# free proxy is worthwhile. Updated March 2026.
PRICING = {
    'claude_haiku_4_5':  {'input': 1.00,  'output': 5.00,  'label': 'Claude Haiku 4.5'},
    'gemini_2_5_flash':  {'input': 0.15,  'output': 0.60,  'label': 'Gemini 2.5 Flash'},
    'gemini_flash_lite': {'input': 0.075, 'output': 0.30,  'label': 'Gemini 2.0 Flash-Lite'},
    'deepseek_v3':       {'input': 0.28,  'output': 0.42,  'label': 'DeepSeek V3.2'},
    'groq_llama_8b':     {'input': 0.06,  'output': 0.06,  'label': 'Groq Llama 3.1 8B'},
    'mistral_nemo':      {'input': 0.02,  'output': 0.04,  'label': 'Mistral Nemo'},
}

# Persisted usage stats file — survives proxy restarts
USAGE_STATS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'usage_stats.json')

app = FastAPI()

# -- State --

_client = None
_client_pid: int | None = None  # PID of the Claude subprocess, tracked explicitly
_request_count = 0
_total_processed = 0
_total_errors = 0
_total_input_chars = 0   # cumulative input characters across all requests
_total_output_chars = 0  # cumulative output characters across all requests
_current_request_start: float | None = None  # timestamp when current request began processing
_startup_time: float = 0
_last_error: str | None = None
_last_error_time: float | None = None
_queue: asyncio.Queue | None = None
_worker_task: asyncio.Task | None = None
_request_counter = 0  # monotonic counter for log correlation


def _next_request_id() -> str:
    global _request_counter
    _request_counter += 1
    return f'req-{_request_counter}'


def _load_usage_stats():
    """Load cumulative usage stats from disk."""
    global _total_processed, _total_errors, _total_input_chars, _total_output_chars
    try:
        with open(USAGE_STATS_FILE, 'r') as f:
            stats = json.load(f)
        _total_processed = stats.get('total_processed', 0)
        _total_errors = stats.get('total_errors', 0)
        _total_input_chars = stats.get('total_input_chars', 0)
        _total_output_chars = stats.get('total_output_chars', 0)
        logger.info(f'Loaded usage stats: {_total_processed} processed, '
                    f'{_total_input_chars} input chars, {_total_output_chars} output chars')
    except (FileNotFoundError, json.JSONDecodeError):
        logger.info('No existing usage stats — starting fresh')


def _save_usage_stats():
    """Persist cumulative usage stats to disk."""
    stats = {
        'total_processed': _total_processed,
        'total_errors': _total_errors,
        'total_input_chars': _total_input_chars,
        'total_output_chars': _total_output_chars,
        'last_saved': time.strftime('%Y-%m-%d %H:%M:%S'),
    }
    try:
        with open(USAGE_STATS_FILE, 'w') as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        logger.warning(f'Failed to save usage stats: {e}')


# ============================================================
# Session management
# ============================================================

async def _create_session():
    """Create a fresh Claude subprocess and return the SDK client.

    Tracks the subprocess PID explicitly so we can force-kill it on timeout
    without probing SDK internals.
    """
    global _client, _client_pid, _request_count

    # Lazy import: allows FastAPI to start even if SDK has issues.
    # The first request will surface any import error.
    from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient

    options = ClaudeAgentOptions(
        model=DEFAULT_MODEL,
        allowed_tools=[],
        disallowed_tools=['Bash', 'Read', 'Write', 'Edit', 'Glob', 'Grep', 'WebSearch', 'WebFetch', 'Agent'],
        max_turns=1,
        cli_path=CLAUDE_PATH,
        cwd=SANDBOX_DIR,
    )
    client = ClaudeSDKClient(options=options)
    await client.__aenter__()

    # Find the subprocess PID — check common SDK internal attributes
    pid = None
    for attr_path in ['_process.pid', 'process.pid', '_transport._process.pid']:
        obj = client
        try:
            for part in attr_path.split('.'):
                obj = getattr(obj, part)
            if isinstance(obj, int) and obj > 0:
                pid = obj
                break
        except AttributeError:
            continue

    _client = client
    _client_pid = pid
    _request_count = 0

    pid_info = f', PID {pid}' if pid else ', PID unknown'
    logger.info(f'Session created (model={DEFAULT_MODEL}{pid_info})')


def _force_kill_subprocess():
    """Force-kill the tracked Claude subprocess. Idempotent."""
    global _client_pid
    if _client_pid is None:
        return

    pid = _client_pid
    _client_pid = None

    logger.warning(f'Force-killing Claude subprocess (PID {pid})')
    # Try process group first (kills child processes too), fall back to direct kill
    for kill_fn in [
        lambda: os.killpg(os.getpgid(pid), signal.SIGKILL),
        lambda: os.kill(pid, signal.SIGKILL),
    ]:
        try:
            kill_fn()
            return
        except (ProcessLookupError, PermissionError):
            continue
    logger.warning(f'Could not kill PID {pid} — process may have already exited')


async def _close_session():
    """Gracefully close the Claude session. Force-kills if __aexit__ hangs."""
    global _client
    if _client is None:
        return

    client = _client
    _client = None

    try:
        await asyncio.wait_for(client.__aexit__(None, None, None), timeout=CLOSE_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        logger.warning(f'Session __aexit__ hung for {CLOSE_TIMEOUT_SECONDS}s — force-killing')
        _force_kill_subprocess()
    except Exception as e:
        logger.warning(f'Error during session close: {e}')


async def _ensure_session():
    """Ensure a healthy session exists. Creates one on first call, resets after N requests."""
    if _client is not None and _request_count < SESSION_RESET_INTERVAL:
        return

    if _client is not None:
        logger.info(f'Recycling session after {_request_count} requests')
        await _close_session()

    await _create_session()


async def _reset_session():
    """Force-reset after an error or timeout. Kills subprocess first, then closes cleanly."""
    global _last_error, _last_error_time
    logger.warning('Force-resetting session')
    _force_kill_subprocess()
    await _close_session()


# ============================================================
# Request processing
# ============================================================

async def _process_request(prompt: str, req_id: str) -> str:
    """Send a prompt through the Claude session. Returns the response text."""
    global _request_count, _total_processed, _total_errors, _total_input_chars, _total_output_chars, _last_error, _last_error_time

    # Lazy import (see module docstring)
    from claude_agent_sdk import ResultMessage

    try:
        await _ensure_session()
    except Exception as e:
        _total_errors += 1
        _last_error = f'Session init failed: {e}'
        _last_error_time = time.time()
        logger.error(f'[{req_id}] Session init failed: {e}')
        await _reset_session()
        raise

    try:
        result_text = ''

        async def _query():
            nonlocal result_text
            await _client.query(prompt)
            async for msg in _client.receive_response():
                if isinstance(msg, ResultMessage):
                    result_text = msg.result or ''

        await asyncio.wait_for(_query(), timeout=REQUEST_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        _total_errors += 1
        _last_error = f'Timeout after {REQUEST_TIMEOUT_SECONDS}s (prompt_len={len(prompt)})'
        _last_error_time = time.time()
        logger.error(f'[{req_id}] Query timed out after {REQUEST_TIMEOUT_SECONDS}s '
                     f'(prompt_len={len(prompt)}) — killing subprocess')
        await _reset_session()
        raise
    except Exception as e:
        _total_errors += 1
        _last_error = str(e)
        _last_error_time = time.time()
        logger.error(f'[{req_id}] Query failed: {e} — resetting session')
        await _reset_session()
        raise

    if not result_text:
        _total_errors += 1
        _last_error = 'Empty response'
        _last_error_time = time.time()
        logger.warning(f'[{req_id}] Claude returned empty response')
        raise ValueError('Claude returned an empty response')

    # Log short responses in full — they may indicate the LLM is returning
    # a canned response instead of processing the tool-forced prompt
    if len(result_text) < 100:
        logger.warning(f'[{req_id}] Short response ({len(result_text)} chars): {result_text!r}')

    _request_count += 1
    _total_processed += 1
    _total_input_chars += len(prompt)
    _total_output_chars += len(result_text)

    # Persist stats every 10 requests to avoid excessive disk writes
    if _total_processed % 10 == 0:
        _save_usage_stats()

    return result_text


async def _queue_worker():
    """Process queued requests one at a time. Retries once on failure."""
    global _current_request_start
    logger.info('Queue worker started')

    while True:
        prompt, future, req_id = await _queue.get()
        t0 = time.time()
        _current_request_start = t0
        last_error = None
        prompt_preview = prompt[:80].replace('\n', ' ')

        for attempt in range(2):
            label = f'attempt {attempt + 1}/2'
            logger.info(f'[{req_id}] Processing ({label}, prompt_len={len(prompt)}, '
                        f'queued={_queue.qsize()}, preview="{prompt_preview}...")')
            try:
                result = await _process_request(prompt, req_id)
                if not future.cancelled():
                    future.set_result(result)
                retry_note = ' (retry succeeded)' if attempt > 0 else ''
                logger.info(f'[{req_id}] Completed{retry_note} ({time.time() - t0:.1f}s, '
                            f'{len(result)} chars, total={_total_processed}, queued={_queue.qsize()})')
                last_error = None
                break
            except asyncio.TimeoutError:
                action = 'retrying with fresh session' if attempt == 0 else 'giving up'
                logger.warning(f'[{req_id}] Timeout on {label} — {action}')
                last_error = asyncio.TimeoutError(f'Timed out after {REQUEST_TIMEOUT_SECONDS}s')
            except Exception as e:
                action = 'retrying with fresh session' if attempt == 0 else 'giving up'
                logger.warning(f'[{req_id}] Error on {label}: {e} — {action}')
                last_error = e

        if last_error is not None:
            if not future.cancelled():
                future.set_exception(last_error)
            logger.error(f'[{req_id}] Failed after 2 attempts ({time.time() - t0:.1f}s): {last_error}')

        _current_request_start = None
        _queue.task_done()


# ============================================================
# Anthropic API format conversion
# ============================================================

def _messages_to_prompt(body: dict) -> str:
    """Convert an Anthropic Messages API request body into a single prompt string."""
    parts = []

    system = body.get('system', '')
    if isinstance(system, list):
        system = '\n'.join(block.get('text', '') for block in system if block.get('type') == 'text')
    if system:
        parts.append(f'SYSTEM INSTRUCTIONS:\n{system}')

    for msg in body.get('messages', []):
        role = msg['role'].upper()
        content = msg['content']
        if isinstance(content, list):
            text_parts = []
            for block in content:
                if isinstance(block, dict) and block.get('type') == 'text':
                    text_parts.append(block['text'])
                elif isinstance(block, dict) and block.get('type') == 'tool_result':
                    text_parts.append(f'[Tool result for {block.get("tool_use_id", "?")}]: {block.get("content", "")}')
            content = '\n'.join(text_parts)
        parts.append(f'{role}: {content}')

    tools = body.get('tools', [])
    tool_choice = body.get('tool_choice', {})
    if tools:
        tool_schemas = [
            {'name': t.get('name'), 'description': t.get('description', ''), 'input_schema': t.get('input_schema', {})}
            for t in tools
        ]
        parts.append(f'\n\nYou have access to the following tools:\n{json.dumps(tool_schemas, indent=2)}')

        if tool_choice and tool_choice.get('type') == 'tool':
            forced_tool = tool_choice['name']
            parts.append(
                f'\nYou MUST call the tool "{forced_tool}". '
                f'Respond with ONLY a JSON object matching the tool\'s input_schema. '
                f'No explanation, no markdown, no extra text — just the raw JSON object.'
            )
        else:
            parts.append(
                '\nIf you use a tool, respond with ONLY a JSON object matching '
                'the tool\'s input_schema. No explanation, no markdown, no extra text.'
            )

    return '\n\n'.join(parts)


def _extract_json(text: str | None) -> dict | None:
    """Try to extract a JSON object from Claude's response text."""
    if not text or not text.strip():
        return None
    text = text.strip()

    # Strip markdown code fences
    if text.startswith('```'):
        lines = [l for l in text.split('\n') if not l.strip().startswith('```')]
        text = '\n'.join(lines).strip()

    # Try direct parse first
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Try extracting the outermost { ... }
    start = text.find('{')
    end = text.rfind('}')
    if start >= 0 and end > start:
        try:
            return json.loads(text[start:end + 1])
        except json.JSONDecodeError:
            pass

    return None


def _build_response(body: dict, result_text: str) -> dict:
    """Build an Anthropic Messages API response from Claude's output."""
    tools = body.get('tools', [])
    tool_choice = body.get('tool_choice', {})
    content = []

    if tools and tool_choice and tool_choice.get('type') == 'tool':
        forced_tool = tool_choice['name']
        parsed = _extract_json(result_text)
        if parsed is not None:
            content.append({
                'type': 'tool_use',
                'id': f'toolu_{uuid.uuid4().hex[:20]}',
                'name': forced_tool,
                'input': parsed,
            })
        else:
            logger.warning(f'Failed to extract JSON for tool {forced_tool}: {result_text[:200]}')
            content.append({'type': 'text', 'text': result_text})
    else:
        content.append({'type': 'text', 'text': result_text})

    return {
        'id': f'msg_{uuid.uuid4().hex[:20]}',
        'type': 'message',
        'role': 'assistant',
        'content': content,
        'model': body.get('model', DEFAULT_MODEL),
        'stop_reason': 'tool_use' if any(c['type'] == 'tool_use' for c in content) else 'end_turn',
        'usage': {'input_tokens': 0, 'output_tokens': 0},
    }


# ============================================================
# HTTP endpoints
# ============================================================

@app.on_event('startup')
async def startup():
    global _queue, _worker_task, _startup_time
    _load_usage_stats()
    _queue = asyncio.Queue()
    _worker_task = asyncio.create_task(_queue_worker())
    _startup_time = time.time()


@app.on_event('shutdown')
async def shutdown():
    _save_usage_stats()
    logger.info('Proxy shutting down — usage stats saved')


@app.post('/v1/messages')
async def proxy_messages(request: Request):
    body = await request.json()
    prompt = _messages_to_prompt(body)
    req_id = _next_request_id()

    logger.info(f'[{req_id}] Queued (prompt_len={len(prompt)}, queue_depth={_queue.qsize()})')

    future = asyncio.get_event_loop().create_future()
    await _queue.put((prompt, future, req_id))

    try:
        result_text = await asyncio.wait_for(future, timeout=REQUEST_TIMEOUT_SECONDS * 2 + 30)
    except asyncio.TimeoutError:
        timeout = REQUEST_TIMEOUT_SECONDS * 2 + 30
        logger.error(f'[{req_id}] HTTP timeout after {timeout}s (including retry)')
        return JSONResponse(
            status_code=504,
            content={'error': {'type': 'timeout', 'message': f'Request timed out after {timeout}s'}},
        )
    except Exception as e:
        logger.error(f'[{req_id}] HTTP error: {e}')
        return JSONResponse(
            status_code=500,
            content={'error': {'type': 'proxy_error', 'message': str(e)}},
        )

    return JSONResponse(content=_build_response(body, result_text))


def _estimate_costs() -> dict:
    """Estimate what the proxy's usage would cost on paid APIs."""
    input_tokens = _total_input_chars / CHARS_PER_TOKEN
    output_tokens = _total_output_chars / CHARS_PER_TOKEN

    costs = {}
    for key, pricing in PRICING.items():
        input_cost = (input_tokens / 1_000_000) * pricing['input']
        output_cost = (output_tokens / 1_000_000) * pricing['output']
        costs[key] = {
            'label': pricing['label'],
            'estimated_cost_usd': round(input_cost + output_cost, 4),
            'input_cost_usd': round(input_cost, 4),
            'output_cost_usd': round(output_cost, 4),
        }
    return costs


@app.get('/health')
async def health():
    now = time.time()
    current_duration = round(now - _current_request_start, 1) if _current_request_start else None
    input_tokens_est = int(_total_input_chars / CHARS_PER_TOKEN)
    output_tokens_est = int(_total_output_chars / CHARS_PER_TOKEN)

    return {
        'status': 'healthy',
        'service': 'claude-proxy',
        'uptime_seconds': round(now - _startup_time) if _startup_time else 0,
        'queue_depth': _queue.qsize() if _queue else 0,
        'current_request_seconds': current_duration,
        'total_processed': _total_processed,
        'total_errors': _total_errors,
        'total_input_chars': _total_input_chars,
        'total_output_chars': _total_output_chars,
        'estimated_tokens': {
            'input': input_tokens_est,
            'output': output_tokens_est,
        },
        'estimated_costs_if_paid': _estimate_costs(),
        'session_requests': _request_count,
        'session_reset_at': SESSION_RESET_INTERVAL,
        'session_pid': _client_pid,
        'last_error': _last_error,
        'last_error_age_seconds': round(now - _last_error_time) if _last_error_time else None,
    }


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='127.0.0.1', port=11437, log_level='info')
