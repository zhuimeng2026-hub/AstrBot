# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AstrBot is a multi-platform LLM chatbot framework (Python 3.12+, Vue.js dashboard). It connects IM platforms (QQ, Telegram, Discord, WeChat Work, Feishu, DingTalk, Slack, etc.) to LLM providers (OpenAI, Anthropic, Gemini, etc.) with a plugin system ("Stars").

## Build & Development Commands

### Python backend
```bash
uv sync                          # Install dependencies (~6-7 min, set timeout 10+ min)
uv run main.py                   # Start AstrBot (WebUI on http://localhost:6185)
uv run ruff check .              # Lint
uv run ruff format .             # Format
uv run ruff format --check .     # Check formatting
```

### Dashboard (Vue.js/Vite)
```bash
cd dashboard
pnpm install                     # Install deps (~2-3 min)
pnpm dev                         # Dev server on http://localhost:3000
pnpm build                       # Production build to dashboard/dist/
```

### Testing
```bash
uv run pytest tests/             # Run all tests
uv run pytest tests/test_smoke.py  # Run single test file
uv run pytest -k "test_name"    # Run tests matching a pattern
```

### Pre-commit
```bash
pip install pre-commit && pre-commit install
```
Hooks run `ruff check --fix`, `ruff format`, and `pyupgrade --py310-plus` on commit (see `.pre-commit-config.yaml`).

## Architecture

### Entry Points
- `main.py` → `InitialLoader` → boots the full system (event bus, platforms, providers, plugins, WebUI)
- `astrbot/cli/` → CLI entry point (`astrbot init`, `astrbot run`), defined in `pyproject.toml` as `astrbot = "astrbot.cli.__main__:cli"`
- `runtime_bootstrap.py` → early runtime setup (runs before imports)

### Core Layers (`astrbot/core/`)

**Platform** (`platform/`): IM adapters. Each platform is a `Platform` subclass in `platform/sources/<name>/`. Adapters receive messages, convert them to `AstrMessageEvent`, and push into the event bus via an async queue. Registration uses the `@register_platform_adapter` decorator which populates `platform_registry` and `platform_cls_map`.

**Event Bus** (`event_bus.py`): Async queue that receives `AstrMessageEvent` from all platforms. The `dispatch()` loop dequeues events, resolves the config ID via `AstrBotConfigRouter`, and spawns `asyncio.Task` per event through the appropriate `PipelineScheduler`.

**Pipeline** (`pipeline/`): Message processing stages run in order via `PipelineScheduler`. Each stage is a class with a `process()` method that returns either a coroutine or an `AsyncGenerator` (for onion-model pre/post processing). Stages execute in this order:
`preprocess` → `whitelist_check` → `rate_limit_check` → `waking_check` → `session_status_check` → `content_safety_check` → `process_stage` → `result_decorate` → `respond`

Stage order is defined in `pipeline/stage_order.py`. The scheduler supports recursive nesting via the `AsyncGenerator` pattern (onion model).

**Provider** (`provider/`): LLM API wrappers. Each provider is a subclass in `provider/sources/`. Registration uses `@register_provider_adapter`. Provider types include `CHAT_COMPLETION`, `STT` (speech-to-text), `TTS` (text-to-speech), `EMBEDDING`, and `RERANK`. Providers are selected per-conversation via the config manager.

**Star (Plugin System)** (`star/`): Plugins load from `astrbot/builtin_stars/` (built-in) and `data/plugins/` (user-installed). Plugin = "Star", handler = "star_handler". Plugins register event handlers via `@register_star_handler` decorated functions. The `StarHandlerRegistry` maps `EventType` enums to handlers. Key event types: `AdapterMessageEvent`, `OnLLMRequestEvent`, `OnLLMResponseEvent`, `OnAgentBeginEvent`, `OnAgentDoneEvent`, `OnCallingFuncToolEvent`, `OnDecoratingResultEvent`, `OnAfterMessageSentEvent`.

**Agent** (`agent/`): Agentic execution layer. Supports tool calling, MCP clients (`mcp_client.py`), sub-agent orchestration (`subagent_orchestrator.py`), handoff between agents (`handoff.py`), and runners in `agent/runners/`.

**Message** (`message/`): `AstrMessageEvent` base class and `MessageChain` (list of components like `Plain`, `Image`, `At`). Platform adapters build `AstrBotMessage` → events; pipeline produces `MessageChain` for responses.

**Config** (`config/`): Configuration management. Defaults in `config/default.py`. Runtime config in `data/config/`. The `AstrBotConfigManager` manages multiple config profiles, each with its own `PipelineScheduler`.

**DB** (`db/`): SQLite via SQLAlchemy async + SQLModel. Migrations in `db/migration/`. Vector DB support in `db/vec_db/`.

**Knowledge Base** (`knowledge_base/`): RAG system with chunking, parsing (PDF, EPUB, etc.), retrieval (BM25 + vector), and SQLite-backed storage.

### Dashboard (`dashboard/`)
Vue 3 + Vite + Vuetify SPA. Communicates with AstrBot backend via REST API on port 6185. Built output goes to `dashboard/dist/` and is bundled into the wheel via a custom Hatch build hook (`scripts/hatch_build.py`).

### Key Patterns
- **Async-first**: the entire runtime is `asyncio`. Use `aiohttp` for HTTP, `aiosqlite` for DB.
- **Decorator-based registration**: Platform adapters use `@register_platform_adapter`, providers use `@register_provider_adapter`, plugin handlers use `@register_star_handler`.
- **Event-driven pipeline**: Messages flow through `EventBus` → `PipelineScheduler` → ordered stages.
- `send_by_session()` for proactive messages; `event.send()` / `event.send_streaming()` for replies in the pipeline.
- Session ID format: `platform_name:identifier` (e.g., `wecomai:user123`).
- **Config profiles**: Multiple named configurations, each routing to its own pipeline scheduler via `AstrBotConfigRouter`.

## Code Style
- Ruff for linting/formatting (line length 88, Python 3.10 target version)
- `pathlib.Path` for path handling, use `astrbot.core.utils.path_utils` / `astrbot.core.utils.astrbot_path` for data/temp directories
- Conventional commits: `feat:`, `fix:`, `chore:`, etc.
- English comments for new code
- Pre-commit hooks: `pip install pre-commit && pre-commit install`
