# Fork notes — agandhi4/graphiti

Personal fork of [getzep/graphiti](https://github.com/getzep/graphiti) powering
the local Claude memory MCP server on this laptop. Working branch:
`feat/falkordblite-support`, kept rebased onto `upstream/main` with a
deliberately minimal delta (4 feature commits + this docs commit).
Fork-specific docs live in this file only — the sole upstream file we touch is
CLAUDE.md, which gets a one-line "Fork notice" pointer to @FORK.md near the top
(so Claude sessions auto-load fork context; trivially re-applied if a rebase
conflicts on it). Upstream READMEs are never edited.

## The delta (upstream/main..HEAD)

| Commit | What |
|---|---|
| falkordblite MCP provider | `database.provider: falkordblite` in mcp_server — builds a redislite `AsyncFalkorDB` and injects it into the stock `FalkorDriver` |
| Claude Code proxy + base_url passthrough | `claude-proxy/` serves the Anthropic Messages API on `127.0.0.1:11437`; a one-line `AnthropicClient` change + factory wiring let `providers.anthropic.api_url` point at it |
| local launch config | `mcp_server/config/config-local.yaml` + `mcp_server/start.sh` (the launchd entrypoint) |
| durable spool | disk-backed episode queue with dead-letter in `mcp_server/src/services/queue_service.py` |
| fork docs | this file + the CLAUDE.md pointer line |

History: before the 0.29.3 rebase (2026-08-30) the fork carried a full
`FalkorLiteDriver` subclass. Upstream now supports falkordb-lite via the
`FalkorDriver(falkor_db=...)` injection parameter and its stock `close()`
(aclose + init-task cancel) and `clone()` (client reuse) cover everything the
subclass did, so it was deleted. Pre-rebase history: tag
`pre-rebase-0.29-backup`.

## Local deployment

```
launchd (com.graphiti.mcp-server, KeepAlive=true)
  └─ mcp_server/start.sh  →  uv run main.py --config config/config-local.yaml
       ├─ MCP server: SSE on 127.0.0.1:11436  (/sse)
       ├─ LLM: anthropic provider → claude-proxy on 127.0.0.1:11437 (Haiku)
       ├─ Embedder: LM Studio on localhost:1234 (nomic-embed-text v1.5, 768d)
       ├─ Reranker: CrossEncoderFactory falls back to the OpenAI-provider
       │            embedder endpoint (LM Studio) — NOT the claude-proxy
       └─ Database: falkordblite, embedded at ~/.graphiti/data
```

- The launchd plist lives OUTSIDE the repo: `~/Library/LaunchAgents/com.graphiti.mcp-server.plist`.
- Restart: `launchctl kickstart -k gui/501/com.graphiti.mcp-server`
- Stop for maintenance (KeepAlive would defeat a plain kill):
  `launchctl bootout gui/501/com.graphiti.mcp-server`, later
  `launchctl bootstrap gui/501 ~/Library/LaunchAgents/com.graphiti.mcp-server.plist`
- Logs: `mcp_server/server.{stdout,stderr}.log`
- Restarting invalidates MCP sessions — running Claude sessions must `/mcp` reconnect.

### claude-proxy

Accepts Anthropic Messages API requests on :11437 and routes them through a
warm, sandboxed `claude` subprocess (Claude Code subscription → zero API token
cost; slow-sequential FIFO is the accepted trade-off). Self-healing: hung
requests kill and recreate the session, retry once; the session is recycled
every 25 requests. The subprocess runs in `claude-proxy/sandbox/` with no file
permissions. Ingestion latency of minutes–hours through this proxy is normal.

### Durable spool (episode queue)

Layout under `GRAPHITI_QUEUE_DIR` (default `~/.graphiti/queue/`):

- `pending/` — records awaiting processing or mid-retry (10 attempts, capped
  exponential backoff spanning ~2.5h of outage)
- `dead/` — exhausted records; an audit trail, never deleted. Re-cycled with a
  fresh attempt budget only on server restart. Corrupt files become `.corrupt`
  alongside them.

Records are JSON; deserialization tolerates records from older formats (new
fields default), so a version upgrade never strands spooled episodes. Replay
uses the record's original `enqueued_at` as the reference time, preserving the
knowledge-graph timeline.

**Test gotcha**: `mcp_server/tests/conftest.py` has an autouse fixture pointing
`GRAPHITI_QUEUE_DIR` at pytest's tmp_path. Without it, upstream's queue tests
would replay and consume the REAL pending spool into a mock. Never run
mcp_server tests with that fixture removed.

## Rebase procedure (next upstream sync)

1. `git fetch upstream && git tag pre-rebase-<version>-backup`
2. Rebuild rather than rebase: `git checkout -B feat/falkordblite-support upstream/main`,
   then re-port the 4 delta commits (they're small and isolated by design).
3. Watch for: upstream churn in `mcp_server/src/services/queue_service.py`
   (thread any new `add_episode` params through the spool record with safe
   defaults) and in the factories (keep the falkordblite + anthropic api_url
   wiring).
4. `uv sync --extra dev` (dev is an optional-dependencies *extra*, not a
   dependency group — `--dev` fails). mcp_server is its own uv project.
5. Test: CI-scoped unit suite + mcp_server tests + a factory smoke test against
   a throwaway db path. Then `git push --force-with-lease` and push the tag.
6. mcp_server `requires-python` is pinned `>=3.12` (falkordblite marker needs
   it); upstream CI assumes 3.10 — irrelevant locally, uv fetches 3.12.
