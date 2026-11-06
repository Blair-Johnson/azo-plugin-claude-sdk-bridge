# Claude SDK bridge

This optional local provider uses the Claude Agent SDK behind Agent Zoo's existing Anthropic Messages client. It adds no core hooks, changes no default model, and does not replace Agent Zoo's runtime. Agent Zoo still runs tools, applies its permission and pipeline logic, and owns the conversation history.

The design follows [pi-claude-bridge](https://github.com/elidickinson/pi-claude-bridge), pinned at `7923e38e1de81dd9562e75754061dd631488e7f3`. Claude Code sees only renamed MCP tools. Each handler waits for a result keyed by the model's tool-call ID; it never performs that action. A complete assistant tool batch returns through the Messages endpoint. Agent Zoo executes it normally, and its next request supplies the results that let the SDK continue. The SDK remains an internal inference process, not a replacement for the host tool loop.

## Opt-in setup

Dependencies are isolated here. Nothing installs into Agent Zoo's root environment, modifies its bundled model catalog, or changes its default model. The lockfiles pin the same Agent SDK and transcript library versions used by the reference adapter.

From this directory, install the optional runtime:

```sh
pixi run install
```

Use Claude Code's normal login before starting the bridge. The bridge itself does not extract OAuth tokens, run its own login flow, or change extra-usage settings. Verify the account and billing settings you intend to use; SDK acceptance is not proof that a response was charged to plan limits rather than extra usage.

Generate a separate local bridge key once. This key only authenticates the loopback endpoint; it is not an Anthropic credential.

```sh
mkdir -p "$HOME/.config/azo-claude-bridge"
chmod 700 "$HOME/.config/azo-claude-bridge"
umask 077
openssl rand -hex -out "$HOME/.config/azo-claude-bridge/key" 32
pixi run start --key-file "$HOME/.config/azo-claude-bridge/key"
```

In the terminal that launches Agent Zoo, export that same local key and explicitly select the optional model. Use the absolute path of this checkout's example fragment:

```sh
export AZO_CLAUDE_BRIDGE_KEY="$(cat "$HOME/.config/azo-claude-bridge/key")"
azo --config /absolute/path/to/integrations/claude-sdk-bridge/models.example.yaml \
    --model claude-sonnet-sdk-bridge
```

On current Agent Zoo, an explicit config file replaces the top-level sections it supplies: using this fragment replaces `llm` for that launch. To retain your normal catalog, add its entry to your existing `config/models.yaml` instead (or use the local setup command below). The fragment deliberately does not set `llm.default`, `harness.claude_oauth`, or a disabled `api` tag. The `{session_id}` header is resolved by the unchanged native provider and is required by the endpoint. Child Agent Zoo sessions use separate IDs; their model configuration must also select this profile if you want them to use the bridge.

Do not set `ANTHROPIC_CUSTOM_HEADERS` for this profile: ambient SDK headers can override explicit local client headers. The example supplies a harmless local-only `auth_token` to prevent inheriting an unrelated Anthropic bearer token. Its `api_key_env` supplies the actual local key. The bridge never forwards either local credential to Anthropic.

For a persistent local setup without environment exports, run `configure_local.py` with Agent Zoo's Python environment, an explicit `--config /path/to/config/models.yaml`, a private `--key-file /path/to/bridge/key`, and one or more `--model PROFILE=SDK_MODEL` arguments. It appends new profiles, preserves existing entries and the default, writes a mode-0600 backup, and stores a generated loopback-only key in the private config and key file without printing it. Existing profile names and backup files are never overwritten. Start the server with that same `--key-file`. The resulting profiles use a conservative 200k context budget with host compaction at 150k; these are operational limits, not claims about each model's maximum capacity.
The server binds only `127.0.0.1:8766`. `--port`, `--idle-seconds` (default 300), `--timeout-seconds` (120), `--max-sessions` (16), and `--system-prompt-mode` (`append` or `custom`) are configurable. Change the profile's `base_url` if you change the port. Stop the foreground server with Ctrl-C. `GET /health` requires the local `x-api-key`; `DELETE /v1/session` additionally requires `x-azo-session-id` and explicitly cancels that SDK session. There is no unauthenticated management endpoint or CORS access.

## Host functionality and boundaries

Agent Zoo's existing tool registration, validation, permission checks, plugins, UI, persistence, and subagent orchestration remain in their normal paths. The bridge exposes the tools supplied on each request as MCP definitions, preserving supported JSON schemas. Root-level `anyOf`, `oneOf`, and `allOf` are rejected because the pinned SDK can silently omit those tools; nested constraints remain intact. Unlike the reference's flattening workaround, this adapter does not weaken those schemas. Built-in Claude Code tools, automatic compaction, project settings, and user hooks are disabled. By default, host instructions are appended in full to the SDK's Claude Code prompt preset, matching the reference's prompt construction. `--system-prompt-mode custom` instead uses the complete host prompt without that preset and enables the pinned CLI's simple mode to prevent automatic environment/model/date attachments from altering tool-result content. Neither mode performs keyword filtering or selective removal of Agent Zoo instructions. Custom mode reduces preset overhead but may have different provider acceptance; it is not a billing workaround.

The provider supports text, custom tool calls, parallel tool batches, tool errors, base64 image tool results, user images, signed thinking, usage counters, streaming-format responses, and ordinary subsequent user turns. Tool names are translated through explicit maps in both directions. Unknown model tools, mismatched IDs, conflicting duplicate results, and unsupported content are errors, not guesses. Tool results must close the complete pending batch before another model response begins.

Agent Zoo's incoming transcript is authoritative. Matching history continues a parked SDK query. Changed history, changed model or tool definitions, context compaction, a new branch, an expired query, or a server restart causes a fresh derived session to be imported through `cc-session-io`. Unlike the reference's more elaborate prefix-copy machinery, this initial adaptation favors full rebuilds when continuation is unsafe. It does not preserve cache equivalence across rebuilds or retain a separate persistent pointer database. The host still retains its normal session history. Claude Code can normalize tool results, add image-source annotations, or save oversized outputs under its own `tool-results` directory; this is also a limitation of the reference approach. Agent Zoo retains the original result, and a rebuild imports that host version, so exact cache/token equivalence is not guaranteed. Any follow-up file access still has to go through a supplied Agent Zoo tool.

When a rebuild occurs at a tool-result boundary, the real host results are imported before the SDK starts, avoiding Claude Code's automatic interrupted-call repair. If no new user instruction accompanies those results, the bridge supplies one fixed SDK-only continuation instruction, as the reference adapter does. That instruction is not inserted into Agent Zoo's stored transcript. Results accompanied by steering use the actual steering instruction instead. Rebuilds therefore preserve host outcomes, but are not byte-identical API replays.

A private temporary working directory prevents accidental loading of project instructions; it is not an OS sandbox. The SDK process runs as your user and still owns its authentication, network requests, internal prompt construction, and transcript interpretation. Disabling built-in tools and denying unknown tools restricts the available action surface, not all behavior of the executable. SDK and transcript-library upgrades require rerunning the compatibility tests.

## Compatibility limits

The endpoint is a deliberately narrow Messages adapter, not a general Anthropic API proxy. It accepts automatic tool selection, thinking settings, effort, and a maximum-output-token request. Temperature, top-p/top-k, forced tool choice, server tools, structured-output formats, PDF/document blocks, and unknown request controls are rejected. API prompt-cache markers are ignored for history comparison; the SDK controls its own prompt caching. Incompatible thinking signatures from a different provider must be removed by the host's normal replay conversion, not forged or silently repaired here.

Responses use native Messages SSE framing and forward genuine text/thinking progress during generation. The complete tool batch is registered before the terminal event that allows the host to execute it. Pings cover periods with no model output but are not counted as model progress. If an SDK retry would replace already-published content, the request fails instead of mixing responses; unsupported nonstreaming fallback tool batches also fail rather than guessing a batch boundary. An active disconnect or timeout cancels inference; successful tool handoff keeps the query parked. Closing a UI session while no HTTP request is active is not immediately visible to the sidecar, so idle expiry bounds its lifetime. The bridge never cancels or re-executes already-running host tools.

No subscription-billing guarantee is made. This uses the real Agent SDK, without spoofing another client's headers or modifying provider attribution. Anthropic may reject requests or apply account-dependent usage rules. The bridge does not enable extra usage and does not fall back to a metered API key.


Mid-answer feedback uses `--interrupt-mode reuse` by default. Agent Zoo cancels its current provider request as usual; the bridge asks the SDK to interrupt rather than waiting for that answer to finish. Once both the interrupt acknowledgement and the old turn's terminal result arrive, it sends only the newly appended feedback on the same Claude process. Repeated feedback arriving during that handshake is combined without sending superseded prompts twice. The acknowledgement must report no queued messages, and the handshake is bounded to 500 ms before falling back to a rebuild.

Claude retains its interrupted partial answer and interruption marker in that live SDK session. These private records are not added to Agent Zoo's saved transcript; a later history rebuild drops them. This is an intentional tradeoff for prompt feedback, not exact transcript equivalence. An unresolved tool call or callback, changed history or request settings, failed interrupt, or nonempty SDK queue instead triggers the safer reconstruction path. The bridge does not substitute Claude Code's synthetic rejected-tool result for a real Agent Zoo outcome. Use `--interrupt-mode rebuild` to require reconstruction on every canceled request. Neither mode interrupts already-running Agent Zoo tools; their execution remains governed by the normal host runtime.
## Tests and reference map

Run the isolated Node tests from this directory:

```sh
pixi run test
```

Run the real Agent Zoo client compatibility tests from the repository root with its normal Python environment:

```sh
pixi run python -m pytest -q integrations/claude-sdk-bridge/tests/test_agent_zoo_provider.py
```

The Python test exercises the unchanged `make_llm_fn` in both streaming and JSON modes, first against a deterministic endpoint and then through the actual bridge, real SDK child, MCP callbacks, and loopback fake model API. Install this integration's dependencies before running it. Node tests exercise ID routing, schema fidelity, history import, session lifecycle, HTTP auth, errors, cancellation, and the real SDK against an isolated local fake model server. These are offline protocol tests, not live-account or plan-billing tests.

The reference's `src/index.ts`, `mcp-server.ts`, and `query-state.ts` motivate `src/engine.mjs` and `src/tools.mjs`. Its `session-sync.ts` and `session-transcript.ts` motivate `src/history.mjs`; `cc-child.ts` and `query-process.ts` motivate `src/child.mjs`. `src/server.mjs` and the model fragment are the Agent Zoo-specific additive boundary. See `THIRD_PARTY_NOTICES.md` for attribution and the reference's MIT license.

## Opt-in live latency checks

The benchmark programs make real model requests only with `--live`. They create separate synthetic sessions, use the SDK's normal login, and do not log credentials. Install this integration's dependencies first, then run from this directory:

```sh
pixi run node benchmarks/live-latency.mjs --live --replicates 3
pixi run node benchmarks/live-latency.mjs --live --replicates 3 --history-kib 32
pixi run node benchmarks/sdk-interrupt.mjs --live --replicates 3
pixi run node benchmarks/runtime-live.mjs --live --replicates 3 --python /absolute/path/to/agent-zoo/python
```

`live-latency` measures first-text latency for cold, reused, rebuilt, and interrupted/replaced requests through the real local HTTP endpoint. Reuse and rebuild receive the same short instruction and canonical history. The difference includes history import, SDK startup, model/cache behavior, and network variance; it is not a pure process-spawn measurement. Teardown, process starts, import time, usage, and actual cancellation/exit evidence are recorded separately. The repeated synthetic history is a controlled size test, not a representative long working session.

`sdk-interrupt` isolates the SDK's interrupt-and-replace operation on one process without tools. `runtime-live` exercises normal `InteractiveSessionIO.send_user_text`, the actual Agent Zoo runtime, and its provider worker, injecting feedback after the first model text. It requires cancellation of the first native worker, an acknowledged SDK interruption with no queued messages, the old turn's terminal result, reuse of the same Claude query/process, and acceptance of only the replacement answer. Both HTTP and runtime benchmarks accept `--interrupt-mode rebuild` to measure the stricter restart path instead. The runtime benchmark does not exercise the terminal frontend or measure rendering latency. Each program emits structured JSON suitable for `rx` capture; install/setup time is outside its latency measurements.
