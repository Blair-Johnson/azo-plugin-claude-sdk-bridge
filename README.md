# Claude SDK bridge

This optional local provider uses the Claude Agent SDK behind Agent Zoo's existing Anthropic Messages client. It adds no core hooks, changes no default model, and does not replace Agent Zoo's runtime. Agent Zoo still runs tools, applies its permission and pipeline logic, and owns the conversation history.

The design follows [pi-claude-bridge](https://github.com/elidickinson/pi-claude-bridge), pinned at `7923e38e1de81dd9562e75754061dd631488e7f3`. Claude Code sees only renamed MCP tools. Each handler waits for a result keyed by the model's tool-call ID; it never performs that action. A complete assistant tool batch returns through the Messages endpoint. Agent Zoo executes it normally, and its next request supplies the results that let the SDK continue. The SDK remains an internal inference process, not a replacement for the host tool loop.

## Managed setup (normal use)

Dependencies are isolated here. Nothing installs into Agent Zoo's root environment, modifies its bundled model catalog, or changes its default model. The lockfiles pin the same Agent SDK and transcript library versions used by the reference adapter.

Install the runtime and common-scope plugin once, from this integration's source directory:

```sh
cd /absolute/path/to/agent-zoo/integrations/claude-sdk-bridge
pixi install
pixi run install  # npm ci with the checked-in lockfile
azo-plugin install "$PWD/plugin/claude_sdk_bridge.py" common
```

The plugin installation links the original source file; keep this checkout and its isolated dependencies available. Do not copy the plugin into a different directory: it locates the Node runtime and bridge files relative to its source. Use Claude Code's normal login. The bridge does not extract OAuth tokens, run its own login flow, enable extra usage, or change billing settings.

Add profiles to your existing `config/models.yaml` using Agent Zoo's Python environment. From the repository root:

```sh
pixi run python integrations/claude-sdk-bridge/configure_local.py \
    --config /absolute/path/to/config/models.yaml \
    --model claude-sonnet-sdk-bridge=sonnet \
    --model claude-opus-sdk-bridge=opus
azo --model claude-sonnet-sdk-bridge
```

After this one-time setup, launch `azo` normally and select a bridge profile when wanted. There is no server to start, fixed port to reserve, key file to generate, or environment variable to export. The setup helper appends new names, preserves the original prefix, comments, default model, and unrelated catalog entries, and writes the updated config and an exclusive backup with mode 0600. Backups use `.before-managed-claude-sdk-bridge` with a unique numeric suffix when needed; existing backups are never overwritten. Profiles use a conservative 200k context budget and host compaction at 150k, operational limits rather than claims about each model's maximum capacity.

To migrate an old profile, pass `--migrate-existing` with the same explicit `--config` and one `--model PROFILE=SDK_MODEL` per profile to change. Migration only accepts existing `sdk-bridge`-tagged entries in the helper's recognized trailing bridge block, including the old `# Local Claude Agent SDK bridge; key is loopback-only.` marker. It removes the selected profiles' persistent local keys and replaces fixed URLs with managed placeholders. Unrequested entries stay unchanged. Warning comments are retained; ambiguous layouts, aliases, inline comments in edited entries, or unrelated entries in the trailing block are rejected for manual review. Backups can contain old local keys, so keep them private. The helper does not delete old key files or stop a previously launched manual server.

You can also merge `models.example.yaml` under your existing `llm.models`. Its port-1 loopback URL is deliberately unusable, and `AZO_CLAUDE_BRIDGE_KEY` is only a compatibility placeholder: the plugin overrides both endpoint and key when the backend binds. Without the installed plugin, the profile fails closed rather than reaching Anthropic directly. Supplying the fragment with `azo --config` replaces the top-level `llm` section for that launch; merging it into your existing catalog retains your other models. It does not set `llm.default`, `harness.claude_oauth`, or a disabled `api` tag.

Do not set `ANTHROPIC_CUSTOM_HEADERS` for bridge profiles: ambient SDK headers can override explicit local client headers. The local-only `auth_token` prevents inheriting an unrelated Anthropic bearer token. The `{session_id}` header is resolved by the unchanged native provider and is required for per-session routing. Child sessions use separate IDs and must select a bridge profile to use it. Local endpoint credentials are never forwarded to Anthropic.

Fable profiles require usage credits on the actual Claude account. Selecting a model does not enable credits or alter billing settings. Check the account you intend to use; SDK acceptance alone is not proof that a response used subscription limits rather than extra usage.

## Managed lifecycle

The common-scope Feature manages one loopback Node sidecar per backend, with an OS-assigned port and a fresh ephemeral key. Only tagged bridge profiles are patched in memory; endpoint credentials are not written into model configuration. The lightweight Node process can remain idle for the backend's lifetime. The Claude SDK inference process is lazy: it starts only when a bridge request needs it, not merely because Agent Zoo launched or a profile was selected.

The sidecar survives Python provider-worker replacement during steering and model switches. This lets compatible requests reuse SDK state rather than tying its lifetime to one provider worker. Changed history or incompatible requests can still require a rebuild. Session resource cleanup releases that Session's bridge state; backend shutdown closes the sidecar and its children. Idle expiry bounds unused SDK queries, including state left parked after switching away from a bridge model. This process ownership is not an OS sandbox; the trust boundary is described below.

## Host functionality and boundaries

Agent Zoo's existing tool registration, validation, permission checks, plugins, UI, persistence, and subagent orchestration remain in their normal paths. The bridge exposes the tools supplied on each request as MCP definitions, preserving supported JSON schemas. Root-level `anyOf`, `oneOf`, and `allOf` are rejected because the pinned SDK can silently omit those tools; nested constraints remain intact. Unlike the reference's flattening workaround, this adapter does not weaken those schemas. Built-in Claude Code tools, automatic compaction, project settings, and user hooks are disabled. By default, host instructions are appended in full to the SDK's Claude Code prompt preset, matching the reference's prompt construction. `--system-prompt-mode custom` instead uses the complete host prompt without that preset. Both modes suppress `CLAUDE_CODE_SIMPLE`, including inherited or explicitly overridden values: the pinned CLI's simple mode bypasses stored OAuth discovery. Normal mode preserves SDK-owned login and may add its own environment/model/date context to upstream messages; these additions are not host-authored transcript content. Neither mode performs keyword filtering or selective removal of Agent Zoo instructions. Custom mode reduces preset overhead but may have different provider acceptance; it is not a billing workaround.

The provider supports text, custom tool calls, parallel tool batches, tool errors, base64 image tool results, user images, signed thinking, usage counters, streaming-format responses, and ordinary subsequent user turns. Tool names are translated through explicit maps in both directions. Unknown model tools, mismatched IDs, conflicting duplicate results, and unsupported content are errors, not guesses. Tool results must close the complete pending batch before another model response begins.

Agent Zoo's incoming transcript is authoritative. Matching history continues a parked SDK query. Changed history, changed model or tool definitions, context compaction, a new branch, an expired query, or a server restart causes a fresh derived session to be imported through `cc-session-io`. Unlike the reference's more elaborate prefix-copy machinery, this initial adaptation favors full rebuilds when continuation is unsafe. It does not guarantee cache equivalence across all rebuilds or retain a separate persistent pointer database. The host still retains its normal session history. Native Anthropic responses now retain their complete ordered blocks for replay, including text boundaries and signatures. This exact replay is used only when the host-visible content, thinking, and tool calls are unchanged and the endpoint/model/API identity matches; edited or legacy messages use the ordinary conversion path. Claude Code can group assistant text ahead of tool calls, normalize tool results, add image-source annotations, or save oversized outputs under its own `tool-results` directory; this is also a limitation of the reference approach. Agent Zoo retains the original result, and a rebuild imports that host version, so exact cache/token equivalence is not guaranteed. Any follow-up file access still has to go through a supplied Agent Zoo tool.

When a rebuild occurs at a tool-result boundary, the real host results are imported before the SDK starts, avoiding Claude Code's automatic interrupted-call repair. If no new user instruction accompanies those results, an empty SDK input triggers continuation without adding bridge-authored continuation text. The SDK may still add its own startup context. Results accompanied by steering use the actual steering instruction instead. The bridge never invents an interruption or resume announcement. Offline real-SDK tests verify every host block for these rebuilds in custom-prompt mode while explicitly accounting for the pinned SDK's startup attachments and separator newline; they do not assert full upstream-transcript equality. The SDK normalization limits above still apply.

A private temporary working directory prevents accidental loading of project instructions; it is not an OS sandbox. Each live host slot retains its own directory across SDK rebuilds so its working-directory context does not change solely because a mirror was reconstructed. This does not guarantee identical full upstream messages: the pinned SDK can regenerate or relocate its own context attachments on cold start or history rebuild, including appending them to an imported tool result. Tests compare the stable system/tool prefix and ordinary reused-query context, while accounting explicitly for those SDK additions and separately requiring exact host history and tool results. Idle eviction, explicit release, or bridge restart ends that lifetime. Cleanup waits for both setup completion and child exit before deleting the directory. The SDK process runs as your user and still owns its authentication, network requests, internal prompt construction, and transcript interpretation. Disabling built-in tools and denying unknown tools restricts the available action surface, not all behavior of the executable. SDK and transcript-library upgrades require rerunning the compatibility tests.

SDK failures pass the actual error details through HTTP and SSE responses, including exception stacks and causes, SDK error messages, and captured process stderr. Lifecycle and rebuild context supplements those details rather than replacing them. Error content is not redacted. The process stderr buffer retains the latest 65,536 characters with an explicit truncation marker when necessary to bound memory use.

## Compatibility limits

The endpoint is a deliberately narrow Messages adapter, not a general Anthropic API proxy. It accepts automatic tool selection, thinking settings, effort, and a maximum-output-token request. Temperature, top-p/top-k, forced tool choice, server tools, structured-output formats, PDF/document blocks, and unknown request controls are rejected. API prompt-cache markers are ignored for history comparison; the SDK controls its own prompt caching. Incompatible thinking signatures from a different provider must be removed by the host's normal replay conversion, not forged or silently repaired here.

Responses use native Messages SSE framing and forward genuine text/thinking progress during generation. The complete tool batch is registered before the terminal event that allows the host to execute it. Pings cover periods with no model output but are not counted as model progress. If an SDK retry would replace already-published content, the request fails instead of mixing responses; unsupported nonstreaming fallback tool batches also fail rather than guessing a batch boundary. An active disconnect or timeout cancels inference; successful tool handoff keeps the query parked. In managed use, Session resource cleanup explicitly releases its SDK state. With the standalone diagnostic server, closing a UI session while no HTTP request is active is not immediately visible, so idle expiry bounds its lifetime. The bridge never cancels or re-executes already-running host tools.

No subscription-billing guarantee is made. This uses the real Agent SDK, without spoofing another client's headers or modifying provider attribution. Anthropic may reject requests or apply account-dependent usage rules. The bridge does not enable extra usage and does not fall back to a metered API key.


Mid-answer feedback uses `--interrupt-mode reuse` by default. Agent Zoo cancels its current provider request as usual; the bridge asks the SDK to interrupt rather than waiting for that answer to finish. Once both the interrupt acknowledgement and the old turn's terminal result arrive, it sends only the newly appended feedback on the same Claude process. Repeated feedback arriving during that handshake is combined without sending superseded prompts twice. The acknowledgement must report no queued messages, and the handshake is bounded to 500 ms before falling back to a rebuild.

Claude retains its interrupted partial answer and interruption marker in that live SDK session. These private records are not added to Agent Zoo's saved transcript; a later history rebuild drops them. This is an intentional tradeoff for prompt feedback, not exact transcript equivalence. An unresolved tool call or callback, changed history or request settings, failed interrupt, or nonempty SDK queue instead triggers the safer reconstruction path. The bridge does not substitute Claude Code's synthetic rejected-tool result for a real Agent Zoo outcome. Use `--interrupt-mode rebuild` to require reconstruction on every canceled request. Neither mode interrupts already-running Agent Zoo tools; their execution remains governed by the normal host runtime.

## Standalone CLI (diagnostics only)

Normal managed use does not need this server. For isolated transport debugging, `pixi run start --key-file /path/to/private/diagnostic-key --port 8766` runs the foreground CLI. Supply a separately prepared mode-0600 loopback key and a separate diagnostic client pointed at that exact URL and key; the managed example's port-1 placeholder is not a standalone-server configuration. Do not put diagnostic credentials into your normal managed profiles. Stop the foreground server with Ctrl-C.

The standalone CLI binds only `127.0.0.1`. Options include `--port`, `--idle-seconds` (default 300), `--timeout-seconds` (0, meaning no total request deadline; client disconnects still cancel inference), `--max-sessions` (16), and `--system-prompt-mode` (`append` or `custom`). `GET /health` requires the local `x-api-key`; `DELETE /v1/session` additionally requires `x-azo-session-id` and explicitly cancels that SDK session. There is no unauthenticated management endpoint or CORS access. These CLI controls are diagnostic options, not flags to pass to `azo`.

## Tests and reference map

Run the isolated Node tests from this directory:

```sh
pixi run test
```

Run the real Agent Zoo client compatibility tests from the repository root with its normal Python environment:

```sh
pixi run python -m pytest -q \
    integrations/claude-sdk-bridge/tests/test_configure_local.py \
    integrations/claude-sdk-bridge/tests/test_agent_zoo_provider.py
```

The setup tests use temporary config files and synthetic keys. The provider test exercises the unchanged `make_llm_fn` in both streaming and JSON modes, first against a deterministic endpoint and then through the actual bridge, real SDK child, MCP callbacks, and loopback fake model API. Install this integration's dependencies before running it. Node tests exercise ID routing, schema fidelity, history import, session lifecycle, HTTP auth, errors, cancellation, and the real SDK against an isolated local fake model server. These are offline protocol tests, not live-account or plan-billing tests.

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
