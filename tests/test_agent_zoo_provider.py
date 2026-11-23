"""Offline contract test through the unchanged native Agent Zoo provider.

Run via `pixi run test-host` with AZO_HOST_MANIFEST selecting Agent Zoo's Python
environment. NODE_BINARY selects this plugin's Pixi node. No remote calls.
"""
import copy
import json
import os
from pathlib import Path
import selectors
import subprocess

import pytest
import yaml

from agent_zoo.llm import make_llm_fn

ROOT = Path(__file__).resolve().parents[1]
KEY = "synthetic-local-bridge-key-not-claude-12345"
TOOLS = [{"type": "function", "function": {
    "name": "probe_add", "description": "Add integers in the host.",
    "parameters": {"type": "object", "properties": {
        "a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"]},
}}]


@pytest.mark.parametrize("stream", [True, False])
@pytest.mark.parametrize("fixture,continuation", [
    ("azo-endpoint.mjs", False),
    ("azo-sdk-endpoint.mjs", False),
    ("azo-sdk-endpoint.mjs", True),
])
def test_native_provider_owns_tool_execution(monkeypatch, stream, fixture, continuation):
    monkeypatch.setenv("AZO_CLAUDE_BRIDGE_KEY", KEY)
    monkeypatch.setenv("AZO_SDK_FIXTURE_CONTINUATION", "1" if continuation else "0")
    for name in ("ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_CUSTOM_HEADERS"):
        monkeypatch.delenv(name, raising=False)
    process = subprocess.Popen(
        [os.environ.get("NODE_BINARY", "node"), str(ROOT / "tests/fixtures" / fixture)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            assert selector.select(timeout=10), "Local fixture did not start"
            line = process.stdout.readline()
            assert line, "Local fixture exited before startup; run the integration's pixi install task first"
            port = json.loads(line)["port"]
        config = yaml.safe_load((ROOT / "models.example.yaml").read_text())
        config["llm"]["default"] = "claude-sonnet-sdk-bridge"
        profile = config["llm"]["models"]["claude-sonnet-sdk-bridge"]
        profile["anthropic"]["client"]["base_url"] = f"http://127.0.0.1:{port}"
        profile["anthropic"]["request"]["stream"] = stream
        model = make_llm_fn(config)
        context = {"session_id": "native-azo-fixture"}
        messages = [{"role": "system", "content": "Host-owned instructions"},
                    {"role": "user", "content": "Use probe_add for 2 + 3."}]
        original = copy.deepcopy(messages)
        response = model(messages, TOOLS, request_context=context)
        assert messages == original
        assert len(response["tool_calls"]) == 1
        if continuation:
            prefix = "OFFLINE_CONTINUATION_PREFIX: unfinished response; "
            assert response["content"].startswith(prefix)
            assert response["content"].count(prefix) == 1
            assert response["content"].count("Before the tool.") == 1
            assert response["content"].count("After the tool.") == 1
        assert response["_usage"]["output_tokens"] == (6 if continuation else 3)
        assert response["_usage"]["cache_read_input_tokens"] == 11
        assert response["_usage"]["prompt_tokens"] == 18
        call = response["tool_calls"][0]
        assert call["id"] == "tool_fixture"
        assert call["function"]["name"] == "probe_add"
        args = json.loads(call["function"]["arguments"])
        assert args == {"a": 2, "b": 3}
        result = args["a"] + args["b"]  # Only the host performs the tool action.
        messages += [response, {"role": "tool", "tool_call_id": call["id"], "content": str(result)}]
        final = model(messages, TOOLS, request_context=context)
        assert final["content"] == "HOST_RESULT_5"
        assert final["_usage"]["cache_read_input_tokens"] == 11
        assert final["_usage"]["prompt_tokens"] == 18
    finally:
        process.terminate()
        try:
            _, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            _, stderr = process.communicate(timeout=5)
        if stderr:
            print(stderr)  # Pytest exposes synthetic fixture diagnostics on failure.
