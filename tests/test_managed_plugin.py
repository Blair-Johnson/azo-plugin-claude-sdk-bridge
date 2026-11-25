"""Managed bridge lifecycle tests: real Session/Runtime + local control-only Node child.

No Claude authentication, SDK import, inference, or production managed.mjs is
needed. The fixture exercises the actual Popen/pipe/process-group boundary.
"""

from __future__ import annotations

import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from agent_utils.core.events import BeforeLLM, IoInterrupted, RenderRequested, SessionRestored
from agent_utils.core.pipeline import PipelineBuilder
from agent_utils.core.session import Session
from agent_utils.core.state import State
from agent_utils.features.core import core_feature
from agent_utils.runtime.loop import Runtime
from agent_utils.storage.repository import StoredState
from agent_utils.storage.serialization import (
    load_session_json,
    restore_session_document,
    save_session_json,
)
from agent_zoo.core.feature_state import session_state
from agent_zoo.events import SessionReady
from agent_zoo.pipelines.default import build_default_builder
from agent_zoo.storage.load import LoadedSession
from agent_zoo.storage.serialization import restore_session_state
from agent_zoo.testing.sans_io_guard import assert_pipeline_sans_io, audit_module_sans_io


PLUGIN = Path(__file__).resolve().parents[1] / "plugin/claude_sdk_bridge.py"


def load_plugin():
    spec = importlib.util.spec_from_file_location("test_managed_claude_sdk_bridge", PLUGIN)
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def plugin(monkeypatch):
    module = load_plugin()
    monkeypatch.setattr(module, "STARTUP_TIMEOUT", 2.0)
    monkeypatch.setattr(module, "SHUTDOWN_TIMEOUT", 0.15)
    monkeypatch.setattr(module, "GRACEFUL_SHUTDOWN_TIMEOUT", 0.15)
    return module


@pytest.fixture
def fake_bridge(tmp_path):
    node = shutil.which("node")
    if node is None:
        pytest.skip("local Node is required for control-process lifecycle tests")
    roots = []

    def make(mode="ready", local_node=True):
        root = tmp_path / f"bridge-{len(roots)}"
        roots.append(root)
        (root / "src").mkdir(parents=True)
        (root / "node_modules/fixture").mkdir(parents=True)
        (root / "node_modules/fixture/package.json").write_text("{}")
        (root / "package.json").write_text(json.dumps({"dependencies": {"fixture": "1"}}))
        if local_node:
            local = root / ".pixi/envs/default/bin/node"
            local.parent.mkdir(parents=True)
            local.symlink_to(node)
        (root / "src/managed.mjs").write_text("""
import net from 'node:net';
const mode = MODE;
const server = net.createServer(socket => socket.destroy());
let input = '';
let started = false;
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => {
  input += chunk;
  if (started || !input.includes('\\n')) return;
  started = true;
  const config = JSON.parse(input.split('\\n')[0]);
  if (config.version !== 1 || config.port !== 0 || config.ownedProcessGroup !== true || !/^[0-9a-f]{64}$/.test(config.key)) process.exit(2);
  if (mode === 'exit') process.exit(3);
  server.listen(0, '127.0.0.1', () => {
    const ready = {kind: 'ready', version: 1, port: server.address().port, pid: process.pid};
    if (mode === 'wrong_pid') ready.pid++;
    if (mode === 'wrong_port') ready.port = true;
    if (mode === 'wrong_version') ready.version = true;
    if (mode === 'partial') process.stdout.write('{"kind":"ready"');
    else if (mode === 'bad') process.stdout.write('PRIVATE-' + config.key + '\\n');
    else if (mode === 'huge') process.stdout.write('x'.repeat(5000));
    else process.stdout.write(JSON.stringify(ready) + '\\n');
  });
});
function stop() { if (mode !== 'stubborn') server.close(() => process.exit(0)); }
process.stdin.on('end', stop);
process.on('SIGTERM', stop);
""".replace("MODE", json.dumps(mode)))
        return root

    return make


def config():
    bridge = {
        "tags": ["sdk-bridge"],
        "anthropic": {
            "client": {
                "base_url": "https://old.invalid", "base_url_env": "OLD_URL",
                "api_key": "old-fixed-key", "api_key_env": "OLD_KEY",
                "auth_token_env": "OLD_TOKEN", "auth_token": "old-token",
                "default_headers_env": "OLD_HEADERS", "max_retries": 0,
                "default_headers": {"Authorization": "old", "X-API-Key": "old",
                                    "X-AZO-Session-ID": "old", "Host": "old", "keep": "yes"},
            },
            "request": {"model": "sonnet", "max_tokens": 32, "extra_headers_env": "OLD_EXTRA",
                        "extra_headers": {"Authorization": "old", "x-api-key": "old", "keep-request": "yes"}},
        },
    }
    off = copy.deepcopy(bridge)
    off["enabled"] = False
    blocked = copy.deepcopy(bridge)
    blocked["tags"].append("blocked")
    other = {"tags": ["ordinary"], "anthropic": {"client": {"base_url": "https://other.invalid"}}}
    return {"llm": {"default": "other", "disabled_tags": ["blocked"], "models": {
        "bridge": bridge, "bridge-two": copy.deepcopy(bridge), "off": off, "blocked": blocked, "other": other,
    }}}


def build_runtime(plugin, cfg, root=None):
    session = Session()
    builder = PipelineBuilder().add(core_feature())
    plugin.register_features(builder, config=cfg)
    session.pipeline = builder.build(suppress_warnings=True)
    drivers = builder.build_drivers(cfg)
    handler = next(c for c in session.pipeline.components if isinstance(c, plugin.ManagedBridge))
    driver = next(d for d in drivers if isinstance(d, plugin.BridgeDriver))
    if root is not None:
        driver._service.root = root
    runtime = Runtime(session, drivers=drivers)
    return session, runtime, handler, driver


def drain(runtime, *, timeout=5.0):
    deadline = time.monotonic() + timeout
    while True:
        if runtime.step_once(timeout=0.0):
            continue
        if not runtime.session.state.io.in_flight:
            if not runtime.step_once(timeout=0.0):
                break
            continue
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("Timed out waiting for runtime IO to drain")
        runtime.step_once(timeout=min(0.05, remaining))


def emit_ready(runtime, session_id="session-one", instance_id="backend-one"):
    smeta = session_state(runtime.session.state)
    smeta.session_id = session_id
    smeta.instance_id = instance_id
    runtime.post(SessionReady(session_id=session_id, instance_id=instance_id, restored=False))
    drain(runtime)


def test_module_and_pipeline_sans_io_clean(plugin):
    assert audit_module_sans_io(PLUGIN) == []
    session, _runtime, handler, driver = build_runtime(plugin, config())
    assert driver.durability == "process_bound"
    assert driver.request_types == (plugin.StartBridge, plugin.ValidateBridge)
    assert not hasattr(plugin, "STATE_ATTR")
    assert not hasattr(handler, "service")
    assert not hasattr(handler, "session")
    assert not hasattr(plugin.BridgeService, "save_state")
    assert not hasattr(plugin.BridgeService, "restore_state")
    session.ensure_initialized()
    session_state(session.state).session_id = "sans-io-check"
    assert_pipeline_sans_io(
        session.pipeline,
        session=session,
        events=[
            SessionReady(session_id="sans-io-check", instance_id="inst-1"),
            BeforeLLM(),
            SessionRestored(reason="reload"),
        ],
    )


def test_import_registration_and_normal_render_are_side_effect_free(monkeypatch):
    before = dict(os.environ)

    def forbidden(*args, **kwargs):
        pytest.fail("process launched during import, registration, initialization or render")

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    plugin = load_plugin()
    cfg = config()
    original = copy.deepcopy(cfg)
    session, runtime, _handler, driver = build_runtime(plugin, cfg)
    with runtime:
        session.ensure_initialized()
        assert not hasattr(session.state, "claude_sdk_bridge_service")
        session.step(RenderRequested())
        assert driver._service.process is None
        assert cfg == original
        assert dict(os.environ) == before


def test_feature_builds_through_real_pipeline_builder_and_default_builder(plugin):
    session = Session()
    builder = build_default_builder(
        session, config=config(), include_plugins=False, include_llm_drivers=False,
    )
    plugin.register_features(builder, config=config())
    session.pipeline = builder.build()
    session.ensure_initialized()
    assert not hasattr(session.state, "claude_sdk_bridge_service")
    drivers = builder.build_drivers()
    assert any(isinstance(d, plugin.BridgeDriver) for d in drivers)
    assert_pipeline_sans_io(session.pipeline)


@pytest.mark.parametrize("cfg", [
    {}, {"llm": {"models": {}}},
    {"llm": {"models": {"ordinary": {"tags": ["claude"]}}}},
    {"llm": {"models": {"off": {"tags": ["sdk-bridge"], "enabled": "false"}}}},
    {"llm": {"disabled_tags": ["sdk-bridge"], "models": {"off": {"tags": ["sdk-bridge"]}}}},
])
def test_disabled_and_other_profiles_do_not_register(plugin, cfg):
    original = copy.deepcopy(cfg)
    features = []
    plugin.register_features(SimpleNamespace(add=features.append), config=cfg)
    assert not features
    assert cfg == original


def test_driver_submit_is_non_blocking_and_quiesce_interrupts(plugin, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    driver = plugin.BridgeDriver()

    def slow_start():
        entered.set()
        release.wait(5)
        return ("http://127.0.0.1:4242", "AZO_CLAUDE_BRIDGE_LOCAL_FAKE")

    monkeypatch.setattr(driver._service, "start", slow_start)
    emitted = []
    driver.start(emitted.append)
    try:
        t0 = time.monotonic()
        driver.submit("g1:op_1", plugin.StartBridge(session_id="s1"))
        elapsed = time.monotonic() - t0
        assert elapsed < 0.2
        assert entered.wait(2)
        events = driver.quiesce(policy="terminate")
        assert len(events) == 1
        assert isinstance(events[0], IoInterrupted)
        assert events[0].op_id == "g1:op_1"
        release.set()
        time.sleep(0.05)
        assert emitted == []
    finally:
        release.set()
        driver.close()


def test_driver_quiesce_calls_request_stop_to_unblock_inflight_start(plugin, fake_bridge, monkeypatch):
    monkeypatch.setattr(plugin, "STARTUP_TIMEOUT", 5.0)
    driver = plugin.BridgeDriver(root=fake_bridge("partial"))
    emitted = []
    driver.start(emitted.append)
    try:
        driver.submit("g1:op_slow", plugin.StartBridge(session_id="s-slow"))
        # Wait briefly so BridgeService.start() enters _read_ready().
        deadline = time.monotonic() + 2.0
        while driver._service.process is None and time.monotonic() < deadline:
            time.sleep(0.01)
        assert driver._service.process is not None
        t0 = time.monotonic()
        driver.quiesce(policy="terminate", deadline=time.monotonic() + 3.0)
        elapsed = time.monotonic() - t0
        # request_stop() must wake _read_ready() promptly instead of waiting 3s/5s.
        assert elapsed < 1.0
        assert driver._service._stop_event.is_set()
    finally:
        driver.close()


def test_cold_bind_shares_private_config_before_worker_and_runtime_exit_cleans(plugin, fake_bridge, monkeypatch):
    from agent_zoo.llm.catalog import _normalize_provider_sections
    from agent_zoo.features.turn.llm_caller import LLMCaller

    cfg = config()
    original = copy.deepcopy(cfg)
    caller = LLMCaller(config=cfg)  # holds reference to original config dict before SessionReady
    calls = []
    popen = subprocess.Popen

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return popen(*args, **kwargs)

    monkeypatch.setattr(plugin.subprocess, "Popen", spy)
    session, runtime, _handler, driver = build_runtime(plugin, cfg, fake_bridge())
    with runtime:
        emit_ready(runtime)
        runtime.post(BeforeLLM())
        drain(runtime)
        service = driver._service
        proc = service.process
        key, env_name = service._key, service.env_name
        assert len(key) == 64 and int(key, 16) >= 0
        assert len(calls) == 1
        assert key not in repr(calls)
        assert calls[0][1]["start_new_session"] is True
        assert calls[0][1]["stdin"] is subprocess.PIPE
        assert calls[0][1]["stdout"] is subprocess.PIPE
        assert calls[0][1]["stderr"] is subprocess.DEVNULL
        assert os.getpgid(proc.pid) == proc.pid
        assert not proc.stdin.closed
        assert service.endpoint.startswith("http://127.0.0.1:")
        assert int(service.endpoint.rsplit(":", 1)[1]) > 0
        for name in ("bridge", "bridge-two"):
            provider = cfg["llm"]["models"][name]["anthropic"]
            client = provider["client"]
            assert caller.config["llm"]["models"][name]["anthropic"]["client"] is client
            assert client["base_url"] == service.endpoint
            assert client["api_key_env"] == env_name
            assert client["auth_token"] == "local-bridge-not-anthropic"
            assert not {"api_key", "base_url_env", "auth_token_env", "default_headers_env"} & client.keys()
            assert client["default_headers"] == {"keep": "yes", "x-azo-session-id": "{session_id}"}
            assert provider["request"]["extra_headers"] == {"keep-request": "yes"}
            assert "extra_headers_env" not in provider["request"]
            assert _normalize_provider_sections("anthropic", provider)["client"]["api_key"] == key
        assert key not in json.dumps(cfg)
        for name in ("off", "blocked", "other"):
            assert cfg["llm"]["models"][name] == original["llm"]["models"][name]
    assert proc.poll() is not None
    assert proc.stdin.closed and proc.stdout.closed
    assert env_name not in os.environ
    service.cleanup()


def test_real_session_save_restore_and_state_replacement_keep_same_driver_owner(plugin, fake_bridge, tmp_path):
    session, runtime, _handler, driver = build_runtime(plugin, config(), fake_bridge())
    with runtime:
        emit_ready(runtime)
        owner = driver._service
        proc, key = owner.process, owner._key
        path = tmp_path / "session.json"
        save_session_json(session.state, session.pipeline, path)
        saved = json.loads(path.read_text())
        assert "claude_sdk_bridge_service" not in saved["components"]
        assert "claude_sdk_bridge" not in saved["features"]
        assert key not in path.read_text()
        assert "owner_pid" not in path.read_text()

        # Restore into the same session state via load_session_json + restore_session_document
        doc = load_session_json(path)
        restore_session_document(doc, session.pipeline, state=session.state)
        assert not hasattr(session.state, "claude_sdk_bridge_service")
        assert owner.process is proc and owner._key == key

        # Exercise the real runtime restore path against a replacement State + SessionRestored
        loaded = LoadedSession(StoredState(
            repository=tmp_path, journal_path=tmp_path / "source.journal",
            session_id="session-one", instance_id="source", commit_id="commit",
            parent_commit_id=None, created_at=0.0, document=saved,
        ), "local", "exact")
        session.state = State()
        session._initialized = False
        assert restore_session_state(session, loaded)
        session.ensure_initialized()
        session_state(session.state).session_id = "session-one"
        runtime.post(SessionRestored(reason="reload"))
        drain(runtime)
        assert owner.process is proc
        assert plugin.bridge_runtime(session.state).bound is True

        # State replacement with a brand-new session identity
        session.state = State()
        session._initialized = False
        session.ensure_initialized()
        assert plugin.bridge_runtime(session.state).bound is False
        emit_ready(runtime, "session-two")
        runtime.post(BeforeLLM())
        drain(runtime)
        assert owner.process is proc and proc.poll() is None
    assert proc.poll() is not None


def test_two_backends_overwrite_inherited_config_without_adopting_owner(plugin, fake_bridge):
    parent_cfg = config()
    _, parent_rt, _, first_drv = build_runtime(plugin, parent_cfg, fake_bridge())
    with parent_rt:
        emit_ready(parent_rt, "parent-session")
        a = first_drv._service
        parent_env, parent_key = a.env_name, a._key
        child_cfg = copy.deepcopy(parent_cfg)
        _, child_rt, _, second_drv = build_runtime(plugin, child_cfg, fake_bridge())
        with child_rt:
            emit_ready(child_rt, "child-session")
            b = second_drv._service
            assert a.process.pid != b.process.pid
            assert a.endpoint != b.endpoint and a.env_name != b.env_name and a._key != b._key
            assert child_cfg["llm"]["models"]["bridge"]["anthropic"]["client"]["api_key_env"] == b.env_name
            assert os.environ[parent_env] == parent_key
            child_proc, child_env = b.process, b.env_name
        assert child_proc.poll() is not None and child_env not in os.environ
        assert a.process.poll() is None and os.environ[parent_env] == parent_key
    assert parent_env not in os.environ


def test_before_llm_and_session_ready_strictly_require_bound_identity_and_live_manager(plugin, fake_bridge):
    session, runtime, _handler, driver = build_runtime(plugin, config(), fake_bridge())
    with runtime:
        session_state(session.state).session_id = "session-one"
        with pytest.raises(RuntimeError, match="matching bind"):
            runtime.post(BeforeLLM())
            drain(runtime)

        emit_ready(runtime, "session-one")
        runtime.post(BeforeLLM())
        drain(runtime)

        session_state(session.state).session_id = "unbound-identity"
        with pytest.raises(RuntimeError, match="matching bind"):
            runtime.post(BeforeLLM())
            drain(runtime)

        with pytest.raises(RuntimeError, match="mismatched session identity"):
            runtime.post(SessionReady(session_id="session-one", instance_id="backend-one"))
            drain(runtime)

        emit_ready(runtime, "unbound-identity")
        driver._service.process.terminate()
        driver._service.process.wait(timeout=2)
        with pytest.raises(RuntimeError, match="not ready"):
            emit_ready(runtime, "unbound-identity")


@pytest.mark.parametrize("field,value", [
    ("base_url", "https://foreign.invalid"), ("api_key", "foreign"),
    ("default_headers", {"Authorization": "foreign", "x-azo-session-id": "{session_id}"}),
])
def test_tampering_rejected_on_before_llm_and_session_ready(plugin, fake_bridge, field, value):
    cfg = config()
    _, runtime, _, _ = build_runtime(plugin, cfg, fake_bridge())
    with runtime:
        emit_ready(runtime)
        cfg["llm"]["models"]["bridge"]["anthropic"]["client"][field] = value
        with pytest.raises(RuntimeError, match="configuration changed"):
            runtime.post(BeforeLLM())
            drain(runtime)
        with pytest.raises(RuntimeError, match="configuration changed"):
            emit_ready(runtime)


@pytest.mark.parametrize("mode, message", [
    ("partial", "timed out"), ("bad", "invalid"), ("wrong_pid", "invalid"),
    ("wrong_port", "invalid"), ("wrong_version", "invalid"), ("huge", "too large"), ("exit", "before readiness"),
])
def test_bounded_bad_startup_cleans_up_driver_owner(plugin, fake_bridge, mode, message, monkeypatch):
    monkeypatch.setattr(plugin, "STARTUP_TIMEOUT", 0.3)
    session, runtime, _handler, driver = build_runtime(plugin, config(), fake_bridge(mode))
    with runtime:
        started = time.monotonic()
        with pytest.raises(RuntimeError, match=message) as error:
            emit_ready(runtime)
        assert time.monotonic() - started < 3
        owner = driver._service
        assert not hasattr(session.state, "claude_sdk_bridge_service")
        assert session.state.readiness.is_ready
        assert owner.process.poll() is not None
        assert owner.env_name not in os.environ
        assert "PRIVATE-" not in str(error.value)


def test_config_failure_after_launch_is_cleaned_by_runtime_exit(plugin, fake_bridge):
    cfg = config()
    cfg["llm"]["models"]["bridge"]["anthropic"]["client"] = None
    session, runtime, _handler, driver = build_runtime(plugin, cfg, fake_bridge())
    with pytest.raises(AttributeError):
        with runtime:
            emit_ready(runtime)
    assert not hasattr(session.state, "claude_sdk_bridge_service")
    assert driver._service.process.poll() is not None
    assert driver._service.env_name not in os.environ


def test_cleanup_escalates_only_owned_group_and_is_bounded(plugin, fake_bridge, monkeypatch):
    signals = []
    killpg = os.killpg

    def spy(pid, sig):
        signals.append((pid, sig))
        return killpg(pid, sig)

    monkeypatch.setattr(plugin.os, "killpg", spy)
    _, runtime, _, driver = build_runtime(plugin, config(), fake_bridge("stubborn"))
    with runtime:
        emit_ready(runtime)
        owner = driver._service
        pid = owner.process.pid
        start = time.monotonic()
        owner.cleanup()
        assert time.monotonic() - start < 2
        assert owner.process.poll() == -signal.SIGKILL
        assert signals == [(pid, signal.SIGTERM), (pid, signal.SIGKILL)]
        owner.cleanup()
        assert len(signals) == 2


def test_inherited_handle_cannot_signal_or_remove_parent_env(plugin, fake_bridge):
    _, runtime, _, driver = build_runtime(plugin, config(), fake_bridge())
    with runtime:
        emit_ready(runtime)
        owner = driver._service
        actual_owner = owner.owner_pid
        owner.owner_pid += 1  # represents the same object inherited by another PID
        try:
            owner.cleanup()
            assert owner.process.poll() is None
            assert os.environ[owner.env_name] == owner._key
            with pytest.raises(RuntimeError, match="not ready"):
                owner.start()
        finally:
            owner.owner_pid = actual_owner


def test_cleanup_preserves_env_value_it_no_longer_owns(plugin, fake_bridge):
    _, runtime, _, driver = build_runtime(plugin, config(), fake_bridge())
    with runtime:
        emit_ready(runtime)
        name = driver._service.env_name
        try:
            os.environ[name] = "replacement"
            driver._service.cleanup()
            assert os.environ[name] == "replacement"
            os.environ[name] = ""
            driver._service.cleanup()
            assert name in os.environ  # an empty replacement is not our cleared key
        finally:
            os.environ.pop(name, None)


def test_local_node_preferred_and_path_fallback(plugin, fake_bridge):
    local_root = fake_bridge()
    assert plugin.BridgeService(local_root)._command()[0] == str(local_root / ".pixi/envs/default/bin/node")
    fallback_root = fake_bridge(local_node=False)
    assert plugin.BridgeService(fallback_root)._command()[0] == shutil.which("node")


def test_missing_dependencies_fail_locally_before_popen(plugin, fake_bridge, monkeypatch):
    root = fake_bridge()
    (root / "node_modules/fixture/package.json").unlink()
    monkeypatch.setattr(plugin.subprocess, "Popen", lambda *a, **kw: pytest.fail("launch before dependency validation"))
    session, runtime, _handler, driver = build_runtime(plugin, config(), root)
    with runtime:
        with pytest.raises(RuntimeError, match="dependencies are missing"):
            emit_ready(runtime)
        assert driver._service.process is None


def test_spawn_failure_leaves_clean_state_without_private_environment(plugin, fake_bridge, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("cannot execute Node")

    monkeypatch.setattr(plugin.subprocess, "Popen", fail)
    session, runtime, _handler, driver = build_runtime(plugin, config(), fake_bridge())
    with runtime:
        with pytest.raises(OSError, match="cannot execute Node"):
            emit_ready(runtime)
        owner = driver._service
        assert owner.process is None and owner._key == ""
        assert owner.env_name not in os.environ
        assert session.state.readiness.is_ready


def test_before_llm_gated_via_readiness_and_released_on_complete_or_interrupt(plugin, fake_bridge):
    session, runtime, _handler, _driver = build_runtime(plugin, config(), fake_bridge())
    session_state(session.state).session_id = "session-gate"
    # Step SessionReady without dispatching to driver yet: op is in-flight & readiness held
    effects = session.step(SessionReady(session_id="session-gate", instance_id="inst-1"))
    assert len(effects) == 1
    assert isinstance(effects[0].request, plugin.StartBridge)
    rt = plugin.bridge_runtime(session.state)
    assert rt.pending_op == effects[0].op_id
    assert not session.state.readiness.is_ready
    assert plugin.READINESS_KEY in session.state.readiness.holds
    # BeforeLLM while bridge startup is in-flight stops propagation and records wait
    before_ev = BeforeLLM()
    session.step(before_ev)
    assert before_ev.stopped is True
    assert rt.waiting_before_llm is True
    # Clean up via Runtime dispatch -> readiness released and bound
    with runtime:
        runtime.drivers.dispatch(effects)
        drain(runtime)
        assert rt.bound is True
        assert session.state.readiness.is_ready
        assert plugin.READINESS_KEY not in session.state.readiness.holds

    # Also verify IoInterrupted releases readiness cleanly without raising
    session2, _rt2, _h2, _d2 = build_runtime(plugin, config(), fake_bridge())
    session_state(session2.state).session_id = "session-interrupt"
    effects2 = session2.step(SessionReady(session_id="session-interrupt", instance_id="inst-2"))
    assert not session2.state.readiness.is_ready
    session2.step(IoInterrupted(op_id=effects2[0].op_id, reason="shutdown"))
    rt2 = plugin.bridge_runtime(session2.state)
    assert rt2.pending_op is None
    assert rt2.bound is False
    assert session2.state.readiness.is_ready
