"""Managed bridge lifecycle tests: real Session + local control-only Node child.

No Claude authentication, SDK import, inference, or production managed.mjs is
needed. The fixture exercises the actual Popen/pipe/process-group boundary.
"""

import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time
from types import SimpleNamespace

import pytest

from agent_utils import PipelineBuilder, Session
from agent_utils.types import State
from agent_zoo.runtime.live_backend import _notify_startup_ready
from agent_zoo.runtime_support import bind_session_runtime, restore_session_state


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


def component(plugin, session, cfg, root=None):
    features = []
    plugin.register_features(SimpleNamespace(add=features.append), session=session, config=cfg)
    assert len(features) == 1
    result = features[0].components[0]
    if root is not None:
        result.service.root = root
    session.pipeline = [result]
    return result


def bind(session, identity="session-one"):
    bind_session_runtime(session, session_id=identity, project_name="test", db=SimpleNamespace())


def ready(session):
    session.state._instance_id = "backend-one"
    _notify_startup_ready(session, restored=False)


def test_import_registration_and_normal_call_are_side_effect_free(monkeypatch):
    before = dict(os.environ)

    def forbidden(*args, **kwargs):
        pytest.fail("process launched during import, registration, initialization or normal call")

    monkeypatch.setattr(subprocess, "Popen", forbidden)
    plugin = load_plugin()
    with Session() as session:
        cfg = config()
        original = copy.deepcopy(cfg)
        handler = component(plugin, session, cfg)
        session.ensure_initialized()
        assert getattr(session.state, plugin.STATE_ATTR) is handler.service
        assert handler(session.state) is session.state
        assert handler.service.process is None
        assert cfg == original
        assert dict(os.environ) == before


def test_feature_builds_through_real_pipeline_builder(plugin):
    with Session() as session:
        builder = PipelineBuilder(session)
        plugin.register_features(builder, session=session, config=config())
        session.pipeline = builder.build(suppress_warnings=True)
        session.ensure_initialized()
        assert isinstance(getattr(session.state, plugin.STATE_ATTR), plugin.BridgeService)


@pytest.mark.parametrize("cfg", [
    {}, {"llm": {"models": {}}},
    {"llm": {"models": {"ordinary": {"tags": ["claude"]}}}},
    {"llm": {"models": {"off": {"tags": ["sdk-bridge"], "enabled": "false"}}}},
    {"llm": {"disabled_tags": ["sdk-bridge"], "models": {"off": {"tags": ["sdk-bridge"]}}}},
])
def test_disabled_and_other_profiles_do_not_register(plugin, cfg):
    original = copy.deepcopy(cfg)
    features = []
    plugin.register_features(SimpleNamespace(add=features.append), session=Session(), config=cfg)
    assert not features
    assert cfg == original


def test_cold_bind_shares_private_config_before_worker_and_session_exit_cleans(plugin, fake_bridge, monkeypatch):
    from agents.llm import _normalize_provider_sections
    from agents.llm_subprocess import LLMProcessSupervisor

    cfg = config()
    original = copy.deepcopy(cfg)
    supervisor = LLMProcessSupervisor(cfg)  # root copy before bind: nested registry must stay shared
    calls = []
    popen = subprocess.Popen

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return popen(*args, **kwargs)

    monkeypatch.setattr(plugin.subprocess, "Popen", spy)
    with Session() as session:
        handler = component(plugin, session, cfg, fake_bridge())
        bind(session)
        ready(session)
        service = handler.service
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
        assert supervisor._proc is None  # manager readiness is not inference
        for name in ("bridge", "bridge-two"):
            provider = cfg["llm"]["models"][name]["anthropic"]
            client = provider["client"]
            assert supervisor._config["llm"]["models"][name]["anthropic"]["client"] is client
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


def test_real_session_save_restore_and_state_replacement_keep_same_owner(plugin, fake_bridge, tmp_path):
    with Session() as session:
        handler = component(plugin, session, config(), fake_bridge())
        bind(session)
        owner = handler.service
        proc, key = owner.process, owner._key
        path = tmp_path / "session.json"
        session.save(str(path))
        saved = json.loads(path.read_text())
        assert saved["components"][plugin.STATE_ATTR] == {"version": 1}
        assert key not in path.read_text()
        assert "owner_pid" not in path.read_text()
        session.load(str(path))
        session.ensure_initialized()
        assert getattr(session.state, plugin.STATE_ATTR) is owner
        assert owner.process is proc and owner._key == key
        # Exercise the real runtime restore path against a replacement state,
        # which must obtain this SAME owner from our init factory.
        from agent_utils.session_repository import StoredState
        from agent_zoo.session_load import LoadedSession
        loaded = LoadedSession(StoredState(
            repository=tmp_path, journal_path=tmp_path / "source.journal",
            session_id="session-one", instance_id="source", commit_id="commit",
            parent_commit_id=None, created_at=0.0, document=saved,
        ), "local", "exact")
        session.state = State()
        session._initialized = False
        assert restore_session_state(session, loaded)
        session.ensure_initialized()
        assert getattr(session.state, plugin.STATE_ATTR) is owner
        bind(session)
        assert owner.process is proc
        session.state = State()
        session._initialized = False
        session.ensure_initialized()
        assert getattr(session.state, plugin.STATE_ATTR) is owner
        bind(session, "session-two")
        ready(session)
        assert owner.process is proc and proc.poll() is None
        owner.restore_state({"version": 1, "pid": 1, "owner_pid": 1, "key": "foreign", "endpoint": "foreign"})
        assert owner.process is proc and owner._key == key
    assert proc.poll() is not None


def test_restore_never_acquires_foreign_ownership(plugin):
    owner = plugin.BridgeService()
    owner.restore_state({"version": 1, "pid": os.getpid(), "key": "foreign", "env_name": "PATH"})
    assert owner.process is None and owner.owner_pid is None
    assert owner._key == "" and owner.env_name == ""
    assert owner.save_state() == {"version": 1}
    owner.cleanup()


def test_two_backends_overwrite_inherited_config_without_adopting_owner(plugin, fake_bridge):
    with Session() as parent:
        parent_cfg = config()
        first = component(plugin, parent, parent_cfg, fake_bridge())
        bind(parent)
        a = first.service
        parent_env, parent_key = a.env_name, a._key
        with Session() as child:
            child_cfg = copy.deepcopy(parent_cfg)
            second = component(plugin, child, child_cfg, fake_bridge())
            bind(child, "child-session")
            b = second.service
            assert a.process.pid != b.process.pid
            assert a.endpoint != b.endpoint and a.env_name != b.env_name and a._key != b._key
            assert child_cfg["llm"]["models"]["bridge"]["anthropic"]["client"]["api_key_env"] == b.env_name
            assert os.environ[parent_env] == parent_key
            child_proc, child_env = b.process, b.env_name
        assert child_proc.poll() is not None and child_env not in os.environ
        assert a.process.poll() is None and os.environ[parent_env] == parent_key
    assert parent_env not in os.environ


def test_session_ready_strictly_requires_bound_identity_and_live_manager(plugin, fake_bridge):
    with Session() as session:
        handler = component(plugin, session, config(), fake_bridge())
        session.state._session_id = "session-one"
        with pytest.raises(RuntimeError, match="matching bind"):
            ready(session)
        bind(session)
        ready(session)
        session.state._session_id = "unbound-identity"
        with pytest.raises(RuntimeError, match="matching bind"):
            ready(session)
        bind(session, "unbound-identity")
        handler.service.process.terminate()
        handler.service.process.wait(timeout=2)
        with pytest.raises(RuntimeError, match="not ready"):
            ready(session)
        with pytest.raises(RuntimeError, match="not ready"):
            bind(session, "unbound-identity")


@pytest.mark.parametrize("field,value", [
    ("base_url", "https://foreign.invalid"), ("api_key", "foreign"),
    ("default_headers", {"Authorization": "foreign", "x-azo-session-id": "{session_id}"}),
])
def test_ready_rejects_config_tampering(plugin, fake_bridge, field, value):
    with Session() as session:
        cfg = config()
        component(plugin, session, cfg, fake_bridge())
        bind(session)
        cfg["llm"]["models"]["bridge"]["anthropic"]["client"][field] = value
        with pytest.raises(RuntimeError, match="configuration changed"):
            ready(session)


@pytest.mark.parametrize("mode, message", [
    ("partial", "timed out"), ("bad", "invalid"), ("wrong_pid", "invalid"),
    ("wrong_port", "invalid"), ("wrong_version", "invalid"), ("huge", "too large"), ("exit", "before readiness"),
])
def test_bounded_bad_startup_retains_state_cleanup_owner(plugin, fake_bridge, mode, message, monkeypatch):
    monkeypatch.setattr(plugin, "STARTUP_TIMEOUT", 0.3)
    with Session() as session:
        handler = component(plugin, session, config(), fake_bridge(mode))
        started = time.monotonic()
        with pytest.raises(RuntimeError, match=message) as error:
            bind(session)
        assert time.monotonic() - started < 3
        owner = handler.service
        assert getattr(session.state, plugin.STATE_ATTR) is owner
        assert owner.process.poll() is not None
        assert owner.env_name not in os.environ
        assert "PRIVATE-" not in str(error.value)


def test_config_failure_after_launch_is_cleaned_by_real_session_exit(plugin, fake_bridge):
    with pytest.raises(AttributeError):
        with Session() as session:
            cfg = config()
            cfg["llm"]["models"]["bridge"]["anthropic"]["client"] = None
            handler = component(plugin, session, cfg, fake_bridge())
            bind(session)
    assert getattr(session.state, plugin.STATE_ATTR) is handler.service
    assert handler.service.process.poll() is not None
    assert handler.service.env_name not in os.environ


def test_cleanup_escalates_only_owned_group_and_is_bounded(plugin, fake_bridge, monkeypatch):
    signals = []
    killpg = os.killpg

    def spy(pid, sig):
        signals.append((pid, sig))
        return killpg(pid, sig)

    monkeypatch.setattr(plugin.os, "killpg", spy)
    with Session() as session:
        handler = component(plugin, session, config(), fake_bridge("stubborn"))
        bind(session)
        owner = handler.service
        pid = owner.process.pid
        start = time.monotonic()
        owner.cleanup()
        assert time.monotonic() - start < 2
        assert owner.process.poll() == -signal.SIGKILL
        assert signals == [(pid, signal.SIGTERM), (pid, signal.SIGKILL)]
        owner.cleanup()
        assert len(signals) == 2


def test_inherited_handle_cannot_signal_or_remove_parent_env(plugin, fake_bridge):
    with Session() as session:
        handler = component(plugin, session, config(), fake_bridge())
        bind(session)
        owner = handler.service
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
    with Session() as session:
        handler = component(plugin, session, config(), fake_bridge())
        bind(session)
        name = handler.service.env_name
        try:
            os.environ[name] = "replacement"
            handler.service.cleanup()
            assert os.environ[name] == "replacement"
            os.environ[name] = ""
            handler.service.cleanup()
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
    with Session() as session:
        handler = component(plugin, session, config(), root)
        with pytest.raises(RuntimeError, match="dependencies are missing"):
            bind(session)
        assert getattr(session.state, plugin.STATE_ATTR) is handler.service
        assert handler.service.process is None


def test_spawn_failure_retains_cleanup_owner_without_private_environment(plugin, fake_bridge, monkeypatch):
    def fail(*args, **kwargs):
        raise OSError("cannot execute Node")

    monkeypatch.setattr(plugin.subprocess, "Popen", fail)
    with Session() as session:
        handler = component(plugin, session, config(), fake_bridge())
        with pytest.raises(OSError, match="cannot execute Node"):
            bind(session)
        owner = handler.service
        assert getattr(session.state, plugin.STATE_ATTR) is owner
        assert owner.process is None and owner._key == ""
        assert owner.env_name not in os.environ
