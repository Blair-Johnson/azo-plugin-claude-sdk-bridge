"""Backend-owned, request-lazy Claude SDK bridge (common userspace feature)."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import secrets
import select
import shutil
import signal
import subprocess
import threading
import time
from typing import Any

from agent_utils.core.events import (
    BeforeLLM,
    Event,
    IoCompleted,
    IoFailed,
    IoInterrupted,
    SessionRestored,
)
from agent_utils.core.io import IoRequest, OpId
from agent_utils.core.pipeline import EmitFn, Feature
from agent_utils.core.state import State
from agent_utils.features.core import IoFence
from agent_zoo.durability import ThreadPoolDriver, session_state
from agent_zoo.events import SessionReady
from agent_zoo.features.turn.llm_caller import LLMCaller


RUNTIME_KEY = "claude_sdk_bridge"
READINESS_KEY = "claude_sdk_bridge"
STARTUP_TIMEOUT = 10.0
SHUTDOWN_TIMEOUT = 1.0
GRACEFUL_SHUTDOWN_TIMEOUT = 6.0
_AUTH_HEADERS = {"authorization", "proxy-authorization", "x-api-key", "x-azo-session-id", "host"}


@dataclass
class StartBridge(IoRequest):
    """Start (or validate an already-live) backend-owned Claude SDK bridge."""

    session_id: str = ""


@dataclass
class ValidateBridge(IoRequest):
    """Validate that the backend-owned Claude SDK bridge process is still live."""

    session_id: str = ""


@dataclass
class BridgeRuntimeState:
    """Ephemeral per-session bridge binding state stored under ``state.runtime(...)``."""

    session_id: str = ""
    endpoint: str = ""
    env_name: str = ""
    pending_op: OpId | None = None
    waiting_before_llm: bool = False
    bound: bool = False
    last_error: BaseException | None = None


def bridge_runtime(state: State) -> BridgeRuntimeState:
    """Return the ephemeral per-session ``BridgeRuntimeState``."""
    return state.runtime(RUNTIME_KEY, BridgeRuntimeState)


class BridgeService:
    """Private process-bound bridge owner managed exclusively by ``BridgeDriver``."""

    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
        self.process: subprocess.Popen[bytes] | None = None
        self.owner_pid: int | None = None
        self.endpoint = ""
        self.env_name = ""
        self._key = ""
        self._closed = False
        self._lock = threading.RLock()
        self._stop_event = threading.Event()

    def request_stop(self) -> None:
        """Wake any in-progress readiness wait so ``cleanup()`` can proceed promptly."""
        if self.owner_pid is not None and self.owner_pid != os.getpid():
            return
        self._stop_event.set()

    def _command(self) -> list[str]:
        local_node = self.root / ".pixi/envs/default/bin/node"
        node = str(local_node) if local_node.is_file() and os.access(local_node, os.X_OK) else shutil.which("node")
        script = self.root / "src/managed.mjs"
        if not node or not script.is_file():
            raise RuntimeError("Claude SDK bridge needs Node and src/managed.mjs; repair its installation.")
        try:
            dependencies = json.loads((self.root / "package.json").read_text())["dependencies"]
            missing = [name for name in dependencies if not (self.root / "node_modules" / name / "package.json").is_file()]
        except (OSError, ValueError, KeyError, TypeError):
            raise RuntimeError("Claude SDK bridge dependency manifest is unavailable; repair its installation.") from None
        if missing:
            raise RuntimeError("Claude SDK bridge dependencies are missing; install them before starting the backend.")
        return [node, str(script)]

    def start(self) -> tuple[str, str]:
        with self._lock:
            if self._closed or self._stop_event.is_set():
                raise RuntimeError("Claude SDK bridge owner is already closed.")
            if self.process is not None:
                self.validate()
                return self.endpoint, self.env_name
            command = self._command()
            self.owner_pid = os.getpid()
            self._key = secrets.token_hex(32)
            self.env_name = "AZO_CLAUDE_BRIDGE_LOCAL_" + secrets.token_hex(16).upper()
            try:
                # No credentials in argv, logs, or durable state. The open stdin is
                # also the manager's parent-death lifeline. Never launch via a shell.
                self.process = subprocess.Popen(
                    command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                    stderr=subprocess.DEVNULL, start_new_session=True, bufsize=0,
                )
                assert self.process.stdin is not None
                self.process.stdin.write(
                    (json.dumps({"version": 1, "key": self._key, "port": 0, "ownedProcessGroup": True}) + "\n").encode()
                )
                ready = self._read_ready()
                if (not isinstance(ready, dict) or ready.get("kind") != "ready"
                        or type(ready.get("version")) is not int or ready["version"] != 1
                        or type(ready.get("pid")) is not int or ready["pid"] != self.process.pid
                        or type(ready.get("port")) is not int or not 0 < ready["port"] < 65536):
                    raise RuntimeError("Claude SDK bridge returned an invalid readiness handshake.")
                self.endpoint = f"http://127.0.0.1:{ready['port']}"
                os.environ[self.env_name] = self._key
                self.validate()
                return self.endpoint, self.env_name
            except BaseException:
                self.cleanup()
                raise

    def _read_ready(self) -> Any:
        assert self.process is not None and self.process.stdout is not None
        deadline = time.monotonic() + STARTUP_TIMEOUT
        data = bytearray()
        fd = self.process.stdout.fileno()
        while b"\n" not in data:
            if self._stop_event.is_set():
                raise RuntimeError("Claude SDK bridge startup cancelled.")
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("Claude SDK bridge startup timed out.")
            ready_fds, _, _ = select.select([fd], [], [], min(0.05, max(0.0, remaining)))
            if not ready_fds:
                continue
            chunk = os.read(fd, 4096)
            if not chunk:
                raise RuntimeError("Claude SDK bridge exited before readiness.")
            data.extend(chunk)
            if len(data) > 4096:
                raise RuntimeError("Claude SDK bridge readiness handshake is too large.")
        try:
            return json.loads(data.split(b"\n", 1)[0])
        except (ValueError, UnicodeError):
            # Do not echo child output: it could contain private material.
            raise RuntimeError("Claude SDK bridge returned an invalid readiness handshake.") from None

    def validate(self) -> tuple[str, str]:
        with self._lock:
            if (self._closed or self.owner_pid != os.getpid() or self.process is None
                    or self.process.poll() is not None or not self.endpoint
                    or not self._key or os.environ.get(self.env_name) != self._key):
                raise RuntimeError("Claude SDK bridge is not ready in this backend; restart the backend.")
            return self.endpoint, self.env_name

    def _signal_group(self, sig: int) -> None:
        proc = self.process
        # The live, unreaped Popen child is the ownership handle, NOT a saved
        # PID. A forked backend must never signal its parent's manager. Do not
        # signal after poll has reaped the child (its PID may then be reused).
        if self.owner_pid == os.getpid() and proc is not None and proc.poll() is None:
            try:
                if os.getpgid(proc.pid) == proc.pid:
                    os.killpg(proc.pid, sig)
            except ProcessLookupError:
                pass

    def cleanup(self) -> None:
        if self.owner_pid is not None and self.owner_pid != os.getpid():
            return
        self._stop_event.set()
        with self._lock:
            if self.owner_pid is not None and self.owner_pid != os.getpid():
                return
            self._closed = True
            proc = self.process
            try:
                if proc is not None:
                    if proc.stdin is not None and not proc.stdin.closed:
                        proc.stdin.close()
                    for sig in (None, signal.SIGTERM, signal.SIGKILL):
                        if sig is not None:
                            self._signal_group(sig)
                        try:
                            proc.wait(timeout=GRACEFUL_SHUTDOWN_TIMEOUT if sig is None else SHUTDOWN_TIMEOUT)
                            break
                        except subprocess.TimeoutExpired:
                            continue
            finally:
                if proc is not None and proc.stdout is not None:
                    proc.stdout.close()
                if self.env_name and self._key and os.environ.get(self.env_name) == self._key:
                    os.environ.pop(self.env_name, None)
                self._key = ""
                self.endpoint = ""


class BridgeDriver(ThreadPoolDriver):
    """Non-blocking process-bound IO driver privately owning ``BridgeService``."""

    durability = "process_bound"
    request_types = (StartBridge, ValidateBridge)
    max_workers = 1
    worker_prefix = "claude-sdk-bridge"
    interrupt_on_quiesce = True

    def __init__(self, *, root: str | Path | None = None) -> None:
        super().__init__()
        self._service = BridgeService(root)

    def execute(self, req: IoRequest) -> dict[str, str]:
        if isinstance(req, StartBridge):
            endpoint, env_name = self._service.start()
        elif isinstance(req, ValidateBridge):
            endpoint, env_name = self._service.validate()
        else:
            raise TypeError(f"Unsupported bridge request: {type(req).__name__}")
        return {"endpoint": endpoint, "env_name": env_name}

    def quiesce(
        self,
        *,
        policy: str = "terminate",
        deadline: float | None = None,
    ) -> list[Event]:
        request_stop = getattr(self._service, "request_stop", None)
        if callable(request_stop):
            request_stop()
        return super().quiesce(policy=policy, deadline=deadline)

    def on_close(self) -> None:
        self._service.request_stop()
        self._service.cleanup()


def _clean_headers(headers: dict[str, Any] | None) -> dict[str, Any]:
    return {key: value for key, value in (headers or {}).items() if key.lower() not in _AUTH_HEADERS}


class ManagedBridge:
    """Pure Sans-I/O pipeline component binding bridge-tagged LLM profiles."""

    handles = (SessionReady, SessionRestored, BeforeLLM, IoCompleted, IoFailed, IoInterrupted)
    reads: set[str] = set()
    writes: set[str] = set()
    optional_reads: set[str] = set()

    def __init__(
        self,
        config: dict[str, Any],
        profiles: list[str],
    ) -> None:
        self.config = config
        self.profiles = list(profiles)

    def _clients(self):
        # enabled_llm_models returns copies. Always edit the original registry:
        # the runtime and LLMCaller share these nested dictionaries even when
        # the root configuration is shallow-copied.
        models = self.config["llm"]["models"]
        for name in self.profiles:
            provider = models[name].setdefault("anthropic", {})
            yield provider, provider.setdefault("client", {})

    def _apply_binding(self, endpoint: str, env_name: str) -> None:
        for provider, client in self._clients():
            for key in ("api_key", "base_url_env", "auth_token_env", "default_headers_env"):
                client.pop(key, None)
            client.update(
                base_url=endpoint,
                api_key_env=env_name,
                auth_token="local-bridge-not-anthropic",
            )
            headers = _clean_headers(client.get("default_headers"))
            headers["x-azo-session-id"] = "{session_id}"
            client["default_headers"] = headers
            request = provider.get("request", {})
            request.pop("extra_headers_env", None)
            if "extra_headers" in request:
                request["extra_headers"] = _clean_headers(request["extra_headers"])

    def verify_config(self, rt: BridgeRuntimeState) -> None:
        for provider, client in self._clients():
            headers = client.get("default_headers", {})
            expected_headers = {**_clean_headers(headers), "x-azo-session-id": "{session_id}"}
            request = provider.get("request", {})
            if (client.get("base_url") != rt.endpoint
                    or client.get("api_key_env") != rt.env_name
                    or client.get("auth_token") != "local-bridge-not-anthropic"
                    or any(key in client for key in ("api_key", "base_url_env", "auth_token_env", "default_headers_env"))
                    or headers != expected_headers or "extra_headers_env" in request
                    or request.get("extra_headers", {}) != _clean_headers(request.get("extra_headers"))):
                raise RuntimeError("Claude SDK bridge runtime configuration changed after binding.")

    def handle(self, event: Event, state: State, emit: EmitFn) -> None:
        if isinstance(event, (SessionReady, SessionRestored)):
            self._handle_session_lifecycle(event, state)
        elif isinstance(event, IoCompleted):
            self._handle_io_completed(event, state, emit)
        elif isinstance(event, IoFailed):
            self._handle_io_failed(event, state)
        elif isinstance(event, IoInterrupted):
            self._handle_io_interrupted(event, state)
        elif isinstance(event, BeforeLLM):
            self._handle_before_llm(event, state)

    def _handle_session_lifecycle(self, event: SessionReady | SessionRestored, state: State) -> None:
        rt = bridge_runtime(state)
        smeta = session_state(state)
        state_sid = str(smeta.session_id or "").strip()
        event_sid = str(getattr(event, "session_id", "") or "").strip()
        session_id = event_sid or state_sid
        if not session_id:
            raise RuntimeError("Claude SDK bridge requires a bound session identity.")
        if event_sid and state_sid and event_sid != state_sid:
            raise RuntimeError("Claude SDK bridge SessionReady arrived with mismatched session identity.")
        if not state_sid:
            smeta.session_id = session_id

        if rt.bound:
            self.verify_config(rt)

        if rt.pending_op is not None:
            state.io.cancel(rt.pending_op)
            rt.pending_op = None

        rt.session_id = session_id
        rt.last_error = None
        op_id = state.io.submit(StartBridge(session_id=session_id), tag=RUNTIME_KEY)
        rt.pending_op = op_id
        state.readiness.hold(READINESS_KEY, "Starting Claude SDK bridge")

    def _handle_io_completed(self, event: IoCompleted, state: State, emit: EmitFn) -> None:
        rt = bridge_runtime(state)
        if rt.pending_op is None or event.op_id != rt.pending_op:
            return
        rt.pending_op = None
        state.readiness.release(READINESS_KEY)

        payload = event.value if isinstance(event.value, dict) else {}
        endpoint = str(payload.get("endpoint") or "")
        env_name = str(payload.get("env_name") or "")
        if not endpoint or not env_name:
            err = RuntimeError("Claude SDK bridge driver returned an incomplete binding.")
            rt.last_error = err
            raise err

        if not rt.bound:
            self._apply_binding(endpoint, env_name)
            rt.endpoint = endpoint
            rt.env_name = env_name
            rt.bound = True
        else:
            if (endpoint, env_name) != (rt.endpoint, rt.env_name):
                self._apply_binding(endpoint, env_name)
                rt.endpoint = endpoint
                rt.env_name = env_name
            self.verify_config(rt)

        if rt.waiting_before_llm:
            rt.waiting_before_llm = False
            if state.readiness.is_ready:
                emit(BeforeLLM())

    def _handle_io_failed(self, event: IoFailed, state: State) -> None:
        rt = bridge_runtime(state)
        if rt.pending_op is None or event.op_id != rt.pending_op:
            return
        rt.pending_op = None
        state.readiness.release(READINESS_KEY)
        rt.bound = False
        rt.waiting_before_llm = False
        err = event.error if isinstance(event.error, BaseException) else RuntimeError(str(event.error))
        rt.last_error = err
        raise err

    def _handle_io_interrupted(self, event: IoInterrupted, state: State) -> None:
        rt = bridge_runtime(state)
        if rt.pending_op is None or event.op_id != rt.pending_op:
            return
        rt.pending_op = None
        state.readiness.release(READINESS_KEY)
        rt.bound = False
        rt.waiting_before_llm = False

    def _handle_before_llm(self, event: BeforeLLM, state: State) -> None:
        rt = bridge_runtime(state)
        if rt.pending_op is not None:
            rt.waiting_before_llm = True
            event.stop()
            return
        if rt.last_error is not None:
            raise rt.last_error
        current_sid = str(session_state(state).session_id or "").strip()
        if not rt.bound or not rt.session_id or current_sid != rt.session_id:
            raise RuntimeError("Claude SDK bridge BeforeLLM arrived without a matching bind.")
        self.verify_config(rt)


def register_features(builder: Any, *, config: dict[str, Any]) -> None:
    from agent_zoo.llm import enabled_llm_models

    # Missing registry is normal in inspector/minimal configurations.
    if not config.get("llm", {}).get("models"):
        return
    profiles = []
    for name, entry in enabled_llm_models(config).items():
        tags = entry.get("tags", [])
        tags = [tags] if isinstance(tags, str) else (tags or [])
        if "sdk-bridge" in tags or entry.get("tag") == "sdk-bridge":
            profiles.append(name)
    if profiles:
        builder.add(
            Feature(
                name="claude_sdk_bridge",
                components=[ManagedBridge(config, profiles)],
                order=[IoFence, ManagedBridge, LLMCaller],
                drivers=[BridgeDriver],
            )
        )


# ---------------------------------------------------------------------------
# Deterministic Simulation Testing (DST) Hooks
# ---------------------------------------------------------------------------


def register_sim_handlers(sim: Any, rng: Any) -> None:
    """Register deterministic in-memory simulation handlers for bridge startup/validation."""
    from agent_utils.runtime.simulation import SimOutcome

    bridge_state: dict[str, Any] = {"started": False, "endpoint": "", "env_name": ""}

    def _handle_start_bridge(_op_id: OpId, _req: StartBridge, _sim: Any) -> SimOutcome:
        if not bridge_state["started"]:
            port = rng.randint(40000, 59999)
            token = f"{rng.randrange(1 << 64):016X}"
            bridge_state["endpoint"] = f"http://127.0.0.1:{port}"
            bridge_state["env_name"] = f"AZO_CLAUDE_BRIDGE_SIM_{token}"
            bridge_state["started"] = True
        return SimOutcome(
            value={
                "endpoint": bridge_state["endpoint"],
                "env_name": bridge_state["env_name"],
            },
            delay=rng.uniform(0.01, 0.05),
        )

    def _handle_validate_bridge(_op_id: OpId, _req: ValidateBridge, _sim: Any) -> SimOutcome:
        delay = rng.uniform(0.01, 0.04)
        if not bridge_state["started"]:
            return SimOutcome(
                error=RuntimeError("Claude SDK bridge is not ready in this backend; restart the backend."),
                delay=delay,
            )
        return SimOutcome(
            value={
                "endpoint": bridge_state["endpoint"],
                "env_name": bridge_state["env_name"],
            },
            delay=delay,
        )

    import sys as _sys

    sim.register_handler(StartBridge, _handle_start_bridge)
    sim.register_handler(ValidateBridge, _handle_validate_bridge)
    # Also bind classes from the latest loaded module in sys.modules in case
    # the plugin file was re-imported on a restore/reload phase.
    live_mod = _sys.modules.get(__name__)
    for req_name, handler in (("StartBridge", _handle_start_bridge), ("ValidateBridge", _handle_validate_bridge)):
        cls = getattr(live_mod, req_name, None)
        if isinstance(cls, type):
            sim.register_handler(cls, handler)


def check_invariants(session: Any) -> None:
    """Verify structural and ReadinessGate invariants on ``BridgeRuntimeState``."""
    state = session.state
    rt = state.runtime(RUNTIME_KEY) if callable(getattr(state, "runtime", None)) else None
    if not (isinstance(rt, BridgeRuntimeState) or type(rt).__name__ == "BridgeRuntimeState"):
        return
    readiness = getattr(state, "readiness", None)
    holds = getattr(readiness, "holds", None) or {}
    if rt.pending_op is not None:
        assert isinstance(rt.pending_op, str) and rt.pending_op
        assert READINESS_KEY in holds, (
            f"pending bridge op {rt.pending_op!r} missing readiness hold {READINESS_KEY!r}"
        )
    else:
        assert READINESS_KEY not in holds, (
            f"dangling readiness hold {READINESS_KEY!r} with no pending bridge op"
        )
        assert not rt.waiting_before_llm, (
            "waiting_before_llm=True with no pending bridge op"
        )
    if rt.bound:
        assert rt.endpoint.startswith("http://127.0.0.1:"), (
            f"bound bridge has unexpected endpoint: {rt.endpoint!r}"
        )
        assert bool(rt.env_name), "bound bridge missing env_name"
        assert bool(rt.session_id), "bound bridge missing session_id"
