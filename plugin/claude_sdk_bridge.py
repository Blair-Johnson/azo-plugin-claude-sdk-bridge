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
import time
from typing import Any

from agent_utils.core.events import BeforeLLM, Event, IoCompleted, IoFailed
from agent_utils.core.io import IoRequest, OpId
from agent_utils.core.pipeline import EmitFn, Feature
from agent_utils.core.state import State
from agent_utils.features.core import IoFence
from agent_utils.runtime.driver import Driver
from agent_zoo.events import SessionReady
from agent_zoo.features.turn.llm_caller import LLMCaller


STATE_ATTR = "claude_sdk_bridge_service"
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


class BridgeService:
    """Ephemeral ownership: never recover a process or credential from a save."""

    def __init__(self, root: str | Path | None = None) -> None:
        self.root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
        self.process: subprocess.Popen[bytes] | None = None
        self.owner_pid: int | None = None
        self.endpoint = ""
        self.env_name = ""
        self._key = ""
        self._closed = False

    def save_state(self) -> dict[str, int]:
        return {"version": 1}

    def restore_state(self, saved: Any) -> None:
        # Initialization supplies this backend's SAME owner. Saved PID/port/key
        # fields, including those in old or foreign snapshots, are not trusted.
        del saved

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

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("Claude SDK bridge owner is already closed.")
        if self.process is not None:
            self.validate()
            return
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
        except BaseException:
            self.cleanup()
            raise

    def _read_ready(self) -> Any:
        assert self.process is not None and self.process.stdout is not None
        deadline = time.monotonic() + STARTUP_TIMEOUT
        data = bytearray()
        fd = self.process.stdout.fileno()
        while b"\n" not in data:
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not select.select([fd], [], [], max(0, remaining))[0]:
                raise RuntimeError("Claude SDK bridge startup timed out.")
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

    def validate(self) -> None:
        if (self._closed or self.owner_pid != os.getpid() or self.process is None
                or self.process.poll() is not None or not self.endpoint
                or not self._key or os.environ.get(self.env_name) != self._key):
            raise RuntimeError("Claude SDK bridge is not ready in this backend; restart the backend.")

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


class BridgeDriver(Driver):
    """Runtime IO driver owning the ``BridgeService`` subprocess lifecycle."""

    request_types = (StartBridge, ValidateBridge)

    def __init__(self, service: BridgeService) -> None:
        super().__init__()
        self.service = service

    def submit(self, op_id: OpId, req: IoRequest) -> None:
        try:
            if isinstance(req, StartBridge):
                self.service.start()
            elif isinstance(req, ValidateBridge):
                self.service.validate()
            else:
                raise TypeError(f"Unsupported bridge request: {type(req).__name__}")
        except Exception as exc:
            self._safe_emit(IoFailed(op_id=op_id, error=exc))
            return
        self._safe_emit(
            IoCompleted(
                op_id=op_id,
                value={
                    "endpoint": self.service.endpoint,
                    "env_name": self.service.env_name,
                },
            )
        )

    def close(self) -> None:
        self.service.cleanup()


def _clean_headers(headers: dict[str, Any] | None) -> dict[str, Any]:
    return {key: value for key, value in (headers or {}).items() if key.lower() not in _AUTH_HEADERS}


class ManagedBridge:
    """Pure Sans-I/O pipeline component binding bridge-tagged LLM profiles."""

    handles = (SessionReady, BeforeLLM, IoCompleted, IoFailed)
    writes = {STATE_ATTR}
    optional_reads = {STATE_ATTR, "_session_id", "llm_gates"}

    def __init__(
        self,
        session: Any,
        config: dict[str, Any],
        profiles: list[str],
        *,
        service: BridgeService | None = None,
    ) -> None:
        self.session = session
        self.config = config
        self.profiles = list(profiles)
        self.service = service if service is not None else BridgeService()
        self.session_id = ""
        self.endpoint = ""
        self.env_name = ""
        self._pending_op: OpId | None = None
        self._waiting_before_llm = False
        self._bound = False
        self._last_error: BaseException | None = None
        self.init = {STATE_ATTR: lambda: self.service}

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

    def verify_config(self) -> None:
        for provider, client in self._clients():
            headers = client.get("default_headers", {})
            expected_headers = {**_clean_headers(headers), "x-azo-session-id": "{session_id}"}
            request = provider.get("request", {})
            if (client.get("base_url") != self.endpoint
                    or client.get("api_key_env") != self.env_name
                    or client.get("auth_token") != "local-bridge-not-anthropic"
                    or any(key in client for key in ("api_key", "base_url_env", "auth_token_env", "default_headers_env"))
                    or headers != expected_headers or "extra_headers_env" in request
                    or request.get("extra_headers", {}) != _clean_headers(request.get("extra_headers"))):
                raise RuntimeError("Claude SDK bridge runtime configuration changed after binding.")

    @staticmethod
    def _add_gate(state: State, op_id: OpId) -> None:
        gates = getattr(state, "llm_gates", None)
        if isinstance(gates, set):
            gates.add(op_id)
        else:
            state.llm_gates = {op_id}

    @staticmethod
    def _clear_gate(state: State, op_id: OpId) -> None:
        gates = getattr(state, "llm_gates", None)
        if isinstance(gates, set):
            gates.discard(op_id)

    def handle(self, event: Event, state: State, emit: EmitFn) -> None:
        if isinstance(event, SessionReady):
            self._handle_session_ready(event, state)
        elif isinstance(event, IoCompleted):
            self._handle_io_completed(event, state, emit)
        elif isinstance(event, IoFailed):
            self._handle_io_failed(event, state)
        elif isinstance(event, BeforeLLM):
            self._handle_before_llm(event, state)

    def _handle_session_ready(self, event: SessionReady, state: State) -> None:
        # Always bind the process-local owner onto State before submitting work,
        # including after state replacement or checkpoint restoration.
        setattr(state, STATE_ATTR, self.service)
        state_sid = str(getattr(state, "_session_id", "") or "").strip()
        event_sid = str(event.session_id or "").strip()
        session_id = event_sid or state_sid
        if not session_id:
            raise RuntimeError("Claude SDK bridge requires a bound session identity.")
        if event_sid and state_sid and event_sid != state_sid:
            raise RuntimeError("Claude SDK bridge SessionReady arrived with mismatched session identity.")
        if not state_sid:
            state._session_id = session_id

        if self._bound:
            self.verify_config()

        if self._pending_op is not None:
            self._clear_gate(state, self._pending_op)
            state.io.cancel(self._pending_op)
            self._pending_op = None

        self.session_id = session_id
        self._last_error = None
        op_id = state.io.submit(StartBridge(session_id=session_id), tag="claude_sdk_bridge")
        self._pending_op = op_id
        self._add_gate(state, op_id)

    def _handle_io_completed(self, event: IoCompleted, state: State, emit: EmitFn) -> None:
        if self._pending_op is None or event.op_id != self._pending_op:
            return
        self._pending_op = None
        self._clear_gate(state, event.op_id)

        payload = event.value if isinstance(event.value, dict) else {}
        endpoint = str(payload.get("endpoint") or "")
        env_name = str(payload.get("env_name") or "")
        if not endpoint or not env_name:
            err = RuntimeError("Claude SDK bridge driver returned an incomplete binding.")
            self._last_error = err
            raise err

        if not self._bound:
            self._apply_binding(endpoint, env_name)
            self.endpoint = endpoint
            self.env_name = env_name
            self._bound = True
        else:
            if (endpoint, env_name) != (self.endpoint, self.env_name):
                self._apply_binding(endpoint, env_name)
                self.endpoint = endpoint
                self.env_name = env_name
            self.verify_config()

        if self._waiting_before_llm:
            self._waiting_before_llm = False
            if not getattr(state, "llm_gates", None):
                emit(BeforeLLM())

    def _handle_io_failed(self, event: IoFailed, state: State) -> None:
        if self._pending_op is None or event.op_id != self._pending_op:
            return
        self._pending_op = None
        self._clear_gate(state, event.op_id)
        self._bound = False
        self._waiting_before_llm = False
        err = event.error if isinstance(event.error, BaseException) else RuntimeError(str(event.error))
        self._last_error = err
        raise err

    def _handle_before_llm(self, event: BeforeLLM, state: State) -> None:
        if self._pending_op is not None:
            self._waiting_before_llm = True
            event.stop()
            return
        if self._last_error is not None:
            raise self._last_error
        if (state is not self.session.state
                or getattr(state, STATE_ATTR, None) is not self.service
                or not self._bound
                or not self.session_id
                or getattr(state, "_session_id", "") != self.session_id):
            raise RuntimeError("Claude SDK bridge BeforeLLM arrived without a matching bind.")
        self.verify_config()


def register_features(builder, *, session, config):
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
        service = BridgeService()
        bridge = ManagedBridge(session, config, profiles, service=service)
        driver = BridgeDriver(service)
        builder.add(
            Feature(
                name="claude_sdk_bridge",
                components=[bridge],
                order=[IoFence, ManagedBridge, LLMCaller],
                drivers=[driver],
            )
        )
