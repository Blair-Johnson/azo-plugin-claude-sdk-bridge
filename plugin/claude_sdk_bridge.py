"""Backend-owned, request-lazy Claude SDK bridge (common userspace feature)."""

import json
import os
from pathlib import Path
import secrets
import select
import shutil
import signal
import subprocess
import time

from agent_utils import Feature
from agent_utils.components import HarnessEventHandler


STATE_ATTR = "claude_sdk_bridge_service"
STARTUP_TIMEOUT = 10.0
SHUTDOWN_TIMEOUT = 1.0
GRACEFUL_SHUTDOWN_TIMEOUT = 6.0
_AUTH_HEADERS = {"authorization", "proxy-authorization", "x-api-key", "x-azo-session-id", "host"}


class BridgeService:
    """Ephemeral ownership: never recover a process or credential from a save."""

    def __init__(self, root=None):
        self.root = Path(root) if root is not None else Path(__file__).resolve().parents[1]
        self.process = None
        self.owner_pid = None
        self.endpoint = ""
        self.env_name = ""
        self._key = ""
        self._closed = False

    def save_state(self):
        return {"version": 1}

    def restore_state(self, saved):
        # Initialization supplies this backend's SAME owner. Saved PID/port/key
        # fields, including those in old or foreign snapshots, are not trusted.
        pass

    def _command(self):
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

    def start(self):
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
            self.process.stdin.write((json.dumps({"version": 1, "key": self._key, "port": 0, "ownedProcessGroup": True}) + "\n").encode())
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

    def _read_ready(self):
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

    def validate(self):
        if (self._closed or self.owner_pid != os.getpid() or self.process is None
                or self.process.poll() is not None or not self.endpoint
                or not self._key or os.environ.get(self.env_name) != self._key):
            raise RuntimeError("Claude SDK bridge is not ready in this backend; restart the backend.")

    def _signal_group(self, sig):
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

    def cleanup(self):
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


def _clean_headers(headers):
    return {key: value for key, value in (headers or {}).items() if key.lower() not in _AUTH_HEADERS}


class ManagedBridge(HarnessEventHandler):
    event_kinds = {"runtime.session_ready"}
    writes = {STATE_ATTR}
    optional_reads = {STATE_ATTR, "_session_id"}

    def __init__(self, session, config, profiles):
        self.session = session
        self.config = config
        self.profiles = profiles
        self.service = BridgeService()
        self.session_id = ""
        self.init = {STATE_ATTR: lambda: self.service}

    def _clients(self):
        # enabled_llm_models returns copies. Always edit the original registry:
        # the runtime and supervisor share these nested dictionaries even when
        # they shallow-copy the root configuration before a worker is spawned.
        models = self.config["llm"]["models"]
        for name in self.profiles:
            provider = models[name].setdefault("anthropic", {})
            yield provider, provider.setdefault("client", {})

    def bind_session(self, session_id, *, project_name, db):
        # Bind BEFORE allocating anything, including on initial/partial startup
        # and when /new or restore replaces the entire State object.
        setattr(self.session.state, STATE_ATTR, self.service)
        if not session_id:
            raise RuntimeError("Claude SDK bridge requires a bound session identity.")
        self.service.start()
        for provider, client in self._clients():
            for key in ("api_key", "base_url_env", "auth_token_env", "default_headers_env"):
                client.pop(key, None)
            client.update(base_url=self.service.endpoint, api_key_env=self.service.env_name,
                          auth_token="local-bridge-not-anthropic")
            headers = _clean_headers(client.get("default_headers"))
            headers["x-azo-session-id"] = "{session_id}"
            client["default_headers"] = headers
            request = provider.get("request", {})
            request.pop("extra_headers_env", None)
            if "extra_headers" in request:
                request["extra_headers"] = _clean_headers(request["extra_headers"])
        self.session_id = str(session_id)

    def handle_harness_event(self, event, state):
        if (state is not self.session.state or getattr(state, STATE_ATTR, None) is not self.service
                or not self.session_id or getattr(state, "_session_id", "") != self.session_id
                or event.payload.get("session_id") != self.session_id):
            raise RuntimeError("Claude SDK bridge runtime.session_ready arrived without a matching bind.")
        self.service.validate()
        for provider, client in self._clients():
            headers = client.get("default_headers", {})
            expected_headers = {**_clean_headers(headers), "x-azo-session-id": "{session_id}"}
            request = provider.get("request", {})
            if (client.get("base_url") != self.service.endpoint
                    or client.get("api_key_env") != self.service.env_name
                    or client.get("auth_token") != "local-bridge-not-anthropic"
                    or any(key in client for key in ("api_key", "base_url_env", "auth_token_env", "default_headers_env"))
                    or headers != expected_headers or "extra_headers_env" in request
                    or request.get("extra_headers", {}) != _clean_headers(request.get("extra_headers"))):
                raise RuntimeError("Claude SDK bridge runtime configuration changed after binding.")


def register_features(builder, *, session, config):
    from agents.llm import enabled_llm_models

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
        builder.add(Feature(name="claude_sdk_bridge", components=[ManagedBridge(session, config, profiles)],
                            order=[HarnessEventHandler]))
