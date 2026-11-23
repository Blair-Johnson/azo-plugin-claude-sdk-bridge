"""Opt-in native Runtime steering benchmark (not a frontend/UI test).

Run in the repository Python environment. No user config is loaded. The parent
owns a loopback bridge/fake API and appends JSONL to --events-file. After sending
its first upstream text delta, while that response is still open, append:
  {"event":"provider_started","session_id":"runtime-steering","request_index":1}
Use a fresh events file and unique session id for each run. The matching event
is required; a submit timestamp alone is NOT proof of an in-flight provider.
The fixture must honor x-azo-session-id and return standard Anthropic SSE.

--offline-fixture asserts the parent serves a synthetic fake API, NOT a live SDK.
--live explicitly authorizes this program to call the parent's live bridge.
Both modes restrict this Python process and its native worker to loopback; only
an explicitly live parent bridge may contact Claude. The key is read only from
AZO_CLAUDE_BRIDGE_KEY, never from argv. Output contains no response/prompt text.

This uses production Runtime, UserInputHandler, LLMCaller, FrontendDriver,
InteractiveSessionIO.send_user_text, and SubprocessLLMDriver with the native
Anthropic provider worker. The subclass only observes real calls; it does not
replace the worker, cancellation, or provider adapter.
"""
from __future__ import annotations

import argparse
import contextlib
import io
import ipaddress
import json
import logging
import os
from pathlib import Path
import sys
import threading
import time
from urllib.parse import urlsplit

from agent_zoo.drivers.llm import SubprocessLLMDriver

STALE = "AZO_RUNTIME_STALE"
STEERED = "AZO_RUNTIME_STEERED"
FIRST_PROMPT = (
    f"Synthetic benchmark. Start your answer with {STALE}. "
    "Then produce approximately 512 tokens of numbered synthetic test words. "
    "Do not use tools. Continue until all 128 numbered items are written. "
    "Synthetic input padding follows: " + "test " * 420
)
STEERING_PROMPT = (
    f"Replace the preceding task. Reply with exactly {STEERED} and nothing else. "
    "Do not continue the numbered list. Do not use tools."
)


def loopback_url(value: str) -> str:
    parsed = urlsplit(value)
    try:
        loopback = ipaddress.ip_address(parsed.hostname or "").is_loopback
    except ValueError:
        loopback = False
    if (parsed.scheme != "http" or not loopback or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
        raise ValueError("base_url_must_be_literal_loopback_http_origin")
    return value.rstrip("/")


def _loopback_network_only(event, args):
    if event == "socket.connect":
        address = args[1]
        if isinstance(address, tuple):
            try:
                allowed = ipaddress.ip_address(address[0]).is_loopback
            except ValueError:
                allowed = False
            if not allowed:
                raise PermissionError("benchmark_non_loopback_connect_blocked")


class MeasuredLLMDriver(SubprocessLLMDriver):
    """Observation only: each overridden operation delegates to production."""

    def __init__(self, state_ref):
        super().__init__("*", "anthropic", context="spawn")
        self._state_ref = state_ref
        self.submissions = []
        self.cancellations = []
        self._done_ops = set()

    def submit(self, op_id, req):
        started = time.monotonic()
        super().submit(op_id, req)
        self.submissions.append({
            "op_id": op_id,
            "at": started,
            "returned": time.monotonic(),
            "process": self._proc,
            "steering_seen": any(
                message.get("role") == "user"
                and STEERING_PROMPT in str(message.get("content", ""))
                for message in getattr(req, "messages", ())
            ),
        })

    def cancel(self, op_id):
        process = self._proc
        started = time.monotonic()
        was_active = self._active is not None and self._active.op_id == op_id
        super().cancel(op_id)
        finished = time.monotonic()
        state = self._state_ref()
        normal_preempt = bool(
            was_active
            and state is not None
            and not getattr(state, "interrupted", False)
            and not getattr(state, "shutdown", False)
        )
        self.cancellations.append({
            "op_id": op_id,
            "at": started,
            "finished": finished,
            "normal_user_preemption": normal_preempt,
            "cancelled": was_active,
            "worker_exited": process is not None and not process.is_alive(),
        })
        if was_active:
            self._done_ops.add(op_id)


def make_config(base_url, model, timeout):
    return {"llm": {"default": "runtime-steering", "models": {
        "runtime-steering": {
            "context_window_tokens": 200000,
            "harness": {"disable_mid_conversation_system_messages": True},
            "anthropic": {
                "client": {
                    "base_url": base_url, "api_key_env": "AZO_CLAUDE_BRIDGE_KEY",
                    "auth_token": "local-bridge-not-anthropic", "max_retries": 0,
                    "timeout": timeout,
                    "default_headers": {"x-azo-session-id": "{session_id}"},
                },
                "request": {"model": model, "max_tokens": 2048, "stream": True},
            },
        },
    }}}


def run_benchmark(args):
    from agent_utils.core.events import IoCompleted, IoFailed, RenderRequested
    from agent_utils.core.pipeline import PipelineBuilder
    from agent_utils.core.session import Session
    from agent_utils.features.core import core_feature
    from agent_utils.runtime import Runtime, TimerDriver
    from agent_zoo.drivers.frontend import FrontendDriver
    from agent_zoo.events import SessionReady, UserMessage
    from agent_zoo.features.turn import turn_orchestration_feature
    from agent_zoo.transport.interactive import InteractiveSessionIO
    from agent_zoo.transport.session_io import SessionIO

    base_url = loopback_url(args.base_url)
    if not os.environ.get("AZO_CLAUDE_BRIDGE_KEY"):
        raise ValueError("missing_local_bridge_key")
    config = make_config(base_url, args.model, args.timeout)
    started = time.monotonic()
    timings = {}
    errors = []
    stop = threading.Event()
    session_io = SessionIO(session_id=args.session_id)
    interactive = InteractiveSessionIO(session_io)
    thread = None

    session = Session(system_prompt="Follow the latest user instruction. Synthetic benchmark only.")
    session.state._session_id = args.session_id
    session.state._instance_id = "runtime-steering-inst"
    session.pipeline = (
        PipelineBuilder()
        .add(core_feature())
        .add(turn_orchestration_feature(config))
        .build()
    )
    driver = MeasuredLLMDriver(lambda: session.state)
    frontend = FrontendDriver(session_io, session_id=args.session_id, start_ws_server=False)
    applied_answers = []
    runtime_holder = {}

    def on_step(event, _effects):
        if isinstance(event, IoCompleted) and event.op_id in {s["op_id"] for s in driver.submissions}:
            driver._done_ops.add(event.op_id)
            applied_answers.append(event.op_id)
            if not session.state.pending_tool_calls and session.state.active_llm is None:
                runtime_holder["runtime"].stop()
        elif isinstance(event, IoFailed) and event.op_id in {s["op_id"] for s in driver.submissions}:
            driver._done_ops.add(event.op_id)
            errors.append(type(event.error).__name__ if isinstance(event.error, BaseException) else "IoFailed")
            runtime_holder["runtime"].stop()
        elif getattr(session.state, "shutdown", False) or getattr(session.state, "done", False):
            runtime_holder["runtime"].stop()

    runtime = Runtime(
        session,
        drivers=[TimerDriver(), frontend, driver],
        on_step=on_step,
    )
    runtime_holder["runtime"] = runtime

    def steer_then_watchdog():
        deadline = started + args.timeout
        offset = 0
        pending = ""
        try:
            while not stop.is_set() and time.monotonic() < deadline:
                try:
                    with args.events_file.open(encoding="utf-8") as events:
                        events.seek(offset)
                        pending += events.read()
                        offset = events.tell()
                except FileNotFoundError:
                    pass
                lines = pending.split("\n")
                pending = lines.pop()
                signal_seen = False
                for line in lines:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    signal_seen |= (
                        event.get("event") == "provider_started"
                        and event.get("session_id") == args.session_id
                        and event.get("request_index") == 1
                    )
                if signal_seen:
                    timings["provider_signal_observed"] = time.monotonic()
                    if stop.wait(args.steer_delay_ms / 1000):
                        return
                    if (len(driver.submissions) != 1
                            or driver.submissions[0]["op_id"] in driver._done_ops):
                        errors.append("first_inference_not_pending_at_injection")
                        break
                    timings["user_text_sent"] = time.monotonic()
                    interactive.send_user_text(STEERING_PROMPT)
                    break
                stop.wait(0.01)
            if stop.wait(max(0, deadline - time.monotonic())):
                return
            errors.append("benchmark_timeout")
        except Exception as error:
            errors.append(type(error).__name__)
        # Failure-only cleanup, not a success-path interrupt or steering aid.
        interactive.send_control("shutdown")
        runtime.stop()

    try:
        runtime.start()
        runtime.post(SessionReady(session_id=args.session_id, instance_id="runtime-steering-inst"))
        runtime.post(RenderRequested())
        runtime.post(UserMessage(text=FIRST_PROMPT))
        thread = threading.Thread(target=steer_then_watchdog, daemon=True)
        thread.start()
        runtime.run()
        timings["runtime_finished"] = time.monotonic()
        stop.set()
        thread.join(timeout=5)
        session.render()
        assistants = [m for m in session.messages if m.get("role") == "assistant"]
        replies = [m.get("content", "") for m in assistants]
        cancellation = driver.cancellations[0] if driver.cancellations else None
        first_op = driver.submissions[0]["op_id"] if driver.submissions else None
        cancelled_future = bool(
            first_op is not None
            and any(c["op_id"] == first_op and c["cancelled"] for c in driver.cancellations)
        )
        applied_count = len(applied_answers)
        checks = {
            "provider_signal_received": "provider_signal_observed" in timings,
            "normal_user_text_sent": "user_text_sent" in timings,
            "exactly_two_native_submissions": len(driver.submissions) == 2,
            "replacement_saw_user_instruction": (
                len(driver.submissions) == 2
                and driver.submissions[1]["steering_seen"]
            ),
            "first_future_cancelled": cancelled_future,
            "normal_user_preemption": bool(cancellation and cancellation["normal_user_preemption"]),
            "first_worker_exited": bool(cancellation and cancellation["worker_exited"]),
            "worker_replaced": (
                len(driver.submissions) == 2
                and driver.submissions[0]["process"] is not driver.submissions[1]["process"]
            ),
            "only_steered_answer_applied": applied_count == 1 and replies == [STEERED],
            "stale_answer_absent": all(STALE not in str(reply) for reply in replies),
            "no_manual_interrupt": not getattr(session.state, "interrupted", False),
            "no_provider_error": not errors and not getattr(session.state, "last_provider_error", ""),
            "watchdog_not_used": not getattr(session.state, "shutdown", False),
        }
        metrics = {"total_ms": (timings["runtime_finished"] - started) * 1000}
        if "user_text_sent" in timings:
            injected = timings["user_text_sent"]
            metrics["steer_to_final_ms"] = (timings["runtime_finished"] - injected) * 1000
            if cancellation:
                metrics["steer_to_cancel_started_ms"] = (cancellation["at"] - injected) * 1000
                metrics["steer_to_cancel_returned_ms"] = (cancellation["finished"] - injected) * 1000
                metrics["cancel_duration_ms"] = (cancellation["finished"] - cancellation["at"]) * 1000
            if len(driver.submissions) == 2:
                metrics["steer_to_submit_started_ms"] = (driver.submissions[1]["at"] - injected) * 1000
                metrics["steer_to_submit_returned_ms"] = (driver.submissions[1]["returned"] - injected) * 1000
        checks["cancellation_bounded"] = (
            0 <= metrics.get("steer_to_cancel_returned_ms", float("inf")) <= args.cancel_budget_ms
        )
        result = {
            "kind": "runtime_steering", "schema_version": 1,
            "config": {"model": args.model, "max_tokens": 2048,
                       "thinking": "sdk_default", "system_prompt_mode": "parent_bridge_config"},
            "mode": "live" if args.live else "offline_fixture",
            "scope": "native_runtime_session_io_not_frontend",
            "ok": all(checks.values()), "checks": checks,
            "metrics": {key: round(value, 3) for key, value in metrics.items()},
            "counts": {"submissions": len(driver.submissions),
                       "applied_answers": applied_count,
                       "cancellations": len(driver.cancellations)},
            "error_types": errors,
        }
    finally:
        stop.set()
        runtime.close()
        if thread:
            thread.join(timeout=5)
    checks["all_native_workers_exited"] = all(
        submission["process"] is None or not submission["process"].is_alive()
        for submission in driver.submissions
    )
    checks["watchdog_thread_exited"] = thread is not None and not thread.is_alive()
    result["ok"] = all(checks.values())
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--live", action="store_true")
    mode.add_argument("--offline-fixture", action="store_true")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--events-file", type=Path, required=True)
    parser.add_argument("--session-id", default="runtime-steering")
    parser.add_argument("--model", default="sonnet")
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--steer-delay-ms", type=float, default=0)
    parser.add_argument("--cancel-budget-ms", type=float, default=5000)
    args = parser.parse_args(argv)
    if args.timeout <= 0 or args.steer_delay_ms < 0 or args.cancel_budget_ms <= 0:
        parser.error("timeouts/budgets must be positive; steering delay must be nonnegative")
    # Never inherit external HTTP proxies or real Anthropic credentials.
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy",
                 "all_proxy", "ANTHROPIC_API_KEY", "ANTHROPIC_AUTH_TOKEN", "ANTHROPIC_CUSTOM_HEADERS"):
        os.environ.pop(name, None)
    os.environ["NO_PROXY"] = "127.0.0.1,::1,localhost"
    logging.disable(logging.CRITICAL)
    try:
        # Suppress incidental pipeline output. Only the structured result is public.
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            result = run_benchmark(args)
    except Exception as error:
        result = {"kind": "runtime_steering", "schema_version": 1, "ok": False,
                  "error_types": [type(error).__name__]}
    print(json.dumps(result, sort_keys=True), flush=True)
    return 0 if result["ok"] else 1


if __name__ in ("__main__", "__mp_main__"):
    sys.addaudithook(_loopback_network_only)
    logging.disable(logging.CRITICAL)
if __name__ == "__main__":
    raise SystemExit(main())
