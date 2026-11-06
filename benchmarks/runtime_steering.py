"""Opt-in native AgentRuntime steering benchmark (not a frontend/UI test).

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

This uses production AgentRuntime.run_llm_loop / poll_during_llm,
InteractiveSessionIO.send_user_text, LLMProcessSupervisor and its default
_worker_main -> agents.llm.make_llm_fn. The subclass only observes real calls;
it does not replace the worker, futures, cancellation, or provider adapter.
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
from queue import Queue
import sys
import threading
import time
from urllib.parse import urlsplit

# Run with the selected Agent Zoo environment; its installed package also
# supplies imports to multiprocessing workers, independently of this repo's path.

from agents.llm_subprocess import LLMCancelledError, LLMProcessSupervisor

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


class MeasuredSupervisor(LLMProcessSupervisor):
    """Observation only: each overridden operation delegates to production."""

    def __init__(self, config):
        super().__init__(config)  # Default native worker and spawn context.
        self.submissions = []
        self.cancellations = []

    def submit(self, messages, tools, request_context=None, *, kind="chat"):
        started = time.monotonic()
        call = super().submit(messages, tools, request_context, kind=kind)
        self.submissions.append({
            "at": started, "returned": time.monotonic(), "call": call, "process": self._proc,
            "steering_seen": any(
                message.get("role") == "user"
                and STEERING_PROMPT in str(message.get("content", ""))
                for message in messages
            ),
        })
        return call

    def cancel_active(self, reason="cancelled"):
        process = self._proc
        started = time.monotonic()
        result = super().cancel_active(reason)
        self.cancellations.append({
            "at": started, "finished": time.monotonic(),
            "normal_user_preemption": reason == "user message preempted pending inference",
            "cancelled": result,
            "worker_exited": process is not None and not process.is_alive(),
        })
        return result


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
    from agent_utils import PipelineBuilder, Session
    from agent_utils.builder import core
    from agent_zoo.interactive_session import InteractiveSessionIO
    from agent_zoo.runtime.backend import AgentRuntime, BackendInputController
    from agent_zoo.runtime.models import Pace
    from agent_zoo.session_io import SessionIO

    base_url = loopback_url(args.base_url)
    if not os.environ.get("AZO_CLAUDE_BRIDGE_KEY"):
        raise ValueError("missing_local_bridge_key")
    config = make_config(base_url, args.model, args.timeout)
    supervisor = MeasuredSupervisor(config)
    started = time.monotonic()
    timings = {}
    errors = []
    stop = threading.Event()
    input_queue = Queue()
    session_io = SessionIO(session_id=args.session_id)
    session_io.set_inbox_wake_callback(lambda: input_queue.put(""))
    interactive = InteractiveSessionIO(session_io)
    thread = None
    with Session(system_prompt="Follow the latest user instruction. Synthetic benchmark only.") as session:
        session.pipeline = PipelineBuilder(session).add(core()).build()
        session.run({"role": "user", "content": FIRST_PROMPT})
        pump = BackendInputController(
            session=session, input_queue=input_queue, repl_history=None,
            handle_slash=lambda _raw: None, command_sink=lambda _value: None,
            emit_info=lambda _text: None, model_display="runtime-steering",
            max_idle=0, sys_role="system", initial_pace=Pace.INTERACTIVE,
        )
        backend = AgentRuntime(
            session=session, event_pump=pump, config=config,
            session_id=args.session_id, project_name="synthetic-benchmark",
            llm_supervisor=supervisor, session_io=session_io,
            report_llm_error=lambda error: errors.append(type(error).__name__),
            has_pending_interrupt=lambda _session: False,
        )

        def steer_then_watchdog():
            deadline = started + args.timeout
            # Open only after startup: parent must use a fresh file, but may have
            # already created it. Read complete lines only, including partial writes.
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
                    signal = False
                    for line in lines:
                        if not line.strip():
                            continue
                        event = json.loads(line)
                        signal |= (event.get("event") == "provider_started"
                                   and event.get("session_id") == args.session_id
                                   and event.get("request_index") == 1)
                    if signal:
                        timings["provider_signal_observed"] = time.monotonic()
                        if stop.wait(args.steer_delay_ms / 1000):
                            return
                        if (len(supervisor.submissions) != 1
                                or supervisor.submissions[0]["call"].done()):
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
            supervisor.shutdown(wait=True)

        try:
            thread = threading.Thread(target=steer_then_watchdog, daemon=True)
            thread.start()
            applied_count = backend.run_llm_loop()
            timings["runtime_finished"] = time.monotonic()
            stop.set()
            thread.join(timeout=5)
            assistants = [m for m in session.messages if m.get("role") == "assistant"]
            replies = [m.get("content", "") for m in assistants]
            cancellation = supervisor.cancellations[0] if supervisor.cancellations else None
            cancelled_future = False
            if supervisor.submissions and supervisor.submissions[0]["call"].done():
                try:
                    supervisor.submissions[0]["call"].result(timeout=0)
                except LLMCancelledError:
                    cancelled_future = True
                except Exception:
                    pass
            checks = {
                "provider_signal_received": "provider_signal_observed" in timings,
                "normal_user_text_sent": "user_text_sent" in timings,
                "exactly_two_native_submissions": len(supervisor.submissions) == 2,
                "replacement_saw_user_instruction": (
                    len(supervisor.submissions) == 2
                    and supervisor.submissions[1]["steering_seen"]),
                "first_future_cancelled": cancelled_future,
                "normal_user_preemption": bool(cancellation and cancellation["normal_user_preemption"]),
                "first_worker_exited": bool(cancellation and cancellation["worker_exited"]),
                "worker_replaced": (len(supervisor.submissions) == 2
                                    and supervisor.submissions[0]["process"] is not
                                    supervisor.submissions[1]["process"]),
                "only_steered_answer_applied": applied_count == 1 and replies == [STEERED],
                "stale_answer_absent": all(STALE not in str(reply) for reply in replies),
                "no_manual_interrupt": not backend.manual_interrupt_requested,
                "no_provider_error": not errors and not backend.state.last_provider_error,
                "watchdog_not_used": not backend.shutdown,
            }
            metrics = {"total_ms": (timings["runtime_finished"] - started) * 1000}
            if "user_text_sent" in timings:
                injected = timings["user_text_sent"]
                metrics["steer_to_final_ms"] = (timings["runtime_finished"] - injected) * 1000
                if cancellation:
                    metrics["steer_to_cancel_started_ms"] = (cancellation["at"] - injected) * 1000
                    metrics["steer_to_cancel_returned_ms"] = (cancellation["finished"] - injected) * 1000
                    metrics["cancel_duration_ms"] = (cancellation["finished"] - cancellation["at"]) * 1000
                if len(supervisor.submissions) == 2:
                    metrics["steer_to_submit_started_ms"] = (supervisor.submissions[1]["at"] - injected) * 1000
                    metrics["steer_to_submit_returned_ms"] = (supervisor.submissions[1]["returned"] - injected) * 1000
            checks["cancellation_bounded"] = (
                0 <= metrics.get("steer_to_cancel_returned_ms", float("inf")) <= args.cancel_budget_ms)
            result = {
                "kind": "runtime_steering", "schema_version": 1,
                "config": {"model": args.model, "max_tokens": 2048,
                           "thinking": "sdk_default", "system_prompt_mode": "parent_bridge_config"},
                "mode": "live" if args.live else "offline_fixture",
                "scope": "native_runtime_session_io_not_frontend",
                "ok": all(checks.values()), "checks": checks,
                "metrics": {key: round(value, 3) for key, value in metrics.items()},
                "counts": {"submissions": len(supervisor.submissions),
                           "applied_answers": applied_count,
                           "cancellations": len(supervisor.cancellations)},
                "error_types": errors,
            }
        finally:
            stop.set()
            supervisor.shutdown(wait=True)
            if thread:
                thread.join(timeout=5)
        checks["all_native_workers_exited"] = all(
            not submission["process"].is_alive() for submission in supervisor.submissions)
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
