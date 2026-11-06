"""Opt-in installed-backend smoke: automatic bridge ownership, models, cleanup."""
import argparse
import asyncio
import json
import os
import re
from pathlib import Path
import subprocess
import sys
import time

from agent_zoo.wsctl import AzoWs


def processes():
    output = subprocess.check_output(["ps", "-axo", "pid=,ppid=,command="], text=True)
    return {int(parts[0]): (int(parts[1]), parts[2]) for line in output.splitlines()
            if len(parts := line.strip().split(None, 2)) == 3}


def descendants(pid):
    rows, found = processes(), set()
    while True:
        new = {child for child, (parent, _) in rows.items() if parent == pid or parent in found}
        if new <= found:
            return {child: rows[child][1] for child in found}
        found |= new


def answered(obs, marker):
    status = obs.state_value("core.status", {})
    if status.get("error_text"):
        raise RuntimeError("Backend reported an error")
    complete = not status.get("spinner", {}).get("active", True)
    return complete and any(item.get("role") == "assistant" and item.get("body", "").strip() == marker
                            for item in obs.transcript_items())


async def run(args):
    from agents import llm
    if Path(llm.__file__).resolve().parent.parent != args.expected_source.resolve():
        raise RuntimeError("Unexpected installed source")
    env = dict(os.environ)
    env.pop("PYTHONPATH", None)
    ws = AzoWs.launch(project="claude-managed-acceptance", model=args.model[0],
                      use_pixi=False, modern=True, extra_args=["--home", str(args.home)], env=env,
                      startup_timeout_s=60, shutdown_timeout_s=15)
    owned, passed, stage = {}, False, "startup"
    try:
        await ws.__aenter__()
        root_pid = ws._process.pid
        owned.update(descendants(root_pid))
        bridges = {pid for pid, command in owned.items() if command.endswith("/src/managed.mjs")}
        assert len(bridges) == 1, "Expected one automatically owned manager"
        print(json.dumps({"kind": "started", "automatic_managers": len(bridges)}), flush=True)
        for index, model in enumerate(args.model):
            stage = "model:" + model
            if index:
                await ws.slash("/model " + model)
                await ws.wait_until(lambda obs: model in obs.state_text("core.status"), timeout=15)
            marker = "MANAGED_CLAUDE_OK_" + str(index)
            before = time.monotonic()
            await ws.send_user("Do not call tools. Reply exactly " + marker + ".")
            await ws.wait_until(lambda obs: answered(obs, marker), timeout=90)
            owned.update(descendants(root_pid))
            assert {pid for pid, command in descendants(root_pid).items()
                    if command.endswith("/src/managed.mjs")} == bridges
            print(json.dumps({"kind": "model_result", "profile": model, "passed": True,
                              "elapsed_s": round(time.monotonic() - before, 3)}), flush=True)
        if args.check_usage:
            stage = "usage"
            status = ws.observation().state_value("core.status", {})
            meter = status.get("token_count_estimate")
            await ws.slash("/usage")
            await ws.wait_until(lambda obs: obs.state_value("core.inspector", {}).get("title") == "Token Usage", timeout=10)
            body = ws.observation().state_value("core.inspector", {}).get("body", "")
            counters = {label: int(value.replace(",", "")) for label, value in
                        re.findall(r"\*\*([^*]+):\*\* ([\d,]+)", body)}
            print(json.dumps({"kind": "usage", "meter": meter,
                              "limit": status.get("token_limit"), "counters": counters,
                              "missing_report": "no provider report" in body,
                              "missing_usage_notice_seen": any("No provider input-usage report" in json.dumps(frame)
                                                               for frame in ws.observation().frames)}), flush=True)
            assert isinstance(meter, int) and meter > 0, "Status meter has no positive input report"
            assert "no provider report" not in body, "Usage inspector has no input report"
        passed = True
    except Exception as error:
        obs = ws.observation()
        status = obs.state_value("core.status", {})
        print(json.dumps({"kind": "failure", "stage": stage, "error_type": type(error).__name__,
                          "status": {key: status.get(key) for key in ("activity", "model_name", "pending_count")},
                          "error": re.sub(r"(?:sk-[\w-]+|[a-zA-Z0-9_-]{40,})", "[redacted]", str(status.get("error_text", "")))[:1800],
                          "items": [{"role": item.get("role"), "body_chars": len(item.get("body", ""))}
                                    for item in obs.transcript_items()[-8:]]}), flush=True)
    finally:
        if not await ws.close():
            await ws.force_close()
            passed = False
        for _ in range(30):
            live = processes()
            remaining = {pid for pid, command in owned.items() if live.get(pid, (None, None))[1] == command}
            if not remaining:
                break
            await asyncio.sleep(0.1)
        passed = passed and not remaining
        print(json.dumps({"kind": "final", "passed": passed, "owned_children_remaining": len(remaining),
                          "credentials_logged": False}), flush=True)
    return passed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live", action="store_true", required=True)
    parser.add_argument("--home", type=Path, required=True)
    parser.add_argument("--expected-source", type=Path, required=True)
    parser.add_argument("--model", action="append", required=True)
    parser.add_argument("--check-usage", action="store_true")
    sys.exit(0 if asyncio.run(run(parser.parse_args())) else 1)
