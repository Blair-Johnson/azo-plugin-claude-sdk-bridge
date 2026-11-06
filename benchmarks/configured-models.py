"""Opt-in smoke test of installed Agent Zoo profiles against a running local bridge."""
import argparse
import copy
import json
from pathlib import Path
import sys
import time
import urllib.request
import uuid

from agent_zoo.config_loader import load_layered_config
from agents import llm

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--live", action="store_true", required=True)
parser.add_argument("--home", type=Path, required=True)
parser.add_argument("--model", action="append", required=True)
parser.add_argument("--expected-source", type=Path, required=True)
args = parser.parse_args()
if not Path(llm.__file__).resolve().is_relative_to(args.expected_source.resolve()):
    parser.error("Installed Agent Zoo imports do not point at --expected-source; check PYTHONPATH")
config = load_layered_config(state_root=args.home)
print(json.dumps({"kind": "config", "implementation": str(Path(llm.__file__).resolve()),
                  "profiles": args.model, "credentials_logged": False}), flush=True)
passed = True
for name in args.model:
    cfg = copy.deepcopy(config)
    cfg["llm"]["default"] = name
    profile = cfg["llm"]["models"][name]
    client = profile["anthropic"]["client"]
    assert client["base_url"].startswith("http://127.0.0.1:"), "Only local bridge profiles are allowed"
    client["timeout"] = 100
    profile["anthropic"]["request"]["max_tokens"] = 1024
    session = "installed-smoke-" + uuid.uuid4().hex
    started = time.monotonic()
    try:
        answer = llm.make_llm_fn(cfg)([{"role": "user", "content": "Reply exactly BRIDGE_READY and nothing else."}],
                                     [], request_context={"session_id": session})
        ok = answer.get("content", "").strip() == "BRIDGE_READY"
        passed &= ok
        print(json.dumps({"kind": "model_result", "profile": name, "passed": ok,
                          "elapsed_s": round(time.monotonic() - started, 3)}), flush=True)
    except Exception as error:
        passed = False
        print(json.dumps({"kind": "model_result", "profile": name, "passed": False,
                          "error_type": type(error).__name__, "status": getattr(error, "status_code", None)}), flush=True)
    finally:
        try:
            request = urllib.request.Request(client["base_url"] + "/v1/session", method="DELETE",
                      headers={"x-api-key": client["api_key"], "x-azo-session-id": session})
            with urllib.request.build_opener(urllib.request.ProxyHandler({})).open(request, timeout=10) as response:
                response.read()
        except Exception:
            passed = False
            print(json.dumps({"kind": "cleanup_failed", "profile": name}), flush=True)
print(json.dumps({"kind": "final", "passed": passed}), flush=True)
sys.exit(0 if passed else 1)
