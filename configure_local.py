"""Opt-in local setup; run with Agent Zoo's Python (PyYAML required)."""
import argparse
import copy
import json
import os
from pathlib import Path
import secrets
import tempfile

import yaml


def configure(config_path, key_path, models, port=8766):
    config_path, key_path = Path(config_path), Path(key_path)
    original = config_path.read_text()
    config = yaml.safe_load(original)
    if list(config) != ["llm"] or list(config["llm"])[-1] != "models":
        raise ValueError("Expected a models.yaml with llm.models as its final section")
    catalog = config["llm"]["models"]
    if not models or set(models) & set(catalog):
        raise ValueError("Provide new, distinct model profile names")
    if not 1 <= port <= 65535:
        raise ValueError("Invalid loopback port")
    key_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if key_path.is_symlink():
        raise ValueError("Refusing a symlinked key file")
    if not key_path.exists():
        with os.fdopen(os.open(key_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as out:
            out.write(secrets.token_hex(32) + "\n")
    if key_path.stat().st_mode & 0o077:
        raise ValueError("Key file must be private (mode 0600)")
    key = key_path.read_text().strip()
    if len(key) < 32:
        raise ValueError("Local key is too short")
    base = yaml.safe_load((Path(__file__).parent / "models.example.yaml").read_text())["llm"]["models"]["claude-sonnet-sdk-bridge"]
    additions = {}
    for name, model in models.items():
        profile = copy.deepcopy(base)
        client = profile["anthropic"]["client"]
        client.pop("api_key_env", None)
        client["api_key"] = key
        client["base_url"] = f"http://127.0.0.1:{port}"
        profile["anthropic"]["request"]["model"] = model
        profile["anthropic"]["request"]["output_config"] = {"effort": "high"}
        profile["harness"]["compaction"] = {"enabled": True, "threshold_tokens": 150000}
        additions[name] = profile
    snippet = yaml.safe_dump(additions, sort_keys=False)
    updated = original.rstrip() + "\n\n    # Local Claude Agent SDK bridge; key is loopback-only.\n" + "".join("    " + line + "\n" for line in snippet.splitlines())
    expected = copy.deepcopy(config)
    expected["llm"]["models"].update(additions)
    if yaml.safe_load(updated) != expected:
        raise ValueError("Refusing to change unrelated configuration")
    backup = config_path.with_name(config_path.name + ".before-claude-sdk-bridge")
    with os.fdopen(os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "w") as out:
        out.write(original)
    descriptor, temporary = tempfile.mkstemp(dir=config_path.parent, prefix=".claude-bridge-")
    try:
        with os.fdopen(descriptor, "w") as out:
            out.write(updated)
        if config_path.read_text() != original:
            raise RuntimeError("Configuration changed during setup; retry after inspection")
        os.replace(temporary, config_path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {"profiles": list(additions), "config": str(config_path), "backup": str(backup), "key_file": str(key_path), "port": port}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--key-file", type=Path, required=True)
    parser.add_argument("--port", type=int, default=8766)
    parser.add_argument("--model", action="append", required=True, metavar="PROFILE=SDK_MODEL")
    args = parser.parse_args()
    pairs = [item.split("=", 1) for item in args.model]
    if any(len(pair) != 2 or not all(pair) for pair in pairs) or len(dict(pairs)) != len(pairs):
        parser.error("Each --model must name a unique PROFILE=SDK_MODEL")
    try:
        print(json.dumps(configure(args.config, args.key_file, dict(pairs), args.port)))
    except Exception as error:
        parser.exit(1, f"Local bridge setup failed ({type(error).__name__}); inspect paths and existing profiles. No key was printed.\n")
