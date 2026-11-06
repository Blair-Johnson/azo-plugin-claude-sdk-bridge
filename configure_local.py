"""Opt-in managed bridge profiles; run with Agent Zoo's Python (PyYAML required)."""
import argparse
import copy
import json
import os
from pathlib import Path
import tempfile

import yaml


LEGACY_MARKER = "    # Local Claude Agent SDK bridge; key is loopback-only."
MANAGED_MARKER = "    # Managed Claude Agent SDK bridge; endpoint supplied by the common plugin."


class UniqueLoader(yaml.SafeLoader):
    """Reject duplicate keys rather than silently deciding which profile to edit."""


def _mapping(loader, node):
    result = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node)
        if key in result:
            raise ValueError("Duplicate configuration key")
        result[key] = loader.construct_object(value_node)
    return result


UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _load(text):
    return yaml.load(text, Loader=UniqueLoader)


def _render(profiles):
    return "".join("    " + line + "\n" for line in yaml.safe_dump(profiles, sort_keys=False).splitlines())


def _migrate(original, catalog, models):
    """Only rewrite requested entries in our final, entirely bridge-owned block."""
    lines = original.splitlines(keepends=True)
    markers = [i for i, line in enumerate(lines) if line.rstrip("\r\n") in (LEGACY_MARKER, MANAGED_MARKER)]
    if not markers:
        raise ValueError("Migration requires a recognized trailing bridge block")
    marker = markers[-1]
    body = "".join(lines[marker + 1:])
    wrapped = "llm:\n  models:\n" + body
    block = _load(wrapped)["llm"]["models"]
    if not isinstance(block, dict) or not set(models) <= set(block):
        raise ValueError("Requested profiles must belong to the trailing bridge block")
    for name, profile in block.items():
        if (not isinstance(profile, dict) or not isinstance(profile.get("tags"), list)
                or "sdk-bridge" not in profile["tags"] or profile != catalog.get(name)):
            raise ValueError("Trailing block contains an unrelated profile")
        anthropic = profile.get("anthropic", {})
        if (not isinstance(anthropic, dict) or not isinstance(anthropic.get("client"), dict)
                or not isinstance(anthropic.get("request"), dict)
                or not isinstance(anthropic["request"].get("model"), str)):
            raise ValueError("Unexpected bridge profile structure")
    if any(isinstance(token, (yaml.AliasToken, yaml.AnchorToken)) for token in yaml.scan(wrapped)):
        raise ValueError("Aliased bridge blocks require manual review")
    node = yaml.compose(wrapped).value[0][1].value[0][1]
    if not isinstance(node, yaml.MappingNode) or node.flow_style:
        raise ValueError("Expected a block-style bridge catalog")
    entries = []
    for key, value in node.value:
        if key.start_mark.column != 4 or not isinstance(value, yaml.MappingNode) or value.flow_style:
            raise ValueError("Unexpected bridge block indentation or structure")
        entries.append((key.value, key.start_mark.line - 2))
    body_lines = body.splitlines(keepends=True)
    # Associate standalone warnings immediately before a key with that profile.
    for i in range(1, len(entries)):
        name, start = entries[i]
        while start > entries[i - 1][1] + 1 and (not body_lines[start - 1].strip() or body_lines[start - 1].lstrip().startswith("#")):
            start -= 1
        entries[i] = (name, start)
    updated = body_lines[:entries[0][1]]
    replacements = {}
    for i, (name, start) in enumerate(entries):
        end = entries[i + 1][1] if i + 1 < len(entries) else len(body_lines)
        segment = body_lines[start:end]
        if name not in models:
            updated.extend(segment)
            continue
        # Full-line warning comments (including Fable billing warnings) survive.
        # Inline comments or unusual quoted '#' values need manual review instead.
        if any("#" in line and not line.lstrip().startswith("#") for line in segment):
            raise ValueError("Inline comments in migrated profiles require manual review")
        profile = copy.deepcopy(block[name])
        client = profile["anthropic"]["client"]
        client.pop("api_key", None)
        client["api_key_env"] = "AZO_CLAUDE_BRIDGE_KEY"
        client["base_url"] = "http://127.0.0.1:1"
        profile["anthropic"]["request"]["model"] = models[name]
        replacements[name] = profile
        updated.extend(line for line in segment if line.lstrip().startswith("#"))
        updated.append(_render({name: profile}))
    prefix = "".join(lines[:marker])
    # Keep the legacy label for a partial migration: other entries remain untouched.
    label = MANAGED_MARKER + "\n" if set(models) == set(block) else lines[marker]
    return prefix + label + "".join(updated), replacements


def configure(config_path, models, migrate=False):
    config_path = Path(config_path)
    if config_path.is_symlink():
        raise ValueError("Refusing a symlinked configuration file")
    original = config_path.read_bytes().decode("utf-8")
    config = _load(original)
    if (not isinstance(config, dict) or list(config) != ["llm"]
            or not isinstance(config["llm"], dict) or list(config["llm"])[-1:] != ["models"]
            or not isinstance(config["llm"]["models"], dict)):
        raise ValueError("Expected a models.yaml with llm.models as its final section")
    catalog = config["llm"]["models"]
    if not models or any(not isinstance(item, str) or not item.strip() for pair in models.items() for item in pair):
        raise ValueError("Provide nonempty profile names and SDK models")
    if migrate:
        if not set(models) <= set(catalog):
            raise ValueError("Migration only accepts existing bridge profiles")
        updated, additions = _migrate(original, catalog, models)
    else:
        if set(models) & set(catalog):
            raise ValueError("Provide new profile names, or explicitly migrate existing bridge profiles")
        base = _load((Path(__file__).parent / "models.example.yaml").read_text())["llm"]["models"]["claude-sonnet-sdk-bridge"]
        additions = {}
        for name, model in models.items():
            profile = copy.deepcopy(base)
            profile["anthropic"]["request"]["model"] = model
            profile["anthropic"]["request"]["output_config"] = {"effort": "high"}
            profile["harness"]["compaction"] = {"enabled": True, "threshold_tokens": 150000}
            additions[name] = profile
        updated = original + ("" if original.endswith("\n") else "\n") + "\n" + MANAGED_MARKER + "\n" + _render(additions)
    expected = copy.deepcopy(config)
    expected["llm"]["models"].update(additions)
    if _load(updated) != expected:
        raise ValueError("Refusing to change unrelated configuration")
    backup_base = config_path.with_name(config_path.name + ".before-managed-claude-sdk-bridge")
    index = 0
    while True:
        backup = backup_base if not index else backup_base.with_name(backup_base.name + f".{index}")
        try:
            descriptor = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            break
        except FileExistsError:
            index += 1
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as out:
        out.write(original)
        out.flush()
        os.fsync(out.fileno())
    descriptor, temporary = tempfile.mkstemp(dir=config_path.parent, prefix=".claude-bridge-")
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as out:
            out.write(updated)
            out.flush()
            os.fsync(out.fileno())
        if config_path.is_symlink() or config_path.read_bytes().decode("utf-8") != original:
            raise RuntimeError("Configuration changed during setup; retry after inspection")
        os.replace(temporary, config_path)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return {"profiles": list(additions), "config": str(config_path), "backup": str(backup)}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--model", action="append", required=True, metavar="PROFILE=SDK_MODEL")
    parser.add_argument("--migrate-existing", action="store_true", help="Migrate requested existing profiles in the owned trailing bridge block")
    args = parser.parse_args()
    pairs = [item.split("=", 1) for item in args.model]
    if any(len(pair) != 2 or not all(pair) for pair in pairs) or len(dict(pairs)) != len(pairs):
        parser.error("Each --model must name a unique PROFILE=SDK_MODEL")
    try:
        print(json.dumps(configure(args.config, dict(pairs), migrate=args.migrate_existing)))
    except Exception as error:
        parser.exit(1, f"Managed bridge setup failed ({type(error).__name__}); inspect paths and profile ownership. No credentials were printed.\n")
