import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

SPEC = importlib.util.spec_from_file_location("configure_local", Path(__file__).parents[1] / "configure_local.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)

PREFIX = "# keep comment\nllm:\n  default: existing\n  models:\n    existing:\n      tags: [custom]\n"
FAKE_KEY = "synthetic-test-key-not-a-real-credential"


def legacy_config(names=("sonnet-bridge", "opus-bridge", "fable-bridge", "fable-small-bridge")):
    base = yaml.safe_load((Path(__file__).parents[1] / "models.example.yaml").read_text())["llm"]["models"]["claude-sonnet-sdk-bridge"]
    profiles = {}
    for name in names:
        profile = copy.deepcopy(base)
        client = profile["anthropic"]["client"]
        client.pop("api_key_env")
        client.update(api_key=FAKE_KEY, base_url="http://127.0.0.1:8766")
        profile["anthropic"]["request"]["model"] = "sonnet"
        profile["harness"]["compaction"] = {"enabled": True, "threshold_tokens": 150000}
        profiles[name] = profile
    block = MODULE._render(profiles)
    block = block.replace("    fable-bridge:\n", "    # Fable requires usage credits on the actual account.\n    fable-bridge:\n")
    return PREFIX + "\n" + MODULE.LEGACY_MARKER + "\n" + block


def assert_managed(profile, model):
    client = profile["anthropic"]["client"]
    assert client["base_url"] == "http://127.0.0.1:1"
    assert client["api_key_env"] == "AZO_CLAUDE_BRIDGE_KEY"
    assert "api_key" not in client
    assert profile["anthropic"]["request"]["model"] == model
    assert "sdk-bridge" in profile["tags"]


def test_setup_preserves_catalog_default_comments_and_private_backup(tmp_path):
    config = tmp_path / "models.yaml"
    config.write_text(PREFIX)
    models = {"sonnet-bridge": "sonnet", "opus-bridge": "opus"}
    result = MODULE.configure(config, models)
    loaded = yaml.safe_load(config.read_text())
    assert config.read_text().startswith(PREFIX)
    assert loaded["llm"]["default"] == "existing"
    assert loaded["llm"]["models"]["existing"] == {"tags": ["custom"]}
    assert set(result) == {"profiles", "config", "backup"}
    for name, model in models.items():
        profile = loaded["llm"]["models"][name]
        assert_managed(profile, model)
        assert profile["harness"]["compaction"]["threshold_tokens"] == 150000
    assert Path(result["backup"]).read_text() == PREFIX
    assert set(tmp_path.iterdir()) == {config, Path(result["backup"])}
    for path in (config, Path(result["backup"])):
        assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(ValueError):
        MODULE.configure(config, {"sonnet-bridge": "sonnet"})


def test_backup_collisions_do_not_block_setup_or_overwrite_backups(tmp_path):
    config = tmp_path / "models.yaml"
    config.write_text(PREFIX)
    legacy = tmp_path / "models.yaml.before-claude-sdk-bridge"
    managed = tmp_path / "models.yaml.before-managed-claude-sdk-bridge"
    legacy.write_text("old backup")
    managed.write_text("another backup")
    first = MODULE.configure(config, {"new": "sonnet"})
    second = MODULE.configure(config, {"another": "opus"})
    assert Path(first["backup"]).name.endswith(".1")
    assert Path(second["backup"]).name.endswith(".2")
    assert legacy.read_text() == "old backup"
    assert managed.read_text() == "another backup"


def test_full_legacy_migration_preserves_prefix_warnings_and_settings(tmp_path):
    config = tmp_path / "models.yaml"
    original = legacy_config()
    config.write_text(original)
    models = {name: name.removesuffix("-bridge") for name in yaml.safe_load(original)["llm"]["models"] if name != "existing"}
    result = MODULE.configure(config, models, migrate=True)
    updated = config.read_text()
    assert updated.startswith(PREFIX + "\n")
    assert "    # Fable requires usage credits on the actual account.\n    fable-bridge:\n" in updated
    assert FAKE_KEY not in updated
    assert FAKE_KEY not in json.dumps(result)
    loaded = yaml.safe_load(updated)
    expected = yaml.safe_load(original)
    for name, model in models.items():
        profile = expected["llm"]["models"][name]
        client = profile["anthropic"]["client"]
        client.pop("api_key")
        client.update(api_key_env="AZO_CLAUDE_BRIDGE_KEY", base_url="http://127.0.0.1:1")
        profile["anthropic"]["request"]["model"] = model
        assert_managed(loaded["llm"]["models"][name], model)
    assert loaded == expected
    assert Path(result["backup"]).read_text() == original
    assert Path(result["backup"]).stat().st_mode & 0o777 == 0o600
    # Already managed entries can be explicitly migrated again without backup collisions.
    MODULE.configure(config, models, migrate=True)
    assert yaml.safe_load(config.read_text()) == expected


def test_partial_migration_leaves_unrequested_profile_text_unchanged(tmp_path):
    config = tmp_path / "models.yaml"
    original = legacy_config(("sonnet-bridge", "opus-bridge"))
    config.write_text(original)
    untouched = original[original.index("    opus-bridge:"):]
    MODULE.configure(config, {"sonnet-bridge": "sonnet"}, migrate=True)
    assert config.read_text().endswith(untouched)
    assert_managed(yaml.safe_load(config.read_text())["llm"]["models"]["sonnet-bridge"], "sonnet")


@pytest.mark.parametrize("original,models,migrate", [
    ("llm:\n  models: {}\npilot: {}\n", {"new": "sonnet"}, False),
    (PREFIX, {}, False),
    (PREFIX, {"": "sonnet"}, False),
    (PREFIX, {"new": " "}, False),
    (PREFIX, {"existing": "sonnet"}, False),
    (PREFIX, {"missing": "sonnet"}, True),
    (PREFIX, {"existing": "sonnet"}, True),
    (legacy_config().replace(MODULE.LEGACY_MARKER, "    # not an owned marker"), {"sonnet-bridge": "sonnet"}, True),
    (legacy_config() + "    unrelated:\n      tags: [custom]\n", {"sonnet-bridge": "sonnet"}, True),
    (legacy_config().replace("    sonnet-bridge:\n      tags:", "    sonnet-bridge:\n      tags: # inline note\n      unused:", 1), {"sonnet-bridge": "sonnet"}, True),
    (legacy_config().replace("      context_window_tokens: 200000", "      context_window_tokens: 200000 # retain me", 1), {"sonnet-bridge": "sonnet"}, True),
    (legacy_config().replace("      anthropic:\n", "      anthropic: null\n      other:\n", 1), {"sonnet-bridge": "sonnet"}, True),
    (legacy_config().replace("    sonnet-bridge:\n", "    sonnet-bridge: &profile\n", 1), {"sonnet-bridge": "sonnet"}, True),
    (PREFIX + "    existing:\n      tags: [duplicate]\n", {"new": "sonnet"}, False),
])
def test_invalid_or_unowned_edits_fail_without_writes(tmp_path, original, models, migrate):
    config = tmp_path / "models.yaml"
    config.write_text(original)
    with pytest.raises(ValueError):
        MODULE.configure(config, models, migrate=migrate)
    assert config.read_text() == original
    assert list(tmp_path.iterdir()) == [config]


def test_config_symlink_is_rejected(tmp_path):
    target, config = tmp_path / "target", tmp_path / "models.yaml"
    target.write_text(PREFIX)
    config.symlink_to(target)
    with pytest.raises(ValueError):
        MODULE.configure(config, {"new": "sonnet"})
    assert config.is_symlink()
    assert target.read_text() == PREFIX


def test_concurrent_change_is_not_replaced_and_temporary_is_removed(tmp_path, monkeypatch):
    config = tmp_path / "models.yaml"
    config.write_text(PREFIX)
    mkstemp = MODULE.tempfile.mkstemp

    def change_before_write(*args, **kwargs):
        config.write_text(PREFIX + "# concurrent edit\n")
        return mkstemp(*args, **kwargs)

    monkeypatch.setattr(MODULE.tempfile, "mkstemp", change_before_write)
    with pytest.raises(RuntimeError):
        MODULE.configure(config, {"new": "sonnet"})
    assert config.read_text() == PREFIX + "# concurrent edit\n"
    assert not list(tmp_path.glob(".claude-bridge-*"))
    assert (tmp_path / "models.yaml.before-managed-claude-sdk-bridge").read_text() == PREFIX


def test_cli_migration_is_explicit_and_never_prints_credentials(tmp_path):
    config = tmp_path / "models.yaml"
    original = legacy_config(("sonnet-bridge",))
    config.write_text(original)
    command = [sys.executable, str(Path(MODULE.__file__)), "--config", str(config), "--model", "sonnet-bridge=sonnet"]
    refused = subprocess.run(command, capture_output=True, text=True)
    assert refused.returncode == 1
    assert config.read_text() == original
    migrated = subprocess.run(command + ["--migrate-existing"], capture_output=True, text=True)
    assert migrated.returncode == 0
    assert json.loads(migrated.stdout)["profiles"] == ["sonnet-bridge"]
    assert FAKE_KEY not in refused.stdout + refused.stderr + migrated.stdout + migrated.stderr


@pytest.mark.parametrize("original", [PREFIX.rstrip("\n").encode(), PREFIX.replace("\n", "\r\n").encode()])
def test_setup_preserves_original_prefix_bytes(tmp_path, original):
    config = tmp_path / "models.yaml"
    config.write_bytes(original)
    result = MODULE.configure(config, {"new": "sonnet"})
    assert config.read_bytes().startswith(original)
    assert Path(result["backup"]).read_bytes() == original


def test_failed_atomic_replace_leaves_original_and_cleans_temporary(tmp_path, monkeypatch):
    config = tmp_path / "models.yaml"
    config.write_text(PREFIX)

    def refuse_replace(*args):
        raise OSError("synthetic replace failure")

    monkeypatch.setattr(MODULE.os, "replace", refuse_replace)
    with pytest.raises(OSError):
        MODULE.configure(config, {"new": "sonnet"})
    assert config.read_text() == PREFIX
    assert not list(tmp_path.glob(".claude-bridge-*"))
    assert (tmp_path / "models.yaml.before-managed-claude-sdk-bridge").read_text() == PREFIX
