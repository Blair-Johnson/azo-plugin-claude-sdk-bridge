import importlib.util
from pathlib import Path

import pytest
import yaml

SPEC = importlib.util.spec_from_file_location("configure_local", Path(__file__).parents[1] / "configure_local.py")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def test_setup_preserves_catalog_default_comments_and_private_backup(tmp_path):
    config, key = tmp_path / "models.yaml", tmp_path / "bridge" / "key"
    original = "# keep comment\nllm:\n  default: existing\n  models:\n    existing:\n      tags: [custom]\n"
    config.write_text(original)
    result = MODULE.configure(config, key, {"sonnet-bridge": "sonnet", "opus-bridge": "opus"})
    loaded = yaml.safe_load(config.read_text())
    assert config.read_text().startswith(original)
    assert loaded["llm"]["default"] == "existing"
    assert loaded["llm"]["models"]["existing"] == {"tags": ["custom"]}
    for name in result["profiles"]:
        client = loaded["llm"]["models"][name]["anthropic"]["client"]
        assert client["api_key"] == key.read_text().strip()
        assert "api_key_env" not in client
    assert key.read_text().strip() not in str(result)
    assert len(key.read_text().strip()) == 64
    assert Path(result["backup"]).read_text() == original
    for path in (config, key, Path(result["backup"])):
        assert path.stat().st_mode & 0o077 == 0
    with pytest.raises(ValueError):
        MODULE.configure(config, key, {"sonnet-bridge": "sonnet"})


def test_wrong_config_shape_is_not_modified(tmp_path):
    config, key = tmp_path / "config.yaml", tmp_path / "key"
    original = "llm:\n  models: {}\npilot: {}\n"
    config.write_text(original)
    with pytest.raises(ValueError):
        MODULE.configure(config, key, {"new": "sonnet"})
    assert config.read_text() == original
    assert not key.exists()


def test_public_key_is_rejected_without_changing_config(tmp_path):
    config, key = tmp_path / "models.yaml", tmp_path / "key"
    original = "llm:\n  models: {}\n"
    config.write_text(original)
    key.write_text("x" * 64)
    key.chmod(0o644)
    with pytest.raises(ValueError):
        MODULE.configure(config, key, {"new": "sonnet"})
    assert config.read_text() == original
