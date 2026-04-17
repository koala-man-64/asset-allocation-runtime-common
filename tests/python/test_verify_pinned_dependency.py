from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest


def load_module():
    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "scripts" / "verify_pinned_dependency.py"
    spec = importlib.util.spec_from_file_location("verify_pinned_dependency", module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


MODULE = load_module()


def test_load_pinned_dependency_returns_exact_spec(tmp_path: Path) -> None:
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        """
[project]
dependencies = [
    "azure-identity==1.25.2",
    "asset-allocation-contracts==1.1.0",
]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    assert (
        MODULE.load_pinned_dependency(pyproject_path, "asset-allocation-contracts")
        == "asset-allocation-contracts==1.1.0"
    )


def test_verify_pinned_dependency_uses_pip_download(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run(args: list[str], *, capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        captured["args"] = args
        assert capture_output is True
        assert text is True
        assert check is False
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(MODULE.subprocess, "run", fake_run)

    MODULE.verify_pinned_dependency("asset-allocation-contracts==1.1.0")

    assert captured["args"][:4] == [sys.executable, "-m", "pip", "download"]
    assert "--pre" not in captured["args"]
    assert "asset-allocation-contracts==1.1.0" in captured["args"]


def test_verify_pinned_dependency_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(args: list[str], *, capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args,
            returncode=1,
            stdout="",
            stderr="ERROR: No matching distribution found",
        )

    monkeypatch.setattr(MODULE.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="Publish the shared package first"):
        MODULE.verify_pinned_dependency("asset-allocation-contracts==1.1.0")


def test_list_published_versions_uses_pip_index_json(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run(args: list[str], *, capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        captured["args"] = args
        return subprocess.CompletedProcess(
            args=args,
            returncode=0,
            stdout=json.dumps({"name": "asset-allocation-contracts", "versions": ["1.1.0", "1.0.0"]}),
            stderr="",
        )

    monkeypatch.setattr(MODULE.subprocess, "run", fake_run)

    assert MODULE.list_published_versions("asset-allocation-contracts") == ["1.1.0", "1.0.0"]
    assert captured["args"][:5] == [sys.executable, "-m", "pip", "index", "versions"]
    assert "--json" in captured["args"]
    assert "--pre" not in captured["args"]


def test_resolve_latest_stable_version_ignores_prereleases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "list_published_versions",
        lambda package_name: ["1.2.0rc1", "1.1.0", "1.0.5"],
    )

    assert MODULE.resolve_latest_stable_version("asset-allocation-contracts") == "1.1.0"


def test_resolve_latest_stable_version_requires_stable_release(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "list_published_versions",
        lambda package_name: ["1.2.0rc1", "1.2.0b1"],
    )

    with pytest.raises(RuntimeError, match="no stable published versions"):
        MODULE.resolve_latest_stable_version("asset-allocation-contracts")


def test_ensure_pinned_dependency_is_latest_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(MODULE, "resolve_latest_stable_version", lambda package_name: "1.2.0")

    with pytest.raises(RuntimeError, match="Latest stable is asset-allocation-contracts==1.2.0"):
        MODULE.ensure_pinned_dependency_is_latest("asset-allocation-contracts==1.1.0", "asset-allocation-contracts")


def test_sync_pinned_dependency_to_latest_updates_only_matching_pin(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        """
[project]
dependencies = [
    "asset-allocation-contracts==1.1.0",
    "azure-identity==1.25.2",
    "httpx==0.28.1",
]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(MODULE, "resolve_latest_stable_version", lambda package_name: "1.2.0")

    result = MODULE.sync_pinned_dependency_to_latest(pyproject_path, "asset-allocation-contracts")

    assert result.changed is True
    assert result.current_spec == "asset-allocation-contracts==1.1.0"
    assert result.latest_spec == "asset-allocation-contracts==1.2.0"
    content = pyproject_path.read_text(encoding="utf-8")
    assert '"asset-allocation-contracts==1.2.0"' in content
    assert '"azure-identity==1.25.2"' in content
    assert '"httpx==0.28.1"' in content


def test_sync_pinned_dependency_to_latest_is_noop_when_already_latest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    pyproject_path = tmp_path / "pyproject.toml"
    original_content = """
[project]
dependencies = [
    "asset-allocation-contracts==1.2.0",
]
""".strip() + "\n"
    pyproject_path.write_text(original_content, encoding="utf-8")

    monkeypatch.setattr(MODULE, "resolve_latest_stable_version", lambda package_name: "1.2.0")

    result = MODULE.sync_pinned_dependency_to_latest(pyproject_path, "asset-allocation-contracts")

    assert result.changed is False
    assert result.latest_spec == "asset-allocation-contracts==1.2.0"
    assert pyproject_path.read_text(encoding="utf-8") == original_content
