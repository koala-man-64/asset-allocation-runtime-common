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


def test_load_dependency_spec_returns_supported_spec(tmp_path: Path) -> None:
    pyproject_path = tmp_path / "pyproject.toml"
    pyproject_path.write_text(
        """
[project]
dependencies = [
    "azure-identity==1.25.2",
    "asset-allocation-contracts>=1.1.0,<2.0.0",
]
""".strip()
        + "\n",
        encoding="utf-8",
    )

    assert (
        MODULE.load_dependency_spec(pyproject_path, "asset-allocation-contracts")
        == "asset-allocation-contracts>=1.1.0,<2.0.0"
    )


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
    assert "--pre" not in captured["args"]
    assert "--json" in captured["args"]


def test_resolve_compatible_versions_ignores_prereleases(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "list_published_versions",
        lambda package_name: ["1.2.0rc1", "1.1.0", "1.0.5"],
    )

    assert MODULE.resolve_compatible_versions(
        "asset-allocation-contracts>=1.0.0,<2.0.0",
        "asset-allocation-contracts",
    ) == ["1.0.5", "1.1.0"]


def test_verify_dependency_spec_returns_highest_compatible_version(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        MODULE,
        "resolve_compatible_versions",
        lambda spec, package_name: ["1.1.0", "1.2.0"],
    )

    assert (
        MODULE.verify_dependency_spec(
            "asset-allocation-contracts>=1.1.0,<2.0.0",
            "asset-allocation-contracts",
        )
        == "1.2.0"
    )


def test_verify_dependency_spec_raises_clear_error_when_no_match(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(MODULE, "resolve_compatible_versions", lambda spec, package_name: [])

    with pytest.raises(RuntimeError, match="has no compatible published stable versions"):
        MODULE.verify_dependency_spec(
            "asset-allocation-contracts>=1.1.0,<2.0.0",
            "asset-allocation-contracts",
        )
