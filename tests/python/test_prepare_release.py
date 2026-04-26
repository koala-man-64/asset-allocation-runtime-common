from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest


def powershell_executable() -> str:
    for candidate in ("pwsh", "powershell", "powershell.exe"):
        executable = shutil.which(candidate)
        if executable:
            return executable
    pytest.skip("PowerShell is required for prepare-release.ps1 tests.")


def write_repo_fixture(tmp_path: Path) -> Path:
    repo_root = Path(__file__).resolve().parents[2]
    script_source = repo_root / "scripts" / "prepare-release.ps1"
    script_target = tmp_path / "scripts" / "prepare-release.ps1"
    script_target.parent.mkdir(parents=True, exist_ok=True)
    script_target.write_text(script_source.read_text(encoding="utf-8"), encoding="utf-8")

    pyproject_path = tmp_path / "python" / "pyproject.toml"
    pyproject_path.parent.mkdir(parents=True, exist_ok=True)
    pyproject_path.write_text(
        """
[project]
name = "asset-allocation-runtime-common"
version = "2.0.10"
""".strip()
        + "\n",
        encoding="utf-8",
    )

    contract_path = tmp_path / "docs" / "architecture" / "architecture-contract.md"
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(
        """
# Architecture Contract

| Field | Value |
| --- | --- |
| Canonical Baseline | `main` branch, package version `2.0.9` in `python/pyproject.toml` |
""".strip()
        + "\n",
        encoding="utf-8",
    )

    return script_target


def test_prepare_release_updates_stale_contract_baseline(tmp_path: Path) -> None:
    script_path = write_repo_fixture(tmp_path)

    result = subprocess.run(
        [powershell_executable(), "-NoProfile", "-File", str(script_path), "-Version", "2.1.0"],
        capture_output=True,
        text=True,
        check=False,
        cwd=tmp_path,
    )

    assert result.returncode == 0, result.stderr
    assert "Architecture contract canonical baseline version '2.0.9' does not match python/pyproject.toml version" in result.stdout
    assert "Current version: 2.0.10" in result.stdout
    assert "New version: 2.1.0" in result.stdout

    pyproject_text = (tmp_path / "python" / "pyproject.toml").read_text(encoding="utf-8")
    contract_text = (tmp_path / "docs" / "architecture" / "architecture-contract.md").read_text(encoding="utf-8")

    assert 'version = "2.1.0"' in pyproject_text
    assert "| Canonical Baseline | `main` branch, package version `2.1.0` in `python/pyproject.toml` |" in contract_text
