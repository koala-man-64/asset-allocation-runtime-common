from __future__ import annotations

import importlib.util
import io
import subprocess
import sys
import tarfile
import zipfile
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


def test_repo_pyproject_declares_contracts_pin() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    pyproject_path = repo_root / "python" / "pyproject.toml"

    assert (
        MODULE.load_pinned_dependency(pyproject_path, "asset-allocation-contracts")
        == "asset-allocation-contracts==2.4.0"
    )


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


def test_verify_dependency_requirement_uses_pip_download(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, list[str]] = {}

    def fake_run(args: list[str], *, capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        captured["args"] = args
        assert capture_output is True
        assert text is True
        assert check is False
        return subprocess.CompletedProcess(args=args, returncode=0, stdout="ok", stderr="")

    monkeypatch.setattr(MODULE.subprocess, "run", fake_run)

    MODULE.verify_dependency_requirement("asset-allocation-contracts==1.1.0")

    assert captured["args"][:4] == [sys.executable, "-m", "pip", "download"]
    assert "--pre" not in captured["args"]
    assert "asset-allocation-contracts==1.1.0" in captured["args"]


def test_verify_dependency_requirement_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_run(args: list[str], *, capture_output: bool, text: bool, check: bool) -> subprocess.CompletedProcess[str]:
        return subprocess.CompletedProcess(
            args=args,
            returncode=1,
            stdout="",
            stderr="ERROR: No matching distribution found",
        )

    monkeypatch.setattr(MODULE.subprocess, "run", fake_run)

    with pytest.raises(RuntimeError, match="Publish the pinned shared package first"):
        MODULE.verify_dependency_requirement("asset-allocation-contracts==1.1.0")


def test_parse_dependency_requirement_rejects_non_stable_semver() -> None:
    with pytest.raises(ValueError, match="stable semver pin"):
        MODULE.parse_dependency_requirement("asset-allocation-contracts==2.0.0rc1", "asset-allocation-contracts")


def write_wheel(path: Path, requires_dist: str) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            "asset_allocation_runtime_common-2.0.3.dist-info/METADATA",
            "\n".join(
                [
                    "Metadata-Version: 2.4",
                    "Name: asset-allocation-runtime-common",
                    "Version: 2.0.3",
                    f"Requires-Dist: {requires_dist}",
                    "",
                ]
            ),
        )


def write_sdist(path: Path, requires_dist: str) -> None:
    metadata = "\n".join(
        [
            "Metadata-Version: 2.4",
            "Name: asset-allocation-runtime-common",
            "Version: 2.0.3",
            f"Requires-Dist: {requires_dist}",
            "",
        ]
    ).encode("utf-8")

    root_info = tarfile.TarInfo(name="asset_allocation_runtime_common-2.0.3/PKG-INFO")
    root_info.size = len(metadata)
    egg_info = tarfile.TarInfo(name="asset_allocation_runtime_common-2.0.3/asset_allocation_runtime_common.egg-info/PKG-INFO")
    egg_info.size = len(metadata)
    with tarfile.open(path, "w:gz") as archive:
        archive.addfile(root_info, io.BytesIO(metadata))
        archive.addfile(egg_info, io.BytesIO(metadata))


def test_verify_built_distributions_accepts_exact_pins(tmp_path: Path) -> None:
    distribution_dir = tmp_path / "dist"
    distribution_dir.mkdir()
    write_wheel(distribution_dir / "asset_allocation_runtime_common-2.0.3-py3-none-any.whl", "asset-allocation-contracts==1.1.0")
    write_sdist(distribution_dir / "asset_allocation_runtime_common-2.0.3.tar.gz", "asset-allocation-contracts==1.1.0")

    MODULE.verify_built_distributions(distribution_dir, "asset-allocation-contracts==1.1.0")


def test_verify_built_distributions_rejects_non_exact_metadata(tmp_path: Path) -> None:
    distribution_dir = tmp_path / "dist"
    distribution_dir.mkdir()
    write_wheel(
        distribution_dir / "asset_allocation_runtime_common-2.0.3-py3-none-any.whl",
        "asset-allocation-contracts>=1.1.0",
    )

    with pytest.raises(RuntimeError, match="expected pinned dependency"):
        MODULE.verify_built_distributions(distribution_dir, "asset-allocation-contracts==1.1.0")
