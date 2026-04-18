from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def workflow_path(name: str) -> Path:
    return repo_root() / ".github" / "workflows" / name


def workflow_text(name: str) -> str:
    return workflow_path(name).read_text(encoding="utf-8")


def test_ci_requires_resolvable_contracts_requirement() -> None:
    text = workflow_text("ci.yml")
    assert "Verify contracts dependency requirement resolves" in text
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts" in text
    assert "--mode latest" not in text
    assert "sync-latest" not in text


def test_security_requires_resolvable_contracts_requirement() -> None:
    text = workflow_text("security.yml")
    assert "Verify contracts dependency requirement resolves" in text
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts" in text
    assert "--mode latest" not in text


def test_refresh_workflow_is_removed() -> None:
    assert not workflow_path("refresh-contracts-pin.yml").exists()
