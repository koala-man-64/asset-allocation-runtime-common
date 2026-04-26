from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def workflow_text(name: str) -> str:
    return (repo_root() / ".github" / "workflows" / name).read_text(encoding="utf-8")


def test_ci_requires_published_compatible_contracts_spec() -> None:
    text = workflow_text("ci.yml")
    assert "Verify contracts dependency spec resolves from published stable versions" in text
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts --mode published" in text


def test_security_requires_published_compatible_contracts_spec() -> None:
    text = workflow_text("security.yml")
    assert "Verify contracts dependency spec resolves from published stable versions" in text
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts --mode published" in text
