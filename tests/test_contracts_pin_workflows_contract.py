from __future__ import annotations

from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def workflow_text(name: str) -> str:
    return (repo_root() / ".github" / "workflows" / name).read_text(encoding="utf-8")


def test_ci_requires_latest_published_contracts_pin() -> None:
    text = workflow_text("ci.yml")
    assert "Verify pinned contracts package is latest published stable version" in text
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts --mode latest" in text


def test_security_requires_published_contracts_pin() -> None:
    text = workflow_text("security.yml")
    assert "Verify pinned contracts package is published" in text
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts" in text
    assert "--mode latest" not in text


def test_refresh_workflow_is_scheduled_and_dispatchable() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert 'name: Runtime Common Contracts Pin Refresh' in text
    assert "schedule:" in text
    assert '- cron: "23 10 * * 1-5"' in text
    assert "workflow_dispatch:" in text


def test_refresh_workflow_uses_repo_write_permissions_only() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert "contents: write" in text
    assert "pull-requests: write" not in text
    assert 'group: runtime-common-contracts-pin-refresh' in text


def test_refresh_workflow_syncs_latest_contracts_pin_directly_to_main() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts --mode sync-latest" in text
    assert "- name: Commit refreshed contracts pin to main" in text
    assert "git push origin HEAD:main" in text
    assert "REFRESH_BRANCH" not in text
    assert "gh pr create" not in text
    assert "gh pr edit" not in text


def test_refresh_workflow_surfaces_validation_failures_after_main_update() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert text.index("- name: Commit refreshed contracts pin to main") < text.index("- name: Install test dependencies")
    assert "continue-on-error: true" in text
    assert 'Refreshed contracts pin requires fix-forward work. Failed checks:' in text
    assert "exit 1" in text
