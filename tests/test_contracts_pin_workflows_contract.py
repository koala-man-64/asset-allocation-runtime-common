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


def test_security_requires_latest_published_contracts_pin() -> None:
    text = workflow_text("security.yml")
    assert "Verify pinned contracts package is latest published stable version" in text
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts --mode latest" in text


def test_refresh_workflow_is_scheduled_and_dispatchable() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert 'name: Runtime Common Contracts Pin Refresh' in text
    assert "schedule:" in text
    assert '- cron: "23 10 * * 1-5"' in text
    assert "workflow_dispatch:" in text


def test_refresh_workflow_uses_repo_write_and_pr_write_permissions() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert "contents: write" in text
    assert "pull-requests: write" in text
    assert 'group: runtime-common-contracts-pin-refresh' in text


def test_refresh_workflow_syncs_latest_contracts_pin_and_opens_pr() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert "python scripts/verify_pinned_dependency.py --package asset-allocation-contracts --mode sync-latest" in text
    assert 'REFRESH_BRANCH: automation/contracts-pin-latest' in text
    assert 'git push --force-with-lease origin "HEAD:refs/heads/${REFRESH_BRANCH}"' in text
    assert 'gh pr create --base main --head "${REFRESH_BRANCH}"' in text
    assert 'gh pr edit "${pr_number}" --title "${pr_title}" --body-file "${pr_body_file}"' in text


def test_refresh_workflow_surfaces_validation_failures_after_pr_update() -> None:
    text = workflow_text("refresh-contracts-pin.yml")
    assert "continue-on-error: true" in text
    assert 'Refreshed contracts pin requires fix-forward work. Failed checks:' in text
    assert "exit 1" in text
