from __future__ import annotations

import csv
from pathlib import Path


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def release_workflow_text() -> str:
    return (repo_root() / ".github" / "workflows" / "release.yml").read_text(encoding="utf-8")


def env_template_keys() -> set[str]:
    keys: set[str] = set()
    for raw_line in (repo_root() / ".env.template").read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        keys.add(line.split("=", 1)[0].strip())
    return keys


def env_contract_rows() -> list[dict[str, str]]:
    path = repo_root() / "docs" / "ops" / "env-contract.csv"
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def test_release_workflow_emits_runtime_common_dispatch_event() -> None:
    text = release_workflow_text()
    assert '"event_type": "runtime_common_released"' in text


def test_release_workflow_dispatches_only_to_real_consumers() -> None:
    text = release_workflow_text()
    assert 'jobs_repo="${JOBS_REPOSITORY:-${owner}/asset-allocation-jobs}"' in text
    assert "CONTROL_" "PLANE_REPOSITORY" not in text
    assert 'gh api "repos/${jobs_repo}/dispatches" \\' in text
    assert "UI_REPOSITORY" not in text
    assert "asset-allocation-ui" not in text


def test_release_summary_matches_runtime_common_consumer_set() -> None:
    text = release_workflow_text()
    assert 'echo "- Downstream dispatch: \\`runtime_common_released\\` to jobs"' in text


def test_runtime_common_dispatch_config_surface_excludes_ui() -> None:
    keys = env_template_keys()
    assert "CONTROL_" "PLANE_REPOSITORY" not in keys
    assert "JOBS_REPOSITORY" in keys
    assert "UI_REPOSITORY" not in keys

    dispatch_rows = {
        row["name"]: row
        for row in env_contract_rows()
        if "runtime_common_released dispatch" in row["notes"]
    }
    assert set(dispatch_rows) == {"JOBS_REPOSITORY"}
