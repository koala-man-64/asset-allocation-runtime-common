from __future__ import annotations

import sys
from pathlib import Path

import pytest


def _prepend_repo_python_path() -> None:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    repo_python_str = str(repo_python)
    if repo_python_str not in sys.path:
        sys.path.insert(0, repo_python_str)


_prepend_repo_python_path()


@pytest.fixture(autouse=True)
def _reset_timeout_circuit_registry() -> None:
    from asset_allocation_runtime_common.shared_core.timeout_circuit_breaker import reset_timeout_circuit_registry

    reset_timeout_circuit_registry()
    yield
    reset_timeout_circuit_registry()
