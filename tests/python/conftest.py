from __future__ import annotations

import sys
from pathlib import Path


def _prepend_repo_python_path() -> None:
    repo_python = Path(__file__).resolve().parents[2] / "python"
    repo_python_str = str(repo_python)
    if repo_python_str not in sys.path:
        sys.path.insert(0, repo_python_str)


_prepend_repo_python_path()
