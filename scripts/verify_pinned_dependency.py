from __future__ import annotations

import argparse
import subprocess
import sys
import tempfile
import tomllib
from pathlib import Path


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Verify that a pinned dependency in python/pyproject.toml is resolvable from the configured index."
    )
    parser.add_argument("--pyproject", default="python/pyproject.toml", help="Path to the pyproject.toml file to inspect.")
    parser.add_argument("--package", required=True, help="Package name to verify, for example asset-allocation-contracts.")
    return parser


def load_pinned_dependency(pyproject_path: Path, package_name: str) -> str:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    dependencies = pyproject.get("project", {}).get("dependencies", [])
    prefix = f"{package_name}=="
    matches = [dependency for dependency in dependencies if dependency.startswith(prefix)]

    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one pinned dependency for {package_name} in {pyproject_path}, found {len(matches)}."
        )

    return matches[0]


def verify_pinned_dependency(spec: str) -> None:
    with tempfile.TemporaryDirectory(prefix="runtime-common-dependency-check-") as download_dir:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "download",
                "--dest",
                download_dir,
                "--disable-pip-version-check",
                "--no-deps",
                spec,
            ],
            capture_output=True,
            text=True,
            check=False,
        )

    if result.returncode == 0:
        print(f"Verified published dependency: {spec}")
        return

    output = result.stderr.strip() or result.stdout.strip() or "pip download failed without output."
    raise RuntimeError(
        "Pinned dependency could not be resolved from the configured package index: "
        f"{spec}. Publish the shared package first or update python/pyproject.toml to a published version.\n"
        f"pip output:\n{output}"
    )


def main() -> int:
    args = build_argument_parser().parse_args()
    pyproject_path = Path(args.pyproject).resolve()

    try:
        spec = load_pinned_dependency(pyproject_path, args.package)
        verify_pinned_dependency(spec)
    except (OSError, ValueError, RuntimeError, tomllib.TOMLDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
