from __future__ import annotations

import argparse
import re
import subprocess
import sys
import tempfile
import tomllib
from dataclasses import dataclass
from pathlib import Path


STABLE_SEMVER_PATTERN = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


@dataclass(frozen=True)
class DependencyPin:
    package_name: str
    version: str

    @property
    def spec(self) -> str:
        return f"{self.package_name}=={self.version}"


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a pinned dependency in python/pyproject.toml using the configured package index."
        )
    )
    parser.add_argument("--pyproject", default="python/pyproject.toml", help="Path to the pyproject.toml file to inspect.")
    parser.add_argument("--package", required=True, help="Package name to verify, for example asset-allocation-contracts.")
    return parser


def parse_stable_semver(version: str) -> tuple[int, int, int] | None:
    match = STABLE_SEMVER_PATTERN.fullmatch(version)
    if not match:
        return None

    return tuple(int(match.group(name)) for name in ("major", "minor", "patch"))


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


def parse_dependency_pin(spec: str, package_name: str) -> DependencyPin:
    prefix = f"{package_name}=="
    if not spec.startswith(prefix):
        raise ValueError(f"Expected pinned spec for {package_name}, found '{spec}'.")

    version = spec.removeprefix(prefix)
    if parse_stable_semver(version) is None:
        raise ValueError(
            f"Expected {package_name} to use a stable semver pin, found '{version}'."
        )

    return DependencyPin(package_name=package_name, version=version)


def run_pip_command(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        check=False,
    )


def verify_pinned_dependency(spec: str) -> None:
    with tempfile.TemporaryDirectory(prefix="runtime-common-dependency-check-") as download_dir:
        result = run_pip_command(
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
            ]
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
        current_pin = parse_dependency_pin(spec, args.package)
        verify_pinned_dependency(current_pin.spec)
    except (OSError, ValueError, RuntimeError, tomllib.TOMLDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
