from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tomllib
from pathlib import Path
import re

from packaging.requirements import Requirement
from packaging.version import Version


STABLE_SEMVER_PATTERN = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify a dependency spec in python/pyproject.toml resolves to at least one published stable version."
        )
    )
    parser.add_argument("--pyproject", default="python/pyproject.toml", help="Path to the pyproject.toml file to inspect.")
    parser.add_argument("--package", required=True, help="Package name to verify, for example asset-allocation-contracts.")
    parser.add_argument(
        "--mode",
        choices=("published",),
        default="published",
        help=(
            "published verifies the dependency spec resolves from the configured index "
            "to at least one stable published version."
        ),
    )
    return parser


def parse_stable_semver(version: str) -> tuple[int, int, int] | None:
    match = STABLE_SEMVER_PATTERN.fullmatch(version)
    if not match:
        return None

    return tuple(int(match.group(name)) for name in ("major", "minor", "patch"))


def load_dependency_spec(pyproject_path: Path, package_name: str) -> str:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    dependencies = pyproject.get("project", {}).get("dependencies", [])
    matches = [dependency for dependency in dependencies if Requirement(dependency).name == package_name]

    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one dependency spec for {package_name} in {pyproject_path}, found {len(matches)}."
        )

    return matches[0]


def run_pip_command(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        check=False,
    )


def list_published_versions(package_name: str) -> list[str]:
    result = run_pip_command(
        [
            sys.executable,
            "-m",
            "pip",
            "index",
            "versions",
            package_name,
            "--json",
            "--disable-pip-version-check",
        ]
    )

    if result.returncode != 0:
        output = result.stderr.strip() or result.stdout.strip() or "pip index failed without output."
        raise RuntimeError(
            "Could not resolve published versions from the configured package index: "
            f"{package_name}.\npip output:\n{output}"
        )

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"Configured package index returned invalid version metadata for {package_name}: {exc}"
        ) from exc

    versions = payload.get("versions")
    if not isinstance(versions, list):
        raise RuntimeError(
            f"Configured package index did not return a versions list for {package_name}."
        )

    return [str(version) for version in versions]


def resolve_compatible_versions(spec: str, package_name: str) -> list[str]:
    requirement = Requirement(spec)
    if requirement.name != package_name:
        raise ValueError(f"Expected requirement for {package_name}, found '{spec}'.")

    compatible_versions = [
        version
        for version in list_published_versions(package_name)
        if parse_stable_semver(version) is not None and requirement.specifier.contains(Version(version), prereleases=False)
    ]
    compatible_versions.sort(key=lambda version: parse_stable_semver(version) or (-1, -1, -1))
    return compatible_versions


def verify_dependency_spec(spec: str, package_name: str) -> str:
    compatible_versions = resolve_compatible_versions(spec, package_name)
    if not compatible_versions:
        raise RuntimeError(
            "Dependency spec has no compatible published stable versions on the configured package index: "
            f"{spec}. Publish a compatible shared package version or widen the supported range.\n"
        )

    resolved = compatible_versions[-1]
    print(f"Verified published compatible dependency spec: {spec} -> {package_name}=={resolved}")
    return resolved


def main() -> int:
    args = build_argument_parser().parse_args()
    pyproject_path = Path(args.pyproject).resolve()

    try:
        spec = load_dependency_spec(pyproject_path, args.package)
        verify_dependency_spec(spec, args.package)
    except (OSError, ValueError, RuntimeError, tomllib.TOMLDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
