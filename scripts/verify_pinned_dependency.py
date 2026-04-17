from __future__ import annotations

import argparse
import json
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


@dataclass(frozen=True)
class SyncResult:
    current_spec: str
    latest_spec: str
    changed: bool


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify or refresh a pinned dependency in python/pyproject.toml using the configured package index."
        )
    )
    parser.add_argument("--pyproject", default="python/pyproject.toml", help="Path to the pyproject.toml file to inspect.")
    parser.add_argument("--package", required=True, help="Package name to verify, for example asset-allocation-contracts.")
    parser.add_argument(
        "--mode",
        choices=("published", "latest", "sync-latest"),
        default="published",
        help=(
            "published verifies the exact pin resolves from the configured index; "
            "latest verifies the exact pin matches the latest stable published version; "
            "sync-latest rewrites the exact pin in-place to the latest stable published version."
        ),
    )
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


def resolve_latest_stable_version(package_name: str) -> str:
    stable_versions = [
        version
        for version in list_published_versions(package_name)
        if parse_stable_semver(version) is not None
    ]

    if not stable_versions:
        raise RuntimeError(
            f"Configured package index returned no stable published versions for {package_name}."
        )

    return max(stable_versions, key=lambda version: parse_stable_semver(version) or (-1, -1, -1))


def ensure_pinned_dependency_is_latest(spec: str, package_name: str) -> None:
    current_pin = parse_dependency_pin(spec, package_name)
    latest_version = resolve_latest_stable_version(package_name)
    latest_spec = f"{package_name}=={latest_version}"

    if current_pin.version == latest_version:
        print(f"Verified latest stable dependency pin: {latest_spec}")
        return

    raise RuntimeError(
        "Pinned dependency is not the latest stable published version: "
        f"{current_pin.spec}. Latest stable is {latest_spec}. "
        f"Run `python scripts/verify_pinned_dependency.py --package {package_name} --mode sync-latest` "
        "to update python/pyproject.toml."
    )


def replace_dependency_spec(pyproject_path: Path, current_spec: str, next_spec: str) -> bool:
    content = pyproject_path.read_text(encoding="utf-8")
    match_count = content.count(current_spec)
    if match_count != 1:
        raise ValueError(
            f"Expected exactly one pinned dependency match for {current_spec} in {pyproject_path}, found {match_count}."
        )

    if current_spec == next_spec:
        return False

    updated_content = content.replace(current_spec, next_spec)
    pyproject_path.write_text(updated_content, encoding="utf-8")
    return True


def sync_pinned_dependency_to_latest(pyproject_path: Path, package_name: str) -> SyncResult:
    current_spec = load_pinned_dependency(pyproject_path, package_name)
    current_pin = parse_dependency_pin(current_spec, package_name)
    latest_spec = f"{package_name}=={resolve_latest_stable_version(package_name)}"
    changed = replace_dependency_spec(pyproject_path, current_pin.spec, latest_spec)
    return SyncResult(current_spec=current_pin.spec, latest_spec=latest_spec, changed=changed)


def main() -> int:
    args = build_argument_parser().parse_args()
    pyproject_path = Path(args.pyproject).resolve()

    try:
        spec = load_pinned_dependency(pyproject_path, args.package)

        if args.mode == "published":
            verify_pinned_dependency(spec)
        elif args.mode == "latest":
            verify_pinned_dependency(spec)
            ensure_pinned_dependency_is_latest(spec, args.package)
        else:
            result = sync_pinned_dependency_to_latest(pyproject_path, args.package)
            if result.changed:
                print(f"Updated pinned dependency: {result.current_spec} -> {result.latest_spec}")
            else:
                print(f"Pinned dependency already latest stable: {result.latest_spec}")
    except (OSError, ValueError, RuntimeError, tomllib.TOMLDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
