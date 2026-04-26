from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib
import zipfile
from dataclasses import dataclass
from email import message_from_string
from pathlib import Path
from pathlib import PurePosixPath


STABLE_SEMVER_PATTERN = re.compile(r"^(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+)$")
DEPENDENCY_NAME_PATTERN = re.compile(r"^\s*(?P<name>[A-Za-z0-9][A-Za-z0-9._-]*)")
VERSION_CONSTRAINT_PATTERN = re.compile(r"(?P<operator>==|>=|<=|>|<)?\s*(?P<version>\d+\.\d+\.\d+)")


@dataclass(frozen=True)
class DependencyRequirement:
    package_name: str
    pinned_version: str

    @property
    def spec(self) -> str:
        return f"{self.package_name}=={self.pinned_version}"


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
    parser.add_argument(
        "--distribution-dir",
        help="Optional directory containing built wheel or sdist files whose metadata should declare the same spec.",
    )
    return parser


def parse_stable_semver(version: str) -> tuple[int, int, int] | None:
    match = STABLE_SEMVER_PATTERN.fullmatch(version)
    if not match:
        return None

    return tuple(int(match.group(name)) for name in ("major", "minor", "patch"))


def normalize_package_name(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def parse_dependency_name(spec: str) -> str:
    match = DEPENDENCY_NAME_PATTERN.match(spec)
    if not match:
        raise ValueError(f"Could not parse dependency name from '{spec}'.")

    return normalize_package_name(match.group("name"))


def load_dependency_spec(pyproject_path: Path, package_name: str) -> str:
    pyproject = tomllib.loads(pyproject_path.read_text(encoding="utf-8"))
    dependencies = pyproject.get("project", {}).get("dependencies", [])
    expected_name = normalize_package_name(package_name)
    matches = [dependency for dependency in dependencies if parse_dependency_name(dependency) == expected_name]

    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one dependency spec for {package_name} in {pyproject_path}, found {len(matches)}."
        )

    return matches[0]


def dependency_constraints(spec: str, package_name: str) -> list[str]:
    match = DEPENDENCY_NAME_PATTERN.match(spec)
    if not match or normalize_package_name(match.group("name")) != normalize_package_name(package_name):
        raise ValueError(f"Expected dependency spec for {package_name}, found '{spec}'.")

    constraint_text = spec[match.end() :].strip()
    if constraint_text.startswith("["):
        extras_end = constraint_text.find("]")
        if extras_end == -1:
            raise ValueError(f"Dependency spec contains an unterminated extras marker: '{spec}'.")
        constraint_text = constraint_text[extras_end + 1 :].strip()

    constraint_text = constraint_text.split(";", 1)[0].strip()
    if not constraint_text or constraint_text.startswith(";"):
        return []

    return [part.strip() for part in constraint_text.split(",") if part.strip()]


def stable_version_satisfies_spec(version: str, spec: str, package_name: str) -> bool:
    parsed_version = parse_stable_semver(version)
    if parsed_version is None:
        return False

    for constraint in dependency_constraints(spec, package_name):
        match = VERSION_CONSTRAINT_PATTERN.fullmatch(constraint)
        if not match:
            raise ValueError(f"Unsupported version constraint in dependency spec '{spec}': '{constraint}'.")

        operator = match.group("operator") or "=="
        target = parse_stable_semver(match.group("version"))
        if target is None:
            return False

        if operator == "==" and parsed_version != target:
            return False
        if operator == ">=" and parsed_version < target:
            return False
        if operator == "<=" and parsed_version > target:
            return False
        if operator == ">" and parsed_version <= target:
            return False
        if operator == "<" and parsed_version >= target:
            return False

    return True


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
        ]
    )

    if result.returncode != 0:
        output = result.stderr.strip() or result.stdout.strip() or "pip index failed without output."
        raise RuntimeError(f"Could not list published versions for {package_name}.\npip output:\n{output}")

    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"pip index returned invalid JSON for {package_name}: {exc}") from exc

    versions = payload.get("versions")
    if not isinstance(versions, list):
        raise RuntimeError(f"pip index JSON for {package_name} did not include a versions list.")

    return [str(version) for version in versions]


def resolve_compatible_versions(spec: str, package_name: str) -> list[str]:
    compatible = [
        version
        for version in list_published_versions(package_name)
        if stable_version_satisfies_spec(version, spec, package_name)
    ]
    return sorted(compatible, key=lambda version: parse_stable_semver(version) or (0, 0, 0))


def verify_dependency_spec(spec: str, package_name: str) -> str:
    compatible_versions = resolve_compatible_versions(spec, package_name)
    if not compatible_versions:
        raise RuntimeError(
            f"{spec} has no compatible published stable versions for {package_name}. "
            "Publish the shared package first or update python/pyproject.toml to a published stable version."
        )

    selected_version = compatible_versions[-1]
    print(f"Verified published dependency spec: {spec} -> {selected_version}")
    return selected_version


def parse_dependency_requirement(spec: str, package_name: str) -> DependencyRequirement:
    prefix = f"{package_name}=="
    if not spec.startswith(prefix):
        raise ValueError(f"Expected == requirement for {package_name}, found '{spec}'.")

    version = spec.removeprefix(prefix)
    if parse_stable_semver(version) is None:
        raise ValueError(
            f"Expected {package_name} to use a stable semver pin, found '{version}'."
        )

    return DependencyRequirement(package_name=package_name, pinned_version=version)


def run_pip_command(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        arguments,
        capture_output=True,
        text=True,
        check=False,
    )


def verify_dependency_requirement(spec: str) -> None:
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
        print(f"Verified published dependency requirement: {spec}")
        return

    output = result.stderr.strip() or result.stdout.strip() or "pip download failed without output."
    raise RuntimeError(
        "Dependency requirement could not be resolved from the configured package index: "
        f"{spec}. Publish the pinned shared package first or update python/pyproject.toml to the exact published version.\n"
        f"pip output:\n{output}"
    )


def read_distribution_metadata(distribution_path: Path) -> str:
    if distribution_path.suffix == ".whl":
        with zipfile.ZipFile(distribution_path) as archive:
            metadata_names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(metadata_names) != 1:
                raise ValueError(
                    f"Expected exactly one wheel metadata file in {distribution_path}, found {len(metadata_names)}."
                )
            return archive.read(metadata_names[0]).decode("utf-8")

    if distribution_path.suffixes[-2:] == [".tar", ".gz"]:
        with tarfile.open(distribution_path, "r:gz") as archive:
            members = [
                member
                for member in archive.getmembers()
                if member.isfile()
                and PurePosixPath(member.name).name == "PKG-INFO"
                and ".egg-info/" not in member.name
            ]
            if len(members) != 1:
                raise ValueError(
                    f"Expected exactly one sdist PKG-INFO file in {distribution_path}, found {len(members)}."
                )
            handle = archive.extractfile(members[0])
            if handle is None:
                raise ValueError(f"Could not extract PKG-INFO from {distribution_path}.")
            return handle.read().decode("utf-8")

    raise ValueError(f"Unsupported distribution format for metadata inspection: {distribution_path.name}")


def verify_distribution_requirement(distribution_path: Path, spec: str) -> None:
    metadata = message_from_string(read_distribution_metadata(distribution_path))
    requirements = [value.strip() for value in metadata.get_all("Requires-Dist", [])]
    if spec not in requirements:
        declared = ", ".join(requirements) if requirements else "(none)"
        raise RuntimeError(
            "Built distribution metadata does not declare the expected dependency spec: "
            f"{distribution_path} expected {spec}; declared requirements: {declared}."
        )


def verify_built_distributions(distribution_dir: Path, spec: str) -> None:
    if not distribution_dir.is_dir():
        raise ValueError(f"Distribution directory does not exist: {distribution_dir}")

    distributions = sorted(
        path
        for path in distribution_dir.iterdir()
        if path.is_file() and (path.suffix == ".whl" or path.suffixes[-2:] == [".tar", ".gz"])
    )
    if not distributions:
        raise ValueError(f"No wheel or sdist files found in {distribution_dir}.")

    for distribution_path in distributions:
        verify_distribution_requirement(distribution_path, spec)
        print(f"Verified built distribution metadata: {distribution_path.name} -> {spec}")


def main() -> int:
    args = build_argument_parser().parse_args()
    pyproject_path = Path(args.pyproject).resolve()

    try:
        spec = load_dependency_spec(pyproject_path, args.package)
        verify_dependency_spec(spec, args.package)
        if args.distribution_dir:
            verify_built_distributions(Path(args.distribution_dir).resolve(), spec)
    except (OSError, ValueError, RuntimeError, tomllib.TOMLDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
