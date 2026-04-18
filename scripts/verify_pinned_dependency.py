from __future__ import annotations

import argparse
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
            "Verify an exact dependency pin in python/pyproject.toml and optionally in built distributions."
        )
    )
    parser.add_argument("--pyproject", default="python/pyproject.toml", help="Path to the pyproject.toml file to inspect.")
    parser.add_argument("--package", required=True, help="Package name to verify, for example asset-allocation-contracts.")
    parser.add_argument(
        "--distribution-dir",
        help="Optional directory containing built wheel or sdist files whose metadata should declare the same exact pin.",
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
            f"Expected exactly one exact-pinned dependency for {package_name} in {pyproject_path}, found {len(matches)}."
        )

    return matches[0]


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
            "Built distribution metadata does not declare the expected pinned dependency: "
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
        spec = load_pinned_dependency(pyproject_path, args.package)
        requirement = parse_dependency_requirement(spec, args.package)
        verify_dependency_requirement(requirement.spec)
        if args.distribution_dir:
            verify_built_distributions(Path(args.distribution_dir).resolve(), requirement.spec)
    except (OSError, ValueError, RuntimeError, tomllib.TOMLDecodeError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
