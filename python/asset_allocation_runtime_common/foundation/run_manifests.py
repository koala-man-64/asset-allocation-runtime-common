from __future__ import annotations

from typing import Any

from asset_allocation_runtime_common.shared_core import run_manifests as _shared

mdc = _shared.mdc

create_bronze_alpha26_manifest = _shared.create_bronze_alpha26_manifest
create_bronze_finance_manifest = _shared.create_bronze_finance_manifest
manifest_blobs = _shared.manifest_blobs
silver_finance_ack_exists = _shared.silver_finance_ack_exists
write_silver_finance_ack = _shared.write_silver_finance_ack


def load_latest_bronze_alpha26_manifest(domain: str) -> dict[str, Any] | None:
    return _shared.load_latest_bronze_alpha26_manifest(domain)


def resolve_active_bronze_alpha26_prefix(domain: str) -> str | None:
    manifest = load_latest_bronze_alpha26_manifest(domain)
    if not isinstance(manifest, dict):
        return None
    data_prefix = str(manifest.get("dataPrefix") or "").strip().strip("/")
    return data_prefix or None


def load_latest_bronze_finance_manifest() -> dict[str, Any] | None:
    manifest = load_latest_bronze_alpha26_manifest("finance")
    if not isinstance(manifest, dict):
        return None

    out = dict(manifest)
    normalized_blobs = manifest_blobs(out)
    if normalized_blobs:
        out.setdefault("blobs", [dict(item) for item in normalized_blobs])
        out.setdefault("bucketPaths", [dict(item) for item in normalized_blobs])
        out.setdefault("blobCount", len(normalized_blobs))
        out.setdefault("bucketCount", len(normalized_blobs))
    out.setdefault("blobPrefix", "finance-data/")
    out.setdefault("dataPrefix", "finance-data")
    return out


__all__ = [
    "create_bronze_alpha26_manifest",
    "create_bronze_finance_manifest",
    "load_latest_bronze_alpha26_manifest",
    "load_latest_bronze_finance_manifest",
    "manifest_blobs",
    "mdc",
    "resolve_active_bronze_alpha26_prefix",
    "silver_finance_ack_exists",
    "write_silver_finance_ack",
]

