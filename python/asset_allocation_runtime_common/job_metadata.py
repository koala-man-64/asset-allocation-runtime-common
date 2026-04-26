from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

from asset_allocation_contracts.job_metadata import RuntimeJobMetadata

JOB_METADATA_TAGS = {
    "category": "job-category",
    "key": "job-key",
    "role": "job-role",
    "trigger_owner": "trigger-owner",
}

VALID_JOB_CATEGORIES = frozenset({"data-pipeline", "strategy-compute", "operational-support"})
VALID_JOB_ROLES = frozenset(
    {
        "aggregate",
        "backfill",
        "cleanup",
        "execute",
        "load",
        "materialize",
        "monitor",
        "publish",
        "reconcile",
        "refresh",
        "support",
        "transform",
    }
)
VALID_TRIGGER_OWNERS = frozenset({"schedule", "control-plane", "operator", "pipeline-chain", "reconciler"})


@dataclass(frozen=True)
class JobMetadataResolution:
    metadata: RuntimeJobMetadata
    errors: tuple[str, ...] = ()


_CATALOG: dict[str, tuple[str, str, str, str]] = {
    "backtests-job": ("strategy-compute", "backtests", "execute", "control-plane"),
    "backtests-reconcile-job": ("operational-support", "backtests", "reconcile", "reconciler"),
    "bronze-earnings-job": ("data-pipeline", "earnings", "load", "schedule"),
    "bronze-economic-catalyst-job": ("data-pipeline", "economic-catalyst", "load", "schedule"),
    "bronze-finance-job": ("data-pipeline", "finance", "load", "schedule"),
    "bronze-market-job": ("data-pipeline", "market", "load", "schedule"),
    "bronze-price-target-job": ("data-pipeline", "price-target", "load", "schedule"),
    "bronze-quiver-backfill-job": ("data-pipeline", "quiver", "backfill", "operator"),
    "bronze-quiver-data-job": ("data-pipeline", "quiver", "load", "schedule"),
    "gold-earnings-job": ("data-pipeline", "earnings", "publish", "pipeline-chain"),
    "gold-economic-catalyst-job": ("data-pipeline", "economic-catalyst", "publish", "pipeline-chain"),
    "gold-finance-job": ("data-pipeline", "finance", "publish", "pipeline-chain"),
    "gold-market-job": ("data-pipeline", "market", "publish", "pipeline-chain"),
    "gold-price-target-job": ("data-pipeline", "price-target", "publish", "pipeline-chain"),
    "gold-quiver-data-job": ("data-pipeline", "quiver", "publish", "pipeline-chain"),
    "gold-regime-job": ("strategy-compute", "regime", "publish", "schedule"),
    "intraday-market-refresh-job": ("operational-support", "intraday-refresh", "refresh", "schedule"),
    "intraday-monitor-job": ("operational-support", "intraday-monitor", "monitor", "schedule"),
    "platinum-rankings-job": ("strategy-compute", "rankings", "materialize", "control-plane"),
    "results-reconcile-job": ("operational-support", "results-reconcile", "reconcile", "reconciler"),
    "silver-earnings-job": ("data-pipeline", "earnings", "transform", "pipeline-chain"),
    "silver-economic-catalyst-job": ("data-pipeline", "economic-catalyst", "transform", "pipeline-chain"),
    "silver-finance-job": ("data-pipeline", "finance", "transform", "pipeline-chain"),
    "silver-market-job": ("data-pipeline", "market", "transform", "pipeline-chain"),
    "silver-price-target-job": ("data-pipeline", "price-target", "transform", "pipeline-chain"),
    "silver-quiver-data-job": ("data-pipeline", "quiver", "transform", "pipeline-chain"),
    "symbol-cleanup-job": ("operational-support", "symbol-cleanup", "cleanup", "schedule"),
}


def catalog_job_names() -> tuple[str, ...]:
    return tuple(sorted(_CATALOG))


def expected_job_metadata(job_name: str) -> RuntimeJobMetadata | None:
    values = _CATALOG.get(_clean(job_name))
    if values is None:
        return None
    return _metadata(*values, metadata_source="legacy-catalog", metadata_status="fallback")


def resolve_job_metadata(job_name: str, tags: Mapping[str, object] | None = None) -> JobMetadataResolution:
    clean_name = _clean(job_name)
    raw_tags = tags or {}
    parsed = _extract_tag_values(raw_tags)
    errors = _validate_values(parsed)
    catalog_values = _CATALOG.get(clean_name)

    if not errors and all(parsed.values()):
        if catalog_values is None:
            fallback_key = clean_name or "unknown-job"
            return JobMetadataResolution(
                metadata=_metadata(
                    "operational-support",
                    fallback_key,
                    "support",
                    "operator",
                    metadata_source="tags",
                    metadata_status="invalid",
                ),
                errors=("job metadata tags do not have a legacy catalog entry",),
            )
        if parsed != _catalog_dict(catalog_values):
            return JobMetadataResolution(
                metadata=_metadata(*catalog_values, metadata_source="tags", metadata_status="invalid"),
                errors=("tag values do not match legacy catalog",),
            )
        return JobMetadataResolution(
            metadata=_metadata(
                parsed["jobCategory"],
                parsed["jobKey"],
                parsed["jobRole"],
                parsed["triggerOwner"],
                metadata_source="tags",
                metadata_status="valid",
            )
        )

    if catalog_values is not None:
        if not any(parsed.values()):
            return JobMetadataResolution(
                metadata=_metadata(*catalog_values, metadata_source="legacy-catalog", metadata_status="fallback"),
            )
        source = "tags"
        status = "invalid" if errors else "fallback"
        return JobMetadataResolution(
            metadata=_metadata(*catalog_values, metadata_source=source, metadata_status=status),
            errors=tuple(errors),
        )

    fallback_key = clean_name or "unknown-job"
    return JobMetadataResolution(
        metadata=_metadata(
            "operational-support",
            fallback_key,
            "support",
            "operator",
            metadata_source="unknown",
            metadata_status="invalid",
        ),
        errors=tuple(errors or ["job metadata tags are missing and no legacy catalog entry exists"]),
    )


def validate_job_metadata_tags(job_name: str, tags: Mapping[str, object]) -> RuntimeJobMetadata:
    resolution = resolve_job_metadata(job_name, tags)
    if resolution.metadata.metadataStatus != "valid" or resolution.errors:
        detail = "; ".join(resolution.errors) or "job metadata tags are incomplete"
        raise ValueError(f"Invalid metadata for {job_name}: {detail}")
    expected = expected_job_metadata(job_name)
    if expected is not None:
        mismatches = [
            field
            for field in ("jobCategory", "jobKey", "jobRole", "triggerOwner")
            if getattr(resolution.metadata, field) != getattr(expected, field)
        ]
        if mismatches:
            joined = ", ".join(mismatches)
            raise ValueError(f"Invalid metadata for {job_name}: tag values do not match catalog fields: {joined}")
    return resolution.metadata


def _extract_tag_values(tags: Mapping[str, object]) -> dict[str, str]:
    return {
        "jobCategory": _tag_value(tags, JOB_METADATA_TAGS["category"]),
        "jobKey": _tag_value(tags, JOB_METADATA_TAGS["key"]),
        "jobRole": _tag_value(tags, JOB_METADATA_TAGS["role"]),
        "triggerOwner": _tag_value(tags, JOB_METADATA_TAGS["trigger_owner"]),
    }


def _catalog_dict(values: tuple[str, str, str, str]) -> dict[str, str]:
    return {
        "jobCategory": values[0],
        "jobKey": values[1],
        "jobRole": values[2],
        "triggerOwner": values[3],
    }


def _validate_values(values: Mapping[str, str]) -> list[str]:
    errors: list[str] = []
    if values["jobCategory"] not in VALID_JOB_CATEGORIES:
        errors.append("job-category is missing or invalid")
    if not values["jobKey"]:
        errors.append("job-key is missing")
    if values["jobRole"] not in VALID_JOB_ROLES:
        errors.append("job-role is missing or invalid")
    if values["triggerOwner"] not in VALID_TRIGGER_OWNERS:
        errors.append("trigger-owner is missing or invalid")
    return errors


def _metadata(
    job_category: str,
    job_key: str,
    job_role: str,
    trigger_owner: str,
    *,
    metadata_source: str,
    metadata_status: str,
) -> RuntimeJobMetadata:
    return RuntimeJobMetadata(
        jobCategory=job_category,
        jobKey=job_key,
        jobRole=job_role,
        triggerOwner=trigger_owner,
        metadataSource=metadata_source,
        metadataStatus=metadata_status,
    )


def _tag_value(tags: Mapping[str, object], key: str) -> str:
    value = tags.get(key)
    return str(value).strip() if value is not None else ""


def _clean(value: str) -> str:
    return str(value or "").strip()
