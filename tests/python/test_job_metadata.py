from __future__ import annotations

import pytest

from asset_allocation_runtime_common.job_metadata import (
    catalog_job_names,
    expected_job_metadata,
    resolve_job_metadata,
    validate_job_metadata_tags,
)


def test_resolve_job_metadata_uses_valid_tags() -> None:
    resolution = resolve_job_metadata(
        "gold-regime-job",
        {
            "job-category": "strategy-compute",
            "job-key": "regime",
            "job-role": "publish",
            "trigger-owner": "schedule",
        },
    )

    assert resolution.errors == ()
    assert resolution.metadata.jobCategory == "strategy-compute"
    assert resolution.metadata.jobKey == "regime"
    assert resolution.metadata.metadataSource == "tags"
    assert resolution.metadata.metadataStatus == "valid"


def test_resolve_job_metadata_marks_known_missing_tags_as_fallback() -> None:
    resolution = resolve_job_metadata("backtests-job", {})

    assert resolution.metadata.jobCategory == "strategy-compute"
    assert resolution.metadata.jobKey == "backtests"
    assert resolution.metadata.metadataSource == "legacy-catalog"
    assert resolution.metadata.metadataStatus == "fallback"


def test_resolve_job_metadata_marks_invalid_tags_without_silent_success() -> None:
    resolution = resolve_job_metadata(
        "platinum-rankings-job",
        {
            "job-category": "portfolio-build",
            "job-key": "rankings",
            "job-role": "materialize",
            "trigger-owner": "control-plane",
        },
    )

    assert resolution.metadata.jobCategory == "strategy-compute"
    assert resolution.metadata.jobKey == "rankings"
    assert resolution.metadata.metadataSource == "tags"
    assert resolution.metadata.metadataStatus == "invalid"
    assert resolution.errors == ("job-category is missing or invalid",)


def test_validate_job_metadata_tags_blocks_missing_values() -> None:
    with pytest.raises(ValueError, match="trigger-owner"):
        validate_job_metadata_tags(
            "results-reconcile-job",
            {
                "job-category": "operational-support",
                "job-key": "results-reconcile",
                "job-role": "reconcile",
            },
        )


def test_resolve_job_metadata_rejects_valid_looking_tags_for_unknown_jobs() -> None:
    resolution = resolve_job_metadata(
        "experimental-job",
        {
            "job-category": "strategy-compute",
            "job-key": "regime",
            "job-role": "publish",
            "trigger-owner": "schedule",
            "owner": "not-returned",
            "cost-center": "not-returned",
        },
    )

    assert resolution.metadata.jobCategory == "operational-support"
    assert resolution.metadata.jobKey == "experimental-job"
    assert resolution.metadata.metadataSource == "tags"
    assert resolution.metadata.metadataStatus == "invalid"
    assert resolution.errors == ("job metadata tags do not have a legacy catalog entry",)


def test_validate_job_metadata_tags_blocks_catalog_drift() -> None:
    resolution = resolve_job_metadata(
        "backtests-reconcile-job",
        {
            "job-category": "strategy-compute",
            "job-key": "backtests",
            "job-role": "reconcile",
            "trigger-owner": "reconciler",
        },
    )
    assert resolution.metadata.jobCategory == "operational-support"
    assert resolution.metadata.metadataStatus == "invalid"
    assert resolution.errors == ("tag values do not match legacy catalog",)

    with pytest.raises(ValueError, match="catalog"):
        validate_job_metadata_tags(
            "backtests-reconcile-job",
            {
                "job-category": "strategy-compute",
                "job-key": "backtests",
                "job-role": "reconcile",
                "trigger-owner": "reconciler",
            },
        )


def test_catalog_includes_required_strategy_compute_and_reconcile_jobs() -> None:
    assert "gold-regime-job" in catalog_job_names()
    assert expected_job_metadata("gold-regime-job").jobCategory == "strategy-compute"  # type: ignore[union-attr]
    assert expected_job_metadata("platinum-rankings-job").jobCategory == "strategy-compute"  # type: ignore[union-attr]
    assert expected_job_metadata("backtests-job").jobCategory == "strategy-compute"  # type: ignore[union-attr]
    assert expected_job_metadata("backtests-reconcile-job").jobCategory == "operational-support"  # type: ignore[union-attr]
    assert expected_job_metadata("results-reconcile-job").jobCategory == "operational-support"  # type: ignore[union-attr]
