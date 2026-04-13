"""
Data quality: schema fingerprinting, drift detection, non-fatal issue logging.

Ingestion continues unless a **critical** failure occurs elsewhere; DQ issues are
persisted to ``data_quality_issues`` with ``severity`` = warning/info by default.
"""

from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping, Sequence, Set
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import DataQualityIssue, SchemaFingerprint
from app.schemas.trade_flow import TradeFlowRecord

logger = logging.getLogger(__name__)

ISSUE_RAW_MISSING_FIELD = "raw_missing_field"
ISSUE_RAW_UNEXPECTED_COLUMN = "raw_unexpected_column"
ISSUE_SCHEMA_FINGERPRINT_CHANGED = "schema_fingerprint_changed"
ISSUE_NORMALIZED_NULL_QUANTITY = "normalized_null_quantity"

SEVERITY_WARNING = "warning"
SEVERITY_INFO = "info"


def fingerprint_raw_column_names(rows: Sequence[Mapping[str, Any]]) -> tuple[str, list[str]]:
    """
    Stable fingerprint of the union of keys across rows (order-independent).

    Returns ``(sha256_hex, sorted_column_names)``.
    """
    keys: set[str] = set()
    for row in rows:
        keys.update(str(k) for k in row.keys())
    sorted_keys = sorted(keys)
    payload = "|".join(sorted_keys).encode("utf-8")
    digest = hashlib.sha256(payload).hexdigest()
    return digest, sorted_keys


def _log_issue(
    session: Session,
    *,
    source: str,
    dataset: str,
    issue_type: str,
    severity: str,
    message: str,
    detail: dict[str, Any] | None,
    ingestion_run_id: int | None,
) -> None:
    row = DataQualityIssue(
        source=source,
        dataset=dataset,
        issue_type=issue_type,
        severity=severity,
        message=message,
        detail=detail,
        ingestion_run_id=ingestion_run_id,
        created_at=datetime.now(timezone.utc),
    )
    session.add(row)
    logger.warning("DQ [%s] %s: %s", issue_type, severity, message)


def evaluate_raw_schema(
    session: Session,
    *,
    source: str,
    dataset: str,
    rows: Sequence[Mapping[str, Any]],
    expected_columns: Set[str] | None,
    ingestion_run_id: int | None,
) -> None:
    """
    Fingerprint raw tabular rows, detect schema change vs last run, and optionally
    report missing / unexpected columns relative to ``expected_columns``.

    Never raises; does not roll back the caller transaction.
    """
    if not rows:
        return

    fp_hash, columns = fingerprint_raw_column_names(rows)

    prev = session.execute(
        select(SchemaFingerprint).where(
            SchemaFingerprint.source == source,
            SchemaFingerprint.dataset == dataset,
        )
    ).scalar_one_or_none()

    if prev is not None and prev.fingerprint_hash != fp_hash:
        _log_issue(
            session,
            source=source,
            dataset=dataset,
            issue_type=ISSUE_SCHEMA_FINGERPRINT_CHANGED,
            severity=SEVERITY_WARNING,
            message="Raw column set changed from previous ingestion run.",
            detail={
                "previous_fingerprint": prev.fingerprint_hash,
                "current_fingerprint": fp_hash,
                "previous_columns": prev.columns_json,
                "current_columns": columns,
            },
            ingestion_run_id=ingestion_run_id,
        )

    if prev is None:
        session.add(
            SchemaFingerprint(
                source=source,
                dataset=dataset,
                fingerprint_hash=fp_hash,
                columns_json=columns,
                updated_at=datetime.now(timezone.utc),
            )
        )
    else:
        prev.fingerprint_hash = fp_hash
        prev.columns_json = columns
        prev.updated_at = datetime.now(timezone.utc)

    if expected_columns:
        union_keys: set[str] = set()
        for row in rows:
            union_keys.update(str(k) for k in row.keys())
        unexpected = sorted(union_keys - expected_columns)
        if unexpected:
            _log_issue(
                session,
                source=source,
                dataset=dataset,
                issue_type=ISSUE_RAW_UNEXPECTED_COLUMN,
                severity=SEVERITY_WARNING,
                message=f"Unexpected column(s) not in expected set: {unexpected}.",
                detail={"unexpected_columns": unexpected, "expected_columns": sorted(expected_columns)},
                ingestion_run_id=ingestion_run_id,
            )

        for col in sorted(expected_columns):
            missing_n = 0
            for row in rows:
                if col not in row or row[col] is None or str(row[col]).strip() == "":
                    missing_n += 1
            if missing_n > 0:
                _log_issue(
                    session,
                    source=source,
                    dataset=dataset,
                    issue_type=ISSUE_RAW_MISSING_FIELD,
                    severity=SEVERITY_WARNING,
                    message=f"Expected field {col!r} missing or empty in {missing_n} of {len(rows)} row(s).",
                    detail={"field": col, "missing_row_count": missing_n, "row_count": len(rows)},
                    ingestion_run_id=ingestion_run_id,
                )


def evaluate_normalized_records(
    session: Session,
    *,
    source: str,
    dataset: str,
    records: Sequence[TradeFlowRecord],
    ingestion_run_id: int | None,
) -> None:
    """Light checks on normalized rows (e.g. null quantity)."""
    if not records:
        return
    null_qty = sum(1 for r in records if r.quantity is None)
    if null_qty:
        _log_issue(
            session,
            source=source,
            dataset=dataset,
            issue_type=ISSUE_NORMALIZED_NULL_QUANTITY,
            severity=SEVERITY_INFO,
            message=f"{null_qty} of {len(records)} normalized row(s) have null quantity.",
            detail={"null_quantity_count": null_qty, "row_count": len(records)},
            ingestion_run_id=ingestion_run_id,
        )
