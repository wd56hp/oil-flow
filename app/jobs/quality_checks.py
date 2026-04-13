"""Read-only reporting job: recent DQ issues and ingestion health (safe for cron)."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select

from app.core.database import SessionLocal
from app.jobs.common import JobOutcome
from app.models import DataQualityIssue, IngestionRun

logger = logging.getLogger(__name__)


def run_quality_checks(*, since_hours: int = 24) -> JobOutcome:
    """
    Summarize ``data_quality_issues`` and recent ``ingestion_runs`` since ``since_hours``.

    Does not mutate data; suitable for scheduled monitoring on Railway/Render.
    """
    session = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)

        dq_stmt = (
            select(
                DataQualityIssue.issue_type,
                DataQualityIssue.severity,
                func.count(DataQualityIssue.id),
            )
            .where(DataQualityIssue.created_at >= cutoff)
            .group_by(DataQualityIssue.issue_type, DataQualityIssue.severity)
        )
        dq_rows = session.execute(dq_stmt).all()
        dq_breakdown = [
            {"issue_type": r[0], "severity": r[1], "count": int(r[2])}
            for r in dq_rows
        ]

        runs_total = session.scalar(
            select(func.count())
            .select_from(IngestionRun)
            .where(IngestionRun.started_at >= cutoff)
        )
        failed_runs = session.scalars(
            select(IngestionRun).where(
                IngestionRun.started_at >= cutoff,
                IngestionRun.status == "failed",
            )
        ).all()

        details = {
            "since_hours": since_hours,
            "cutoff_utc": cutoff.isoformat(),
            "data_quality_issue_groups": dq_breakdown,
            "ingestion_runs_in_window": int(runs_total or 0),
            "ingestion_runs_failed_in_window": len(failed_runs),
            "failed_run_ids": [r.id for r in failed_runs],
        }
        logger.info(
            "run_quality_checks: dq_groups=%s runs=%s failed_runs=%s",
            len(dq_breakdown),
            runs_total,
            len(failed_runs),
        )
        return JobOutcome(
            ok=True,
            message="Quality summary completed",
            details=details,
        )
    except Exception as exc:
        logger.exception("run_quality_checks failed: %s", exc)
        return JobOutcome(
            ok=False,
            message=str(exc),
            details={"error": str(exc)},
        )
    finally:
        session.close()
