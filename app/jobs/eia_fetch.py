"""CLI/cron job: fetch EIA crude imports and ingest (idempotent via ingestion engine)."""

from __future__ import annotations

import logging

from app.connectors.eia.crude_imports import (
    CRUDE_IMPORTS_EXPECTED_RAW_COLUMNS,
    fetch_crude_oil_imports,
    normalize_crude_import_rows,
)
from app.core.database import SessionLocal
from app.jobs.common import JobOutcome
from app.services.ingestion_engine import ingest_trade_flow_records

logger = logging.getLogger(__name__)


def run_fetch_eia_data(
    *,
    start: str | None = None,
    end: str | None = None,
    paginate: bool = True,
) -> JobOutcome:
    """
    Fetch EIA ``crude-oil-imports``, normalize, ingest with raw-schema DQ.

    Safe to run on a schedule: duplicate business keys collapse to insert/skip/revise only.
    """
    session = SessionLocal()
    try:
        raw_rows = fetch_crude_oil_imports(
            start=start,
            end=end,
            paginate=paginate,
        )
        records = normalize_crude_import_rows(raw_rows)
        stats = ingest_trade_flow_records(
            session,
            records,
            source_hint="eia",
            raw_rows=raw_rows,
            expected_raw_columns=CRUDE_IMPORTS_EXPECTED_RAW_COLUMNS,
        )
        details = {
            "connector": "eia",
            "dataset": "crude-oil-imports",
            "raw_row_count": len(raw_rows),
            "normalized_count": len(records),
            "ingestion_run_id": stats.run_id,
            "inserted": stats.inserted,
            "revised": stats.revised,
            "unchanged": stats.unchanged,
            "failed": stats.failed,
        }
        logger.info(
            "fetch_eia_data completed run_id=%s inserted=%s revised=%s unchanged=%s",
            stats.run_id,
            stats.inserted,
            stats.revised,
            stats.unchanged,
        )
        return JobOutcome(
            ok=True,
            message="EIA fetch and ingestion completed",
            details=details,
        )
    except Exception as exc:
        logger.exception("fetch_eia_data failed: %s", exc)
        session.rollback()
        return JobOutcome(
            ok=False,
            message=str(exc),
            details={"connector": "eia", "error": str(exc)},
        )
    finally:
        session.close()
