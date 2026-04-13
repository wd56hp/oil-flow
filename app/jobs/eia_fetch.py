"""CLI/cron job: fetch EIA crude imports and ingest (idempotent via ingestion engine)."""

from __future__ import annotations

import json
import logging

from app.connectors.eia.aggregation import aggregate_eia_crude_imports_for_canonical
from app.connectors.eia.crude_imports import (
    CRUDE_IMPORTS_EXPECTED_RAW_COLUMNS,
    CrudeImportNormalizeSummary,
    fetch_crude_oil_imports,
    normalize_crude_import_rows,
)
from app.connectors.eia.diagnostics import duplicate_business_key_report
from app.core.database import SessionLocal
from app.jobs.common import JobOutcome
from app.services.ingestion_engine import ingest_trade_flow_records

logger = logging.getLogger(__name__)


def run_fetch_eia_data(
    *,
    start: str | None = None,
    end: str | None = None,
    paginate: bool = True,
    dry_run: bool = False,
    country_origin_only: bool = True,
) -> JobOutcome:
    """
    Fetch EIA ``crude-oil-imports``, normalize, aggregate to canonical key, ingest.

    ``dry_run``: fetch + normalize + aggregate + duplicate-key report only; no DB writes.

    ``country_origin_only``: when True, drop non-``CTY`` / non-``CTY_XX`` origin rows
    (WORLD, REG_*, OPN_*, etc.) before aggregation.
    """
    try:
        raw_rows = fetch_crude_oil_imports(
            start=start,
            end=end,
            paginate=paginate,
        )
        norm_summary = CrudeImportNormalizeSummary()
        pre_agg = normalize_crude_import_rows(
            raw_rows,
            country_origin_only=country_origin_only,
            summary=norm_summary,
        )
        records, agg_summary = aggregate_eia_crude_imports_for_canonical(pre_agg)
        dup_report = duplicate_business_key_report(records, top_n=25)

        if dry_run:
            details = {
                "connector": "eia",
                "dataset": "crude-oil-imports",
                "dry_run": True,
                "country_origin_only": country_origin_only,
                "raw_row_count": len(raw_rows),
                "normalize_summary": norm_summary.as_dict(),
                "aggregation_summary": agg_summary.as_dict(),
                "duplicate_business_keys": dup_report,
            }
            logger.info("fetch_eia_data dry_run %s", json.dumps(details, default=str))
            return JobOutcome(
                ok=True,
                message="EIA dry run (no ingest)",
                details=details,
            )

        session = SessionLocal()
        try:
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
                "dry_run": False,
                "country_origin_only": country_origin_only,
                "raw_row_count": len(raw_rows),
                "normalize_summary": norm_summary.as_dict(),
                "aggregation_summary": agg_summary.as_dict(),
                "duplicate_business_keys": dup_report,
                "normalized_count": len(records),
                "ingestion_run_id": stats.run_id,
                "inserted": stats.inserted,
                "revised": stats.revised,
                "unchanged": stats.unchanged,
                "failed": stats.failed,
            }
            logger.info(
                "fetch_eia_data completed run_id=%s inserted=%s revised=%s unchanged=%s "
                "aggregated=%s unique_keys=%s groups_collapsed=%s",
                stats.run_id,
                stats.inserted,
                stats.revised,
                stats.unchanged,
                len(records),
                dup_report.get("unique_business_keys"),
                agg_summary.groups_collapsed,
            )
            return JobOutcome(
                ok=True,
                message="EIA fetch and ingestion completed",
                details=details,
            )
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    except Exception as exc:
        logger.exception("fetch_eia_data failed: %s", exc)
        return JobOutcome(
            ok=False,
            message=str(exc),
            details={"connector": "eia", "error": str(exc)},
        )
