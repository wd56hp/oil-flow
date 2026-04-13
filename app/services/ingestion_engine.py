"""
Reusable ingestion: apply normalized :class:`TradeFlowRecord` rows idempotently.

Decisions
---------

**Business key** — Uniqueness is enforced on
``(source, dataset, period_date, reporter_country, partner_country, commodity,
flow_direction)``. Lookups use the same columns.

**What counts as “the value”** — Only **measures** participate in change detection:
``quantity`` and ``quantity_unit``. Same key + same measures → *unchanged* (skip).

**Lineage fields (intentional behavior)** — Fields such as ``eia_origin_id``,
``eia_destination_id``, and ``eia_grade_id`` do **not** by themselves trigger insert,
revision, or in-place update. When a row is *unchanged* (measures match), the engine
**skips** the row entirely, so the canonical ``trade_flows`` row **may keep older
lineage values** if the upstream API changed destination/port metadata without
changing quantity. That is a deliberate trade-off: revisions and canonical updates
run only when **measures** change. On **insert** and **measure revision**, lineage
is written from the incoming ``TradeFlowRecord``.

**Where rows live** — This is **canonical current row + revision history**, not a
single append-only facts table: ``trade_flows`` holds **one row per business key**
(the latest measures we trust). When measures change, we append to
``trade_flow_revisions`` (previous measures) and **update** that canonical row.

**Atomicity** — The whole batch runs in a single database transaction. On any
exception, the transaction rolls back (including the ``ingestion_runs`` row).
``failed_count`` is reserved for future per-record failure handling; on success it is 0.

**Cross-source reuse** — Any connector that outputs :class:`~app.schemas.trade_flow.TradeFlowRecord`
can call :func:`ingest_trade_flow_records`; only the record contents vary.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Sequence

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import IngestionRun, TradeFlow, TradeFlowRevision
from app.schemas.trade_flow import TradeFlowRecord

STATUS_RUNNING = "running"
STATUS_COMPLETED = "completed"


@dataclass(frozen=True)
class IngestionStats:
    run_id: int
    inserted: int
    revised: int
    unchanged: int
    failed: int


def _decimal_equal(a: Decimal | None, b: Decimal | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return Decimal(str(a)) == Decimal(str(b))


def measures_unchanged(row: TradeFlow, rec: TradeFlowRecord) -> bool:
    """True if measure fields match the incoming record."""
    if not _decimal_equal(row.quantity, rec.quantity):
        return False
    u1 = (row.quantity_unit or "").strip()
    u2 = (rec.quantity_unit or "").strip()
    return u1 == u2


def _trade_flow_from_record(rec: TradeFlowRecord) -> TradeFlow:
    return TradeFlow(
        source=rec.source,
        dataset=rec.dataset,
        period_date=rec.period_date,
        reporter_country=rec.reporter_country,
        partner_country=rec.partner_country,
        commodity=rec.commodity,
        flow_direction=rec.flow_direction,
        quantity=rec.quantity,
        quantity_unit=rec.quantity_unit,
        observed_at=rec.observed_at,
        eia_origin_id=rec.eia_origin_id,
        eia_destination_id=rec.eia_destination_id,
        eia_grade_id=rec.eia_grade_id,
    )


def _apply_canonical_row(row: TradeFlow, rec: TradeFlowRecord) -> None:
    """Update canonical measures + observation time + optional lineage."""
    row.quantity = rec.quantity
    row.quantity_unit = rec.quantity_unit
    row.observed_at = rec.observed_at
    row.eia_origin_id = rec.eia_origin_id
    row.eia_destination_id = rec.eia_destination_id
    row.eia_grade_id = rec.eia_grade_id


def ingest_trade_flow_records(
    session: Session,
    records: Sequence[TradeFlowRecord],
    *,
    source_hint: str | None = None,
    commit: bool = True,
) -> IngestionStats:
    """
    Process normalized records: insert new keys, skip identical measures, revise changed measures.

    :param source_hint: Optional operator-facing label (e.g. ``"eia"``).
    :param commit: If ``False``, only ``flush`` (for embedding in a larger transaction).
    """
    started = datetime.now(timezone.utc)
    run = IngestionRun(
        status=STATUS_RUNNING,
        source_hint=source_hint,
        inserted_count=0,
        revised_count=0,
        unchanged_count=0,
        failed_count=0,
        started_at=started,
    )
    session.add(run)
    session.flush()

    inserted = revised = unchanged = 0

    for rec in records:
        stmt = select(TradeFlow).where(
            TradeFlow.source == rec.source,
            TradeFlow.dataset == rec.dataset,
            TradeFlow.period_date == rec.period_date,
            TradeFlow.reporter_country == rec.reporter_country,
            TradeFlow.partner_country == rec.partner_country,
            TradeFlow.commodity == rec.commodity,
            TradeFlow.flow_direction == rec.flow_direction,
        )
        existing = session.execute(stmt).scalar_one_or_none()

        if existing is None:
            session.add(_trade_flow_from_record(rec))
            inserted += 1
            session.flush()
            continue

        if measures_unchanged(existing, rec):
            unchanged += 1
            continue

        session.add(
            TradeFlowRevision(
                trade_flow_id=existing.id,
                ingestion_run_id=run.id,
                previous_quantity=existing.quantity,
                previous_quantity_unit=existing.quantity_unit,
                previous_observed_at=existing.observed_at,
                new_quantity=rec.quantity,
                new_quantity_unit=rec.quantity_unit,
                new_observed_at=rec.observed_at,
            )
        )
        _apply_canonical_row(existing, rec)
        revised += 1
        session.flush()

    finished = datetime.now(timezone.utc)
    run.status = STATUS_COMPLETED
    run.finished_at = finished
    run.inserted_count = inserted
    run.revised_count = revised
    run.unchanged_count = unchanged
    run.failed_count = 0

    if commit:
        session.commit()
    else:
        session.flush()

    return IngestionStats(
        run_id=run.id,
        inserted=inserted,
        revised=revised,
        unchanged=unchanged,
        failed=0,
    )
