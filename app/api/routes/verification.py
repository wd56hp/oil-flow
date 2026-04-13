"""Read-only verification API (table-first inspection)."""

from __future__ import annotations

import csv
from datetime import date
from io import StringIO

from fastapi import APIRouter, Depends, Query, Response
from sqlalchemy import Select, and_, func, select
from sqlalchemy.orm import Session

from app.core.database import get_db
from app.models import IngestionRun, TradeFlow, TradeFlowRevision
from app.schemas.inspection import (
    IngestionRunListResponse,
    IngestionRunRead,
    TradeFlowListResponse,
    TradeFlowRead,
    TradeFlowRevisionListResponse,
    TradeFlowRevisionRead,
)

router = APIRouter(tags=["verification"])

MAX_PAGE = 500
CSV_MAX_ROWS = 10_000


def _paginate(stmt: Select, session: Session, *, limit: int, offset: int) -> tuple[list, int]:
    count_stmt = select(func.count()).select_from(stmt.subquery())
    total = session.scalar(count_stmt)
    if total is None:
        total = 0
    rows = session.scalars(stmt.limit(limit).offset(offset)).all()
    return rows, total


def _trade_flow_filters(
    *,
    source: str | None = None,
    dataset: str | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
    reporter_country: str | None = None,
    partner_country: str | None = None,
    commodity: str | None = None,
    flow_direction: str | None = None,
) -> list:
    clauses: list = []
    if source is not None:
        clauses.append(TradeFlow.source == source)
    if dataset is not None:
        clauses.append(TradeFlow.dataset == dataset)
    if period_from is not None:
        clauses.append(TradeFlow.period_date >= period_from)
    if period_to is not None:
        clauses.append(TradeFlow.period_date <= period_to)
    if reporter_country is not None:
        clauses.append(TradeFlow.reporter_country == reporter_country)
    if partner_country is not None:
        clauses.append(TradeFlow.partner_country == partner_country)
    if commodity is not None:
        clauses.append(TradeFlow.commodity == commodity)
    if flow_direction is not None:
        clauses.append(TradeFlow.flow_direction == flow_direction)
    return clauses


def _trade_flows_select(
    *,
    source: str | None = None,
    dataset: str | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
    reporter_country: str | None = None,
    partner_country: str | None = None,
    commodity: str | None = None,
    flow_direction: str | None = None,
) -> Select:
    clauses = _trade_flow_filters(
        source=source,
        dataset=dataset,
        period_from=period_from,
        period_to=period_to,
        reporter_country=reporter_country,
        partner_country=partner_country,
        commodity=commodity,
        flow_direction=flow_direction,
    )
    stmt = select(TradeFlow).order_by(TradeFlow.id.desc())
    if clauses:
        stmt = stmt.where(and_(*clauses))
    return stmt


TRADE_FLOW_CSV_COLUMNS = [
    "id",
    "source",
    "dataset",
    "period_date",
    "reporter_country",
    "partner_country",
    "commodity",
    "flow_direction",
    "quantity",
    "quantity_unit",
    "observed_at",
    "eia_origin_id",
    "eia_destination_id",
    "eia_grade_id",
]


def _trade_flow_row_csv(r: TradeFlow) -> list[str]:
    return [
        str(r.id),
        r.source,
        r.dataset,
        r.period_date.isoformat(),
        r.reporter_country,
        r.partner_country,
        r.commodity,
        r.flow_direction,
        "" if r.quantity is None else str(r.quantity),
        r.quantity_unit or "",
        r.observed_at.isoformat(),
        r.eia_origin_id or "",
        r.eia_destination_id or "",
        r.eia_grade_id or "",
    ]


@router.get("/trade-flows", response_model=TradeFlowListResponse)
def list_trade_flows(
    session: Session = Depends(get_db),
    limit: int = Query(100, ge=1, le=MAX_PAGE),
    offset: int = Query(0, ge=0),
    source: str | None = None,
    dataset: str | None = None,
    period_from: date | None = Query(None, description="Inclusive lower bound for period_date"),
    period_to: date | None = Query(None, description="Inclusive upper bound for period_date"),
    reporter_country: str | None = None,
    partner_country: str | None = None,
    commodity: str | None = None,
    flow_direction: str | None = None,
) -> TradeFlowListResponse:
    stmt = _trade_flows_select(
        source=source,
        dataset=dataset,
        period_from=period_from,
        period_to=period_to,
        reporter_country=reporter_country,
        partner_country=partner_country,
        commodity=commodity,
        flow_direction=flow_direction,
    )
    rows, total = _paginate(stmt, session, limit=limit, offset=offset)
    return TradeFlowListResponse(
        items=[TradeFlowRead.model_validate(r) for r in rows],
        limit=limit,
        offset=offset,
        total=total,
    )


@router.get("/trade-flows/export.csv")
def export_trade_flows_csv(
    session: Session = Depends(get_db),
    limit: int = Query(5_000, ge=1, le=CSV_MAX_ROWS),
    offset: int = Query(0, ge=0),
    source: str | None = None,
    dataset: str | None = None,
    period_from: date | None = None,
    period_to: date | None = None,
    reporter_country: str | None = None,
    partner_country: str | None = None,
    commodity: str | None = None,
    flow_direction: str | None = None,
) -> Response:
    """Same filters as GET /trade-flows; returns CSV (bounded by limit/offset)."""
    stmt = _trade_flows_select(
        source=source,
        dataset=dataset,
        period_from=period_from,
        period_to=period_to,
        reporter_country=reporter_country,
        partner_country=partner_country,
        commodity=commodity,
        flow_direction=flow_direction,
    )
    stmt = stmt.limit(limit).offset(offset)
    rows = session.scalars(stmt).all()

    buf = StringIO()
    writer = csv.writer(buf)
    writer.writerow(TRADE_FLOW_CSV_COLUMNS)
    for r in rows:
        writer.writerow(_trade_flow_row_csv(r))

    return Response(
        content=buf.getvalue(),
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="trade_flows.csv"',
        },
    )


@router.get("/revisions", response_model=TradeFlowRevisionListResponse)
def list_revisions(
    session: Session = Depends(get_db),
    trade_flow_id: int | None = Query(None, description="Filter by canonical trade_flow id"),
    limit: int = Query(100, ge=1, le=MAX_PAGE),
    offset: int = Query(0, ge=0),
) -> TradeFlowRevisionListResponse:
    stmt = select(TradeFlowRevision).order_by(TradeFlowRevision.id.desc())
    if trade_flow_id is not None:
        stmt = stmt.where(TradeFlowRevision.trade_flow_id == trade_flow_id)
    rows, total = _paginate(stmt, session, limit=limit, offset=offset)
    return TradeFlowRevisionListResponse(
        items=[TradeFlowRevisionRead.model_validate(r) for r in rows],
        limit=limit,
        offset=offset,
        total=total,
    )


@router.get("/ingestion-runs", response_model=IngestionRunListResponse)
def list_ingestion_runs(
    session: Session = Depends(get_db),
    limit: int = Query(50, ge=1, le=MAX_PAGE),
    offset: int = Query(0, ge=0),
) -> IngestionRunListResponse:
    stmt = select(IngestionRun).order_by(IngestionRun.id.desc())
    rows, total = _paginate(stmt, session, limit=limit, offset=offset)
    return IngestionRunListResponse(
        items=[IngestionRunRead.model_validate(r) for r in rows],
        limit=limit,
        offset=offset,
        total=total,
    )
