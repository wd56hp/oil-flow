"""Tests for read-only verification HTTP API."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from app.models import TradeFlow


def _add_trade_flow(
    session,
    *,
    source: str,
    dataset: str = "crude-oil-imports",
    period_date: date | None = None,
    partner: str = "AG",
    commodity: str = "crude_oil:LSW",
    flow: str = "import",
) -> None:
    session.add(
        TradeFlow(
            source=source,
            dataset=dataset,
            period_date=period_date or date(2025, 6, 1),
            reporter_country="US",
            partner_country=partner,
            commodity=commodity,
            flow_direction=flow,
            quantity=Decimal("100"),
            quantity_unit="thousand barrels",
            observed_at=datetime(2025, 7, 1, 12, 0, 0, tzinfo=timezone.utc),
        )
    )
    session.commit()


class TestTradeFlowsFilters:
    def test_filter_by_source_returns_only_matching_rows(self, client, db_session):
        _add_trade_flow(db_session, source="eia")
        _add_trade_flow(db_session, source="iea", partner="DE")

        r = client.get("/trade-flows", params={"source": "eia"})

        assert r.status_code == 200
        body = r.json()
        assert body["total"] == 1
        assert len(body["items"]) == 1
        assert body["items"][0]["source"] == "eia"

    def test_period_date_range_inclusive(self, client, db_session):
        _add_trade_flow(db_session, source="eia", period_date=date(2024, 1, 1))
        _add_trade_flow(db_session, source="eia", period_date=date(2024, 6, 1))
        _add_trade_flow(db_session, source="eia", period_date=date(2025, 1, 1))

        r = client.get(
            "/trade-flows",
            params={
                "period_from": "2024-03-01",
                "period_to": "2024-12-31",
                "source": "eia",
            },
        )

        assert r.status_code == 200
        assert r.json()["total"] == 1
        assert r.json()["items"][0]["period_date"] == "2024-06-01"


class TestTradeFlowsCsvExport:
    def test_export_csv_returns_header_and_rows(self, client, db_session):
        _add_trade_flow(db_session, source="eia")

        r = client.get("/trade-flows/export.csv", params={"source": "eia"})

        assert r.status_code == 200
        assert "text/csv" in r.headers.get("content-type", "")
        assert "attachment" in r.headers.get("content-disposition", "")

        lines = [ln for ln in r.text.strip().splitlines() if ln]
        assert lines[0].startswith("id,source,dataset,period_date")
        assert "eia" in lines[1]
        assert "thousand barrels" in lines[1]


class TestRevisionsAndRuns:
    def test_revisions_list_empty(self, client, db_session):
        r = client.get("/revisions")
        assert r.status_code == 200
        assert r.json()["total"] == 0

    def test_ingestion_runs_empty(self, client, db_session):
        r = client.get("/ingestion-runs")
        assert r.status_code == 200
        assert r.json()["total"] == 0
