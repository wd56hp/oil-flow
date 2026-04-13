"""IEA connector: table normalization and compatibility with TradeFlowRecord."""

from __future__ import annotations

from datetime import datetime, timezone

from app.connectors.iea.constants import DATASET_OIL_TRADE_TABLE, IEA_SOURCE
from app.connectors.iea.fetch import fetch_table_rows
from app.connectors.iea.tables import normalize_iea_table_row, normalize_iea_table_rows


def test_normalize_row_produces_trade_flow_record_for_ingestion():
    row = {
        "period": "2024-03-01",
        "reporter_country": "US",
        "partner_country": "NO",
        "commodity": "crude_oil",
        "flow_direction": "import",
        "quantity": "1000",
        "quantity_unit": "kbbl",
    }
    rec = normalize_iea_table_row(row, dataset=DATASET_OIL_TRADE_TABLE)
    assert rec is not None
    assert rec.source == IEA_SOURCE
    assert rec.dataset == DATASET_OIL_TRADE_TABLE
    assert rec.reporter_country == "US"
    assert rec.partner_country == "NO"
    assert rec.flow_direction == "import"


def test_fetch_without_base_url_returns_empty():
    assert fetch_table_rows("/any") == []


def test_normalize_many_filters_invalid():
    rows = [
        {"period": "2024-01-01", "reporter_country": "US", "partner": "DE",
         "commodity": "x", "flow_direction": "export", "quantity": "1"},
        {"incomplete": True},
    ]
    out = normalize_iea_table_rows(rows, dataset=DATASET_OIL_TRADE_TABLE)
    assert len(out) == 1


def test_normalized_rows_compatible_with_ingestion_engine(db_session):
    from app.services.ingestion_engine import ingest_trade_flow_records

    rows = normalize_iea_table_rows(
        [
            {
                "period": "2024-01-01",
                "reporter_country": "US",
                "partner_country": "GB",
                "commodity": "crude_oil",
                "flow_direction": "import",
                "quantity": "42",
                "quantity_unit": "kbbl",
            }
        ],
        dataset=DATASET_OIL_TRADE_TABLE,
    )
    stats = ingest_trade_flow_records(db_session, rows, source_hint="iea")
    assert stats.inserted == 1
    assert stats.revised == 0


def test_observed_at_passed_through():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rec = normalize_iea_table_row(
        {
            "period_date": "2024-06-01",
            "reporter_country": "US",
            "partner_country": "CA",
            "commodity": "c",
            "flow_direction": "import",
        },
        dataset=DATASET_OIL_TRADE_TABLE,
        observed_at=ts,
    )
    assert rec is not None
    assert rec.observed_at == ts
