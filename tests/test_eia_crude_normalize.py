"""EIA crude-import normalization, filtering, and business-key diagnostics."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

from app.connectors.eia.aggregation import aggregate_eia_crude_imports_for_canonical
from app.connectors.eia.crude_imports import (
    CrudeImportNormalizeSummary,
    normalize_crude_import_rows,
)
from app.connectors.eia.diagnostics import duplicate_business_key_report, trade_flow_business_key
from app.schemas.trade_flow import TradeFlowRecord


def _row(**kwargs):
    base = {
        "period": "2025-06",
        "quantity": "100",
        "originType": "CTY",
        "originId": "CTY_AG",
        "destinationId": "PP_1",
        "gradeId": "LSW",
    }
    base.update(kwargs)
    return base


def test_country_origin_only_drops_world_and_regional():
    rows = [
        _row(originId="WORLD", originType="WRL"),
        _row(originId="REG_CA", originType="REG"),
        _row(),
    ]
    summary = CrudeImportNormalizeSummary()
    out = normalize_crude_import_rows(rows, country_origin_only=True, summary=summary)
    assert len(out) == 1
    assert summary.dropped_non_country_origin == 2
    assert summary.normalized_out == 1


def test_country_origin_only_false_keeps_aggregate_codes():
    rows = [_row(originId="WORLD", originType="WRL")]
    out = normalize_crude_import_rows(rows, country_origin_only=False)
    assert len(out) == 1
    assert out[0].partner_country == "WORLD"


def test_duplicate_business_key_report_counts_collisions():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    r1 = TradeFlowRecord(
        source="eia",
        dataset="crude-oil-imports",
        period_date=date(2025, 6, 1),
        reporter_country="US",
        partner_country="AG",
        commodity="crude_oil:LSW",
        flow_direction="import",
        observed_at=ts,
        quantity=Decimal("10"),
        quantity_unit="bbl",
        eia_origin_id="CTY_AG",
        eia_destination_id="PP_1",
        eia_grade_id="LSW",
    )
    r2 = r1.model_copy(
        update={
            "quantity": Decimal("99"),
            "eia_destination_id": "PP_9",
        }
    )
    rep = duplicate_business_key_report([r1, r2], top_n=25)
    assert rep["normalized_row_count"] == 2
    assert rep["unique_business_keys"] == 1
    assert rep["keys_with_duplicates"] == 1
    assert rep["top_duplicate_keys"][0]["count"] == 2


def test_aggregate_sums_quantity_and_clears_destination():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    ts2 = datetime(2025, 1, 2, tzinfo=timezone.utc)
    base_kw = dict(
        source="eia",
        dataset="crude-oil-imports",
        period_date=date(2025, 6, 1),
        reporter_country="US",
        partner_country="AG",
        commodity="crude_oil:LSW",
        flow_direction="import",
        quantity_unit="thousand barrels",
        eia_origin_id="CTY_AG",
        eia_grade_id="LSW",
    )
    r1 = TradeFlowRecord(
        **base_kw,
        observed_at=ts,
        quantity=Decimal("10"),
        eia_destination_id="PP_1",
    )
    r2 = TradeFlowRecord(
        **base_kw,
        observed_at=ts2,
        quantity=Decimal("25"),
        eia_destination_id="PP_2",
    )
    out, summary = aggregate_eia_crude_imports_for_canonical([r1, r2])
    assert len(out) == 1
    assert out[0].quantity == Decimal("35")
    assert out[0].eia_destination_id is None
    assert out[0].quantity_unit == "thousand barrels"
    assert out[0].observed_at == datetime(2025, 1, 2, tzinfo=timezone.utc)
    assert summary.normalized_out_before_aggregation == 2
    assert summary.aggregated_out == 1
    assert summary.groups_collapsed == 1
    assert summary.quantity_unit_inconsistent_groups == 0


def test_aggregate_mismatched_units_counts_in_summary():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    base_kw = dict(
        source="eia",
        dataset="crude-oil-imports",
        period_date=date(2025, 6, 1),
        reporter_country="US",
        partner_country="AG",
        commodity="crude_oil:LSW",
        flow_direction="import",
        observed_at=ts,
        eia_origin_id="CTY_AG",
        eia_grade_id="LSW",
    )
    r1 = TradeFlowRecord(
        **base_kw,
        quantity=Decimal("1"),
        quantity_unit="thousand barrels",
        eia_destination_id="PP_1",
    )
    r2 = TradeFlowRecord(
        **base_kw,
        quantity=Decimal("2"),
        quantity_unit="barrels",
        eia_destination_id="PP_2",
    )
    out, summary = aggregate_eia_crude_imports_for_canonical([r1, r2])
    assert len(out) == 1
    assert out[0].quantity == Decimal("3")
    assert out[0].quantity_unit == "thousand barrels"
    assert summary.quantity_unit_inconsistent_groups == 1


def test_post_aggregate_no_duplicate_business_keys():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    base_kw = dict(
        source="eia",
        dataset="crude-oil-imports",
        period_date=date(2025, 6, 1),
        reporter_country="US",
        partner_country="AG",
        commodity="crude_oil:LSW",
        flow_direction="import",
        observed_at=ts,
        quantity_unit="thousand barrels",
        eia_origin_id="CTY_AG",
        eia_grade_id="LSW",
    )
    r1 = TradeFlowRecord(**base_kw, quantity=Decimal("1"), eia_destination_id="PP_1")
    r2 = TradeFlowRecord(**base_kw, quantity=Decimal("2"), eia_destination_id="PP_2")
    merged, _ = aggregate_eia_crude_imports_for_canonical([r1, r2])
    rep = duplicate_business_key_report(merged, top_n=25)
    assert rep["keys_with_duplicates"] == 0
    assert rep["unique_business_keys"] == 1


def test_trade_flow_business_key_matches_tuple():
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    r = TradeFlowRecord(
        source="eia",
        dataset="crude-oil-imports",
        period_date=date(2025, 6, 1),
        reporter_country="US",
        partner_country="AG",
        commodity="crude_oil:LSW",
        flow_direction="import",
        observed_at=ts,
    )
    k = trade_flow_business_key(r)
    assert k[0] == "eia" and k[4] == "AG" and k[5] == "crude_oil:LSW"
