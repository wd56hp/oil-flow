"""Tests for schema fingerprinting and data quality issue logging."""

from __future__ import annotations

from decimal import Decimal

from sqlalchemy import select

from app.models import DataQualityIssue, SchemaFingerprint
from app.services.data_quality import (
    ISSUE_NORMALIZED_NULL_QUANTITY,
    ISSUE_RAW_MISSING_FIELD,
    ISSUE_RAW_UNEXPECTED_COLUMN,
    ISSUE_SCHEMA_FINGERPRINT_CHANGED,
    evaluate_raw_schema,
    fingerprint_raw_column_names,
)
from app.services.ingestion_engine import ingest_trade_flow_records
from tests.factories import mock_eia_crude_import


class TestFingerprint:
    def test_fingerprint_stable_for_same_keys_different_order(self):
        rows = [{"b": 1, "a": 2}, {"c": 3}]
        h1, cols1 = fingerprint_raw_column_names(rows)
        h2, cols2 = fingerprint_raw_column_names([{"a": 0, "b": 0, "c": 0}])
        assert h1 == h2
        assert cols1 == cols2 == ["a", "b", "c"]


class TestEvaluateRawSchema:
    def test_logs_unexpected_and_missing_expected_columns(self, db_session):
        rows = [{"foo": 1, "extra": 2}]
        evaluate_raw_schema(
            db_session,
            source="eia",
            dataset="crude-oil-imports",
            rows=rows,
            expected_columns={"foo", "bar"},
            ingestion_run_id=None,
        )
        db_session.commit()
        issues = db_session.scalars(select(DataQualityIssue)).all()
        types = {i.issue_type for i in issues}
        assert ISSUE_RAW_UNEXPECTED_COLUMN in types
        assert ISSUE_RAW_MISSING_FIELD in types
        missing = next(i for i in issues if i.issue_type == ISSUE_RAW_MISSING_FIELD)
        assert missing.detail is not None
        assert missing.detail.get("field") == "bar"

    def test_schema_change_emits_fingerprint_issue_second_run(self, db_session):
        evaluate_raw_schema(
            db_session,
            source="eia",
            dataset="ds1",
            rows=[{"a": 1}],
            expected_columns=None,
            ingestion_run_id=None,
        )
        db_session.commit()

        evaluate_raw_schema(
            db_session,
            source="eia",
            dataset="ds1",
            rows=[{"a": 1, "b": 2}],
            expected_columns=None,
            ingestion_run_id=None,
        )
        db_session.commit()

        fp = db_session.scalars(
            select(SchemaFingerprint).where(
                SchemaFingerprint.source == "eia",
                SchemaFingerprint.dataset == "ds1",
            )
        ).one()
        assert "b" in fp.columns_json

        changed = [
            i
            for i in db_session.scalars(select(DataQualityIssue)).all()
            if i.issue_type == ISSUE_SCHEMA_FINGERPRINT_CHANGED
        ]
        assert len(changed) == 1
        assert changed[0].severity == "warning"


class TestIngestionEngineDataQuality:
    def test_ingest_with_raw_rows_does_not_fail_and_links_run(self, db_session):
        rec = mock_eia_crude_import()
        raw = {"period": "2025-06", "qty": "100", "unexpected_col": "x"}

        stats = ingest_trade_flow_records(
            db_session,
            [rec],
            raw_rows=[raw],
            expected_raw_columns={"period", "qty"},
        )

        assert stats.inserted == 1
        issues = db_session.scalars(select(DataQualityIssue)).all()
        assert any(i.issue_type == ISSUE_RAW_UNEXPECTED_COLUMN for i in issues)
        assert all(i.ingestion_run_id == stats.run_id for i in issues)

    def test_null_quantity_logs_info_does_not_abort(self, db_session):
        row = mock_eia_crude_import()
        row = row.model_copy(update={"quantity": None})

        stats = ingest_trade_flow_records(db_session, [row])

        assert stats.inserted == 1
        infos = [
            i
            for i in db_session.scalars(select(DataQualityIssue)).all()
            if i.issue_type == ISSUE_NORMALIZED_NULL_QUANTITY
        ]
        assert len(infos) == 1
        assert infos[0].severity == "info"
