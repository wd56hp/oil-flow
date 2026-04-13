"""
Tests for :mod:`app.services.ingestion_engine`.

Data is **mock EIA** normalized rows (see :mod:`tests.factories`); no HTTP calls.
"""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from sqlalchemy import select

from app.models import IngestionRun, TradeFlow, TradeFlowRevision
from app.services.ingestion_engine import ingest_trade_flow_records
from tests.factories import mock_eia_crude_import


class TestInsertNewRecords:
    def test_inserts_single_new_row_and_counts_it(self, db_session):
        row = mock_eia_crude_import(quantity="100")

        stats = ingest_trade_flow_records(db_session, [row], source_hint="eia")

        assert stats.inserted == 1
        assert stats.revised == 0
        assert stats.unchanged == 0
        assert stats.failed == 0

        stored = db_session.scalars(select(TradeFlow)).one()
        assert stored.quantity == Decimal("100")
        assert stored.partner_country == "AG"

        run = db_session.get(IngestionRun, stats.run_id)
        assert run is not None
        assert run.status == "completed"
        assert run.inserted_count == 1
        assert run.failed_count == 0
        assert run.source_hint == "eia"

    def test_inserts_multiple_distinct_keys_in_one_batch(self, db_session):
        us_ag = mock_eia_crude_import(partner_country="AG", quantity="100")
        us_sa = mock_eia_crude_import(
            partner_country="SA",
            quantity="200",
            eia_origin_id="CTY_SA",
        )

        stats = ingest_trade_flow_records(db_session, [us_ag, us_sa])

        assert stats.inserted == 2
        rows = db_session.scalars(select(TradeFlow)).all()
        assert len(rows) == 2
        partners = {r.partner_country for r in rows}
        assert partners == {"AG", "SA"}


class TestSkipUnchanged:
    def test_skips_when_same_key_and_same_measures(self, db_session):
        first = mock_eia_crude_import(quantity="500")
        ingest_trade_flow_records(db_session, [first])

        # Same business key and same quantity/unit → unchanged
        repeat = mock_eia_crude_import(quantity="500")

        stats = ingest_trade_flow_records(db_session, [repeat])

        assert stats.inserted == 0
        assert stats.revised == 0
        assert stats.unchanged == 1
        assert len(db_session.scalars(select(TradeFlow)).all()) == 1

    def test_lineage_only_change_still_counts_as_unchanged_and_does_not_update_row(
        self, db_session,
    ):
        """Same measures → skipped; lineage columns are not refreshed on that path."""
        ingest_trade_flow_records(
            db_session,
            [mock_eia_crude_import(quantity="100", eia_destination_id="PP_1")],
        )

        same_qty_new_dest = mock_eia_crude_import(
            quantity="100",
            eia_destination_id="PS_NJ",  # different port / lineage only
        )

        stats = ingest_trade_flow_records(db_session, [same_qty_new_dest])

        assert stats.unchanged == 1
        assert len(db_session.scalars(select(TradeFlowRevision)).all()) == 0
        canonical = db_session.scalars(select(TradeFlow)).one()
        assert canonical.eia_destination_id == "PP_1"


class TestRevisions:
    def test_creates_revision_row_when_quantity_changes(self, db_session):
        ingest_trade_flow_records(db_session, [mock_eia_crude_import(quantity="100")])

        updated = mock_eia_crude_import(quantity="150")

        stats = ingest_trade_flow_records(db_session, [updated])

        assert stats.revised == 1
        assert stats.inserted == 0
        assert stats.unchanged == 0

        canonical = db_session.scalars(select(TradeFlow)).one()
        assert canonical.quantity == Decimal("150")

        rev = db_session.scalars(select(TradeFlowRevision)).one()
        assert rev.previous_quantity == Decimal("100")
        assert rev.new_quantity == Decimal("150")

    def test_second_revision_stacks_another_audit_row(self, db_session):
        ingest_trade_flow_records(db_session, [mock_eia_crude_import(quantity="10")])
        ingest_trade_flow_records(db_session, [mock_eia_crude_import(quantity="20")])
        stats = ingest_trade_flow_records(db_session, [mock_eia_crude_import(quantity="30")])

        assert stats.revised == 1
        revisions = db_session.scalars(select(TradeFlowRevision)).all()
        assert len(revisions) == 2
        assert db_session.scalars(select(TradeFlow)).one().quantity == Decimal("30")


class TestDuplicatesInBatch:
    def test_duplicate_rows_same_measures_second_is_unchanged(self, db_session):
        """Two identical normalized rows in one run: insert once, skip once."""
        row = mock_eia_crude_import(quantity="999")

        stats = ingest_trade_flow_records(db_session, [row, row])

        assert stats.inserted == 1
        assert stats.unchanged == 1
        assert len(db_session.scalars(select(TradeFlow)).all()) == 1

    def test_duplicate_key_in_batch_last_measure_wins_revision(self, db_session):
        """Same key twice with different quantities: insert then revise within one batch."""
        first = mock_eia_crude_import(quantity="100")
        second = mock_eia_crude_import(
            quantity="250",
            observed_at=datetime(2025, 7, 16, 12, 0, 0, tzinfo=timezone.utc),
        )

        stats = ingest_trade_flow_records(db_session, [first, second])

        assert stats.inserted == 1
        assert stats.revised == 1
        assert stats.unchanged == 0

        canonical = db_session.scalars(select(TradeFlow)).one()
        assert canonical.quantity == Decimal("250")
        # SQLite may persist naive datetimes; compare wall-clock time only.
        assert canonical.observed_at.replace(tzinfo=None) == second.observed_at.replace(
            tzinfo=None
        )

        rev = db_session.scalars(select(TradeFlowRevision)).one()
        assert rev.previous_quantity == Decimal("100")
        assert rev.new_quantity == Decimal("250")

    def test_triple_duplicate_key_progresses_insert_unchanged_revise(self, db_session):
        a = mock_eia_crude_import(quantity="1")
        b = mock_eia_crude_import(quantity="1")
        c = mock_eia_crude_import(quantity="9")

        stats = ingest_trade_flow_records(db_session, [a, b, c])

        assert stats.inserted == 1
        assert stats.unchanged == 1
        assert stats.revised == 1
        assert db_session.scalars(select(TradeFlow)).one().quantity == Decimal("9")
