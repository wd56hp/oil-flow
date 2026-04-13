"""Tests for CLI entrypoints and job runners (DB mocked or test session)."""

from __future__ import annotations

from click.testing import CliRunner

from app.cli.main import main
from app.jobs.uncomtrade_fetch import default_comtrade_period


def _bind_session(monkeypatch, db_session, target: str) -> None:
    monkeypatch.setattr(target, lambda: db_session)


def test_cli_help_lists_commands():
    runner = CliRunner()
    r = runner.invoke(main, ["--help"])
    assert r.exit_code == 0
    assert "fetch_eia_data" in r.output
    assert "fetch_uncomtrade_data" in r.output
    assert "run_quality_checks" in r.output


def test_run_quality_checks_empty_db(monkeypatch, db_session):
    _bind_session(monkeypatch, db_session, "app.jobs.quality_checks.SessionLocal")
    from app.jobs.quality_checks import run_quality_checks

    outcome = run_quality_checks(since_hours=24)
    assert outcome.ok
    assert outcome.details["ingestion_runs_in_window"] == 0
    assert outcome.details["data_quality_issue_groups"] == []


def test_run_fetch_uncomtrade_no_key(monkeypatch, db_session):
    _bind_session(monkeypatch, db_session, "app.jobs.uncomtrade_fetch.SessionLocal")
    from app.jobs.uncomtrade_fetch import run_fetch_uncomtrade_data

    outcome = run_fetch_uncomtrade_data(period="202401")
    assert outcome.ok
    assert outcome.details.get("note") == "skipped_or_empty"


def test_default_comtrade_period_is_six_digits():
    p = default_comtrade_period()
    assert len(p) == 6 and p.isdigit()


def test_run_fetch_eia_data_with_mock_fetch(monkeypatch, db_session):
    _bind_session(monkeypatch, db_session, "app.jobs.eia_fetch.SessionLocal")
    raw = {
        "period": "2025-06",
        "quantity": "100",
        "originType": "CTY",
        "originId": "CTY_AG",
        "destinationId": "PP_1",
        "gradeId": "LSW",
    }
    monkeypatch.setattr(
        "app.jobs.eia_fetch.fetch_crude_oil_imports",
        lambda **kwargs: [raw],
    )
    from app.jobs.eia_fetch import run_fetch_eia_data

    outcome = run_fetch_eia_data(paginate=False)
    assert outcome.ok
    assert outcome.details["inserted"] == 1
    assert outcome.details["raw_row_count"] == 1


def test_cli_run_quality_checks_invocation(monkeypatch, db_session):
    _bind_session(monkeypatch, db_session, "app.jobs.quality_checks.SessionLocal")
    runner = CliRunner()
    r = runner.invoke(main, ["run_quality_checks", "--since-hours", "1"])
    assert r.exit_code == 0
    assert '"ok": true' in r.output
