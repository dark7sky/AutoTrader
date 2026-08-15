from datetime import datetime, timezone

from kis_ai_scalper.cli import main
from kis_ai_scalper.paper import report_from_database
from kis_ai_scalper.storage import connect_database


def test_empty_database_report_is_empty(tmp_path):
    path = tmp_path / "empty.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        report = report_from_database(database)

    assert report.empty is True
    assert report.total_paper_orders == 0
    assert report.total_paper_fills == 0
    assert report.open_positions == ()
    assert report.realized_pnl == 0


def test_report_calculates_one_buy_position(tmp_path):
    path = tmp_path / "buy.sqlite3"
    filled_at = datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc)
    with connect_database(path) as database:
        database.init_schema()
        assert database.record_paper_buy(
            order_id="o1", fill_id="f1", signal_id="s1", symbol="005930",
            quantity=10, price=100, created_at=filled_at,
        )
        report = report_from_database(database)

    assert report.empty is False
    assert report.total_paper_orders == 1
    assert report.total_paper_fills == 1
    assert report.gross_buy_value == 1000
    assert report.realized_pnl == 0
    assert report.symbols == ("005930",)
    assert report.open_positions[0].quantity == 10
    assert report.open_positions[0].average_cost == 100
    assert report.first_fill_timestamp == filled_at.isoformat()


def test_report_realizes_sell_pnl_using_weighted_average_cost(tmp_path):
    path = tmp_path / "sell.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        database.record_paper_buy(
            order_id="o1", fill_id="f1", signal_id="s1", symbol="005930",
            quantity=10, price=100,
            created_at=datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc),
        )
        database.record_paper_buy(
            order_id="o2", fill_id="f2", signal_id="s2", symbol="005930",
            quantity=10, price=110,
            created_at=datetime(2026, 8, 15, 9, 1, tzinfo=timezone.utc),
        )
        database.record_paper_sell(
            order_id="o3", fill_id="f3", signal_id="s3", symbol="005930",
            quantity=5, price=120,
            created_at=datetime(2026, 8, 15, 9, 2, tzinfo=timezone.utc),
        )
        report = report_from_database(database)

    assert report.gross_buy_value == 2100
    assert report.realized_pnl == 75
    assert report.open_positions[0].quantity == 15
    assert report.open_positions[0].average_cost == 105


def test_record_paper_sell_rejects_oversell_at_write_time(tmp_path):
    path = tmp_path / "oversell.sqlite3"
    with connect_database(path) as database:
        database.init_schema()
        database.record_paper_buy(
            order_id="o1", fill_id="f1", signal_id="s1", symbol="005930",
            quantity=2, price=100,
            created_at=datetime(2026, 8, 15, 9, 0, tzinfo=timezone.utc),
        )
        try:
            database.record_paper_sell(
                order_id="o2", fill_id="f2", signal_id="s2", symbol="005930",
                quantity=3, price=110,
                created_at=datetime(2026, 8, 15, 9, 1, tzinfo=timezone.utc),
            )
        except ValueError as exc:
            assert "exceeds long position" in str(exc)
        else:
            raise AssertionError("oversell should fail before being written")


def test_paper_report_cli_prints_concise_local_only_summary(tmp_path, capsys):
    path = tmp_path / "cli.sqlite3"
    with connect_database(path) as database:
        database.init_schema()

    assert main(["paper-report", "--db", str(path), "--symbol", "005930"]) == 0
    output = capsys.readouterr().out
    assert "paper-report: empty=true" in output
    assert "total_paper_orders=0 total_paper_fills=0" in output
    assert "broker_calls=none broker_orders=none account_queries=none ai_calls=none" in output
