from sqlalchemy import create_engine, text

from src import monitoring


def _sqlite_engine():
    return create_engine("sqlite:///:memory:")


def test_log_run_metrics_creates_table_and_inserts(monkeypatch):
    engine = _sqlite_engine()
    monkeypatch.setattr(monitoring, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(monitoring, "_pipeline_runs_table", lambda: "pipeline_runs")

    monitoring.log_run_metrics("run-1", "extraction", 161, 2.5)
    monitoring.log_run_metrics("run-1", "transform", 161, 4.1)

    with engine.connect() as conn:
        rows = conn.execute(
            text("select run_id, step, row_count, duration_seconds from pipeline_runs order by id")
        ).all()

    assert rows == [("run-1", "extraction", 161, 2.5), ("run-1", "transform", 161, 4.1)]


def test_send_alert_uses_dedicated_channel_and_severity(monkeypatch):
    sent = []
    monkeypatch.setattr(monitoring, "get_param", lambda key, default=None: "#pipeline_alerte")

    import src.notifier as notifier_module

    monkeypatch.setattr(notifier_module, "send_slack_message", lambda channel, message: sent.append((channel, message)))

    monitoring.send_alert("dbt test a echoue", severity="critical")

    assert sent[0][0] == "#pipeline_alerte"
    assert "[CRITICAL]" in sent[0][1]
    assert "dbt test a echoue" in sent[0][1]
