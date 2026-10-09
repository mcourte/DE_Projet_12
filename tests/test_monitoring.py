from datetime import datetime, timedelta

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


def test_check_data_freshness_true_when_recent(monkeypatch):
    engine = _sqlite_engine()
    with engine.begin() as conn:
        conn.execute(text("create table gold_kpi (id_salarie integer, calcule_le timestamp)"))
        conn.execute(
            text("insert into gold_kpi values (1, :ts)"),
            {"ts": datetime.utcnow().isoformat()},
        )
    monkeypatch.setattr(monitoring, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(monitoring, "get_param", lambda key, default=None: default)

    assert monitoring.check_data_freshness("gold_kpi") is True


def test_check_data_freshness_false_when_stale(monkeypatch):
    engine = _sqlite_engine()
    with engine.begin() as conn:
        conn.execute(text("create table gold_kpi (id_salarie integer, calcule_le timestamp)"))
        stale = datetime.utcnow() - timedelta(hours=48)
        conn.execute(text("insert into gold_kpi values (1, :ts)"), {"ts": stale.isoformat()})
    monkeypatch.setattr(monitoring, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(monitoring, "get_param", lambda key, default=None: 24 if "freshness" in key else default)

    assert monitoring.check_data_freshness("gold_kpi") is False


def test_check_data_freshness_false_when_no_known_column(monkeypatch):
    engine = _sqlite_engine()
    with engine.begin() as conn:
        conn.execute(text("create table mystery (foo integer)"))
        conn.execute(text("insert into mystery values (1)"))
    monkeypatch.setattr(monitoring, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(monitoring, "get_param", lambda key, default=None: default)

    assert monitoring.check_data_freshness("mystery") is False


def test_check_volumetry_drift_within_and_outside_range(monkeypatch):
    engine = _sqlite_engine()
    with engine.begin() as conn:
        conn.execute(text("create table activities (id integer)"))
        for i in range(50):
            conn.execute(text("insert into activities values (:i)"), {"i": i})
    monkeypatch.setattr(monitoring, "_postgres_engine", lambda: engine)

    assert monitoring.check_volumetry_drift("activities", (10, 100)) is True
    assert monitoring.check_volumetry_drift("activities", (1000, 2000)) is False


def test_send_alert_uses_dedicated_channel_and_severity(monkeypatch):
    sent = []
    monkeypatch.setattr(monitoring, "get_param", lambda key, default=None: "#pipeline_alerte")

    import src.notifier as notifier_module

    monkeypatch.setattr(notifier_module, "send_slack_message", lambda channel, message: sent.append((channel, message)))

    monitoring.send_alert("dbt test a echoue", severity="critical")

    assert sent[0][0] == "#pipeline_alerte"
    assert "[CRITICAL]" in sent[0][1]
    assert "dbt test a echoue" in sent[0][1]
