import pandas as pd
import pytest

from src import pipeline


def _patch_happy_path(monkeypatch, calls):
    monkeypatch.setattr(pipeline.extract, "extract_employees_with_distance", lambda: pd.DataFrame({"a": [1, 2]}))
    monkeypatch.setattr(pipeline.extract, "extract_sport_referential", lambda: pd.DataFrame({"a": [1]}))
    monkeypatch.setattr(pipeline.extract, "extract_activities_from_postgres", lambda: pd.DataFrame({"a": [1, 2, 3]}))
    monkeypatch.setattr(pipeline.extract, "write_bronze_parquet", lambda df, name: calls.append(f"write:{name}"))
    monkeypatch.setattr(pipeline.transform, "run_dbt_transform", lambda: calls.append("dbt_run"))
    monkeypatch.setattr(pipeline.quality_checks, "run_dbt_tests", lambda: True)
    monkeypatch.setattr(pipeline.load, "export_gold_to_postgres", lambda: calls.append("export_gold") or 5)
    monkeypatch.setattr(pipeline.monitoring, "log_run_metrics", lambda *a, **k: calls.append(("log", a[1])))
    monkeypatch.setattr(pipeline.monitoring, "send_alert", lambda *a, **k: calls.append(("alert", a)))
    monkeypatch.setattr(pipeline, "sync_employees", lambda rh: calls.append("sync_employees") or len(rh))
    monkeypatch.setattr(pipeline.security, "apply_access_control", lambda user, resource: True)
    monkeypatch.setattr(pipeline.security, "audit_log", lambda action, user, resource: calls.append(("audit", action, resource)))


def test_run_pipeline_happy_path_runs_every_step(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)

    pipeline.run_pipeline()

    assert "export_gold" in calls
    assert ("alert",) not in [c[:1] for c in calls if isinstance(c, tuple)]
    logged_steps = [c[1] for c in calls if isinstance(c, tuple) and c[0] == "log"]
    assert "extract_rh" in logged_steps
    assert "sync_employees" in logged_steps
    assert "export_gold" in logged_steps


def test_run_pipeline_audits_reads_and_export(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)

    pipeline.run_pipeline()

    audits = [(c[1], c[2]) for c in calls if isinstance(c, tuple) and c[0] == "audit"]
    assert audits == [
        ("read", "referentiel_rh"),
        ("write_encrypted", "employees"),
        ("read", "activities"),
        ("export", "gold.gold_kpi"),
    ]


def test_run_pipeline_aborts_when_access_is_denied(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)
    monkeypatch.setattr(pipeline.security, "apply_access_control", lambda user, resource: False)

    pipeline.run_pipeline()

    assert "dbt_run" not in calls
    assert "export_gold" not in calls
    alerts = [c for c in calls if isinstance(c, tuple) and c[0] == "alert"]
    assert len(alerts) == 1
    assert "check_access" in alerts[0][1][0]


def test_audit_failure_does_not_stop_the_pipeline(monkeypatch, capsys):
    calls = []
    _patch_happy_path(monkeypatch, calls)

    def broken_audit(action, user, resource):
        raise RuntimeError("table audit indisponible")

    monkeypatch.setattr(pipeline.security, "audit_log", broken_audit)

    pipeline.run_pipeline()

    assert "export_gold" in calls
    assert "table audit indisponible" in capsys.readouterr().err


def test_run_pipeline_aborts_when_extraction_fails(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)
    monkeypatch.setattr(
        pipeline.extract, "extract_employees_with_distance", lambda: (_ for _ in ()).throw(RuntimeError("xlsx illisible"))
    )

    pipeline.run_pipeline()

    assert "dbt_run" not in calls
    assert "export_gold" not in calls
    alerts = [c for c in calls if isinstance(c, tuple) and c[0] == "alert"]
    assert len(alerts) == 1
    assert "extract_rh" in alerts[0][1][0]


def test_run_pipeline_aborts_when_dbt_tests_fail(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)
    monkeypatch.setattr(pipeline.quality_checks, "run_dbt_tests", lambda: False)
    monkeypatch.setattr(pipeline.quality_checks, "parse_dbt_run_results", lambda: [{"unique_id": "test_x"}])

    pipeline.run_pipeline()

    assert "export_gold" not in calls
    alerts = [c for c in calls if isinstance(c, tuple) and c[0] == "alert"]
    assert len(alerts) == 1
    assert "dbt_test" in alerts[0][1][0]


def test_handle_pipeline_failure_sends_critical_alert(monkeypatch):
    sent = []
    monkeypatch.setattr(pipeline.monitoring, "send_alert", lambda message, severity: sent.append((message, severity)))

    pipeline.handle_pipeline_failure("export_gold", RuntimeError("connexion refusee"))

    assert len(sent) == 1
    message, severity = sent[0]
    assert "export_gold" in message
    assert "connexion refusee" in message
    assert severity == "critical"


def test_replay_pipeline_replays_then_exports(monkeypatch):
    calls = []
    monkeypatch.setattr(
        pipeline.transform, "replay_historical_kpis", lambda params: calls.append(("replay", params)) or pd.DataFrame()
    )
    monkeypatch.setattr(pipeline.load, "export_gold_to_postgres", lambda: calls.append("export_gold") or 5)
    monkeypatch.setattr(pipeline.monitoring, "log_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(pipeline.security, "audit_log", lambda *a, **k: None)

    pipeline.replay_pipeline({"taux_prime": 0.10})

    assert calls == [("replay", {"taux_prime": 0.10}), "export_gold"]


def test_replay_pipeline_does_not_export_when_replay_fails(monkeypatch):
    calls = []

    def failing_replay(params):
        raise RuntimeError("dbt a echoue")

    monkeypatch.setattr(pipeline.transform, "replay_historical_kpis", failing_replay)
    monkeypatch.setattr(pipeline.load, "export_gold_to_postgres", lambda: calls.append("export_gold"))
    monkeypatch.setattr(pipeline.monitoring, "log_run_metrics", lambda *a, **k: None)
    monkeypatch.setattr(pipeline.monitoring, "send_alert", lambda *a, **k: None)

    pipeline.replay_pipeline({"taux_prime": 0.10})

    assert "export_gold" not in calls


def test_handle_pipeline_failure_survives_alert_failure(monkeypatch, capsys):
    def broken_alert(message, severity):
        raise RuntimeError("channel_not_found")

    monkeypatch.setattr(pipeline.monitoring, "send_alert", broken_alert)

    pipeline.handle_pipeline_failure("export_gold", RuntimeError("boom"))  # ne doit pas lever

    err = capsys.readouterr().err
    assert "boom" in err
    assert "channel_not_found" in err


def test_failed_step_is_logged_with_failed_status(monkeypatch):
    logged = []
    _patch_happy_path(monkeypatch, [])
    monkeypatch.setattr(pipeline.monitoring, "log_run_metrics", lambda *a, **k: logged.append((a[1], k.get("status", "success"))))
    monkeypatch.setattr(
        pipeline.extract, "extract_employees_with_distance", lambda: (_ for _ in ()).throw(RuntimeError("xlsx"))
    )

    pipeline.run_pipeline()

    assert ("extract_rh", "failed") in logged
