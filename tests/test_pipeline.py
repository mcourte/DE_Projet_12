import pandas as pd
import pytest

from src import pipeline


def _patch_happy_path(monkeypatch, calls):
    monkeypatch.setattr(pipeline.extract, "extract_rh_referential", lambda: pd.DataFrame({"a": [1, 2]}))
    monkeypatch.setattr(pipeline.extract, "extract_sport_referential", lambda: pd.DataFrame({"a": [1]}))
    monkeypatch.setattr(pipeline.extract, "extract_activities_from_postgres", lambda: pd.DataFrame({"a": [1, 2, 3]}))
    monkeypatch.setattr(pipeline.extract, "write_bronze_parquet", lambda df, name: calls.append(f"write:{name}"))
    monkeypatch.setattr(pipeline.transform, "run_dbt_transform", lambda: calls.append("dbt_run"))
    monkeypatch.setattr(pipeline.quality_checks, "run_dbt_tests", lambda: True)
    monkeypatch.setattr(pipeline.load, "export_gold_to_postgres", lambda: calls.append("export_gold") or 5)
    monkeypatch.setattr(pipeline.monitoring, "log_run_metrics", lambda *a, **k: calls.append(("log", a[1])))
    monkeypatch.setattr(pipeline.monitoring, "send_alert", lambda *a, **k: calls.append(("alert", a)))


def test_run_pipeline_happy_path_runs_every_step(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)

    pipeline.run_pipeline()

    assert "export_gold" in calls
    assert ("alert",) not in [c[:1] for c in calls if isinstance(c, tuple)]
    logged_steps = [c[1] for c in calls if isinstance(c, tuple) and c[0] == "log"]
    assert "extract_rh" in logged_steps
    assert "export_gold" in logged_steps


def test_run_pipeline_aborts_when_extraction_fails(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)
    monkeypatch.setattr(
        pipeline.extract, "extract_rh_referential", lambda: (_ for _ in ()).throw(RuntimeError("xlsx illisible"))
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
