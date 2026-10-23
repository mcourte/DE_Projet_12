import pandas as pd
import pytest
from dagster import materialize

from src.orchestration import dagster_definitions as dd


ALL_ASSETS = [
    dd.access_check,
    dd.bronze_employees,
    dd.bronze_sport_declare,
    dd.bronze_activities,
    dd.dbt_gold_kpi,
    dd.gold_kpi_in_postgres,
]


def _patch_happy_path(monkeypatch, calls):
    monkeypatch.setattr(dd.extract, "extract_employees_with_distance", lambda: pd.DataFrame({"a": [1, 2]}))
    monkeypatch.setattr(dd.extract, "extract_sport_referential", lambda: pd.DataFrame({"a": [1]}))
    monkeypatch.setattr(dd.extract, "extract_activities_from_postgres", lambda: pd.DataFrame({"a": [1, 2, 3]}))
    monkeypatch.setattr(dd.extract, "write_bronze_parquet", lambda df, name: calls.append(f"write:{name}"))
    monkeypatch.setattr(dd.transform, "run_dbt_transform", lambda: calls.append("dbt_run"))
    monkeypatch.setattr(dd.quality_checks, "run_dbt_tests", lambda: True)
    monkeypatch.setattr(dd.load, "export_gold_to_postgres", lambda: calls.append("export_gold") or 5)
    monkeypatch.setattr(dd.pipeline, "check_access", lambda: calls.append("check_access"))
    monkeypatch.setattr(dd.pipeline, "sync_employees", lambda df: calls.append("sync_employees") or len(df))
    monkeypatch.setattr(dd.pipeline, "audit", lambda action, resource: calls.append(("audit", action, resource)))


def test_all_assets_materialize_successfully(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)

    result = materialize(ALL_ASSETS)

    assert result.success
    assert calls.index("check_access") < calls.index("write:employees")
    assert "sync_employees" in calls
    assert ("audit", "export", "gold.gold_kpi") in calls
    assert "dbt_run" in calls
    assert "export_gold" in calls
    assert calls.count("write:employees") == 1


def test_gold_kpi_in_postgres_does_not_run_when_dbt_tests_fail(monkeypatch):
    calls = []
    _patch_happy_path(monkeypatch, calls)
    monkeypatch.setattr(dd.quality_checks, "run_dbt_tests", lambda: False)
    monkeypatch.setattr(dd.quality_checks, "parse_dbt_run_results", lambda: [{"unique_id": "test_x"}])

    result = materialize(ALL_ASSETS, raise_on_error=False)

    assert not result.success
    assert "export_gold" not in calls


def test_daily_kpi_schedule_cron_expression():
    assert dd.daily_kpi_schedule.cron_schedule == "0 6 * * *"


def test_definitions_expose_all_assets():
    asset_keys = {key.to_user_string() for key in dd.defs.resolve_asset_graph().get_all_asset_keys()}
    assert asset_keys == {
        "access_check",
        "bronze_employees",
        "bronze_sport_declare",
        "bronze_activities",
        "dbt_gold_kpi",
        "gold_kpi_in_postgres",
    }
