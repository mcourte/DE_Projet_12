from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import load

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_PROFILES_EXISTS = (_PROJECT_ROOT / "dbt" / "profiles.yml").exists()


def _sqlite_engine_with_employees():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                "create table employees (id_salarie integer primary key, nom text, salaire_brut integer)"
            )
        )
        conn.execute(
            text("insert into employees (id_salarie, nom, salaire_brut) values (1, 'Ancien', 30000)")
        )
    return engine


def test_upsert_employee_referential_inserts_and_updates(monkeypatch):
    engine = _sqlite_engine_with_employees()
    monkeypatch.setattr(load, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(load, "get_param", lambda key, default=None: None if "schema" in key else default)

    df = pd.DataFrame([
        {"id_salarie": 1, "nom": "Renomme", "salaire_brut": 31000},  # update
        {"id_salarie": 2, "nom": "Nouveau", "salaire_brut": 40000},  # insert
    ])
    count = load.upsert_employee_referential(df)
    assert count == 2

    with engine.connect() as conn:
        rows = conn.execute(text("select id_salarie, nom, salaire_brut from employees order by id_salarie")).all()

    assert rows == [(1, "Renomme", 31000), (2, "Nouveau", 40000)]


def test_upsert_employee_referential_empty_dataframe_noop(monkeypatch):
    calls = []
    monkeypatch.setattr(load, "_postgres_engine", lambda: calls.append("called"))
    assert load.upsert_employee_referential(pd.DataFrame()) == 0
    assert calls == []


@pytest.mark.skipif(not _PROFILES_EXISTS, reason="necessite dbt/profiles.yml")
def test_export_gold_to_postgres_writes_to_sqlite(monkeypatch, dbt_bronze_fixtures):
    from src import transform

    transform.run_dbt_transform(full_refresh=True)

    engine = create_engine("sqlite:///:memory:")
    monkeypatch.setattr(load, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(load, "get_param", lambda key, default=None: None if "schema" in key else default)

    count = load.export_gold_to_postgres("gold_kpi")
    assert count > 0

    with engine.connect() as conn:
        n = conn.execute(text("select count(*) from gold_kpi")).scalar_one()
    assert n == count
