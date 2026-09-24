import json
import random
from datetime import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import generator


def test_generate_activity_record_distance_relevant_sport():
    rng = random.Random(1)
    record = generator.generate_activity_record(42, "Randonnée", datetime(2025, 1, 1), rng=rng)
    assert record["id_salarie"] == 42
    assert record["type"] == "Randonnée"
    assert record["distance_m"] is not None
    assert record["distance_m"] > 0
    assert record["date_fin"] > record["date_debut"]


def test_generate_activity_record_non_distance_sport_has_no_distance():
    rng = random.Random(1)
    record = generator.generate_activity_record(42, "Escalade", datetime(2025, 1, 1), rng=rng)
    assert record["distance_m"] is None


def test_generate_activity_history_no_sport_is_empty():
    df = generator.generate_activity_history(1, None, datetime(2024, 1, 1), datetime(2024, 12, 31))
    assert df.empty
    assert list(df.columns) == generator._ACTIVITY_COLUMNS

    df_nan = generator.generate_activity_history(
        1, float("nan"), datetime(2024, 1, 1), datetime(2024, 12, 31)
    )
    assert df_nan.empty


def test_generate_activity_history_is_deterministic_with_seeded_rng():
    start, end = datetime(2024, 1, 1), datetime(2024, 12, 31)
    df1 = generator.generate_activity_history(7, "Runing", start, end, rng=random.Random(123))
    df2 = generator.generate_activity_history(7, "Runing", start, end, rng=random.Random(123))
    pd.testing.assert_frame_equal(df1, df2)


def test_generate_activity_history_stays_within_bounds():
    start, end = datetime(2024, 1, 1), datetime(2024, 12, 31)
    df = generator.generate_activity_history(7, "Runing", start, end, rng=random.Random(5))
    assert (df["date_debut"] >= start).all()
    assert (df["date_debut"] <= end).all()
    assert (df["id_salarie"] == 7).all()
    assert (df["type"] == "Runing").all()


def _sqlite_engine_with_activities_table():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                create table activities (
                    id integer primary key autoincrement,
                    id_salarie integer,
                    date_debut text,
                    date_fin text,
                    type text,
                    distance_m real,
                    commentaire text
                )
                """
            )
        )
    return engine


def test_seed_operational_db_inserts_rows(monkeypatch):
    engine = _sqlite_engine_with_activities_table()
    monkeypatch.setattr(generator, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(
        generator, "get_param", lambda key, default=None: None if "schema" in key else default
    )

    df = generator.generate_activity_history(
        7, "Runing", datetime(2024, 1, 1), datetime(2024, 12, 31), rng=random.Random(5)
    )
    inserted = generator.seed_operational_db(df)

    assert inserted == len(df)
    with engine.connect() as conn:
        count = conn.execute(text("select count(*) from activities")).scalar_one()
    assert count == len(df)


def test_seed_operational_db_empty_dataframe_noop(monkeypatch):
    calls = []
    monkeypatch.setattr(generator, "_postgres_engine", lambda: calls.append("called"))
    empty = pd.DataFrame(columns=generator._ACTIVITY_COLUMNS)
    assert generator.seed_operational_db(empty) == 0
    assert calls == []  # jamais connecté si rien à insérer


def test_publish_activity_event_serializes_and_publishes(monkeypatch):
    captured = {}

    async def fake_publish_async(subject, payload, nats_url):
        captured["subject"] = subject
        captured["payload"] = payload
        captured["nats_url"] = nats_url

    monkeypatch.setattr(generator, "_publish_async", fake_publish_async)

    activity = {"id_salarie": 1, "type": "Runing", "date_debut": datetime(2024, 1, 1)}
    generator.publish_activity_event(activity)

    assert captured["subject"] == "activities.new"
    decoded = json.loads(captured["payload"])
    assert decoded["id_salarie"] == 1
    assert decoded["type"] == "Runing"


def test_emit_live_activity_inserts_and_publishes(monkeypatch):
    engine = _sqlite_engine_with_activities_table()
    monkeypatch.setattr(generator, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(
        generator, "get_param", lambda key, default=None: None if "schema" in key else default
    )
    published = []
    monkeypatch.setattr(generator, "publish_activity_event", published.append)

    activity = generator.emit_live_activity(99, "Natation", rng=random.Random(1))

    assert activity["id"] is not None
    assert activity["id_salarie"] == 99
    assert len(published) == 1
    assert published[0]["id"] == activity["id"]
