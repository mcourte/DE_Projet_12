import asyncio

import pytest
from sqlalchemy import create_engine, text

from src import notifier


def test_format_slack_message_with_distance():
    activity = {
        "type": "Runing",
        "distance_m": 10800,
        "date_debut": "2024-06-01T18:00:00",
        "date_fin": "2024-06-01T18:46:00",
        "commentaire": "",
    }
    employee = {"prenom": "Juliette", "nom": "Mendes"}
    msg = notifier.format_slack_message(activity, employee)
    assert "Juliette Mendes" in msg
    assert "10.8 km" in msg
    assert "46 min" in msg
    assert "🏃" in msg


def test_format_slack_message_without_distance_includes_comment():
    activity = {
        "type": "Escalade",
        "distance_m": None,
        "date_debut": "2024-06-01T18:00:00",
        "date_fin": "2024-06-01T19:30:00",
        "commentaire": "Reprise du sport :)",
    }
    employee = {"prenom": "Laurence", "nom": "Morvan"}
    msg = notifier.format_slack_message(activity, employee)
    assert "Laurence Morvan" in msg
    assert "escalade" in msg.lower()
    assert "Reprise du sport" in msg
    assert "90 min" in msg


def test_format_slack_message_unknown_sport_falls_back():
    activity = {
        "type": "Curling",
        "distance_m": None,
        "date_debut": "2024-06-01T18:00:00",
        "date_fin": "2024-06-01T19:00:00",
        "commentaire": None,
    }
    msg = notifier.format_slack_message(activity, {"prenom": "A", "nom": "B"})
    assert "curling" in msg.lower()
    assert "💪" in msg


class _FakeSlackClient:
    def __init__(self, should_fail=False):
        self.should_fail = should_fail
        self.calls = []

    def chat_postMessage(self, channel, text):
        if self.should_fail:
            from slack_sdk.errors import SlackApiError

            raise SlackApiError("failed", {"error": "channel_not_found"})
        self.calls.append((channel, text))


def test_send_slack_message_success():
    client = _FakeSlackClient()
    notifier.send_slack_message("#pratique-sportive", "hello", _client=client)
    assert client.calls == [("#pratique-sportive", "hello")]


def test_send_slack_message_wraps_api_error():
    client = _FakeSlackClient(should_fail=True)
    with pytest.raises(RuntimeError):
        notifier.send_slack_message("#pratique-sportive", "hello", _client=client)


def _sqlite_engine_with_employees():
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as conn:
        conn.execute(text("create table employees (id_salarie integer, nom text, prenom text)"))
        conn.execute(
            text("insert into employees (id_salarie, nom, prenom) values (1, 'Mendes', 'Juliette')")
        )
    return engine


def test_get_employee_found(monkeypatch):
    engine = _sqlite_engine_with_employees()
    monkeypatch.setattr(notifier, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(notifier, "get_param", lambda key, default=None: None if "schema" in key else default)

    employee = notifier._get_employee(1)
    assert employee == {"nom": "Mendes", "prenom": "Juliette"}


def test_get_employee_not_found_returns_placeholder(monkeypatch):
    engine = _sqlite_engine_with_employees()
    monkeypatch.setattr(notifier, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(notifier, "get_param", lambda key, default=None: None if "schema" in key else default)

    employee = notifier._get_employee(999)
    assert employee == {"nom": "", "prenom": "#999"}


def test_on_new_activity_event_formats_and_sends(monkeypatch):
    monkeypatch.setattr(notifier, "_get_employee", lambda id_salarie: {"nom": "Mendes", "prenom": "Juliette"})
    sent = []
    monkeypatch.setattr(notifier, "send_slack_message", lambda channel, message: sent.append((channel, message)))

    event = {
        "id_salarie": 1,
        "type": "Runing",
        "distance_m": 5000,
        "date_debut": "2024-06-01T18:00:00",
        "date_fin": "2024-06-01T18:30:00",
        "commentaire": None,
    }
    asyncio.run(notifier.on_new_activity_event(event))

    assert len(sent) == 1
    channel, message = sent[0]
    assert channel == "#pratique-sportive"
    assert "Juliette Mendes" in message
