"""M6 - Notifications Slack.

Consommateur NATS : transforme chaque activité publiée sur le sujet
`activities.new` en publication Slack, pour créer de l'émulation entre
salariés. Découplé du pipeline analytique (dbt), qui tourne en batch
sur son propre planning via Dagster.
"""

import asyncio
import json
import os
from datetime import datetime
from typing import Optional

from src.config import get_param
from src.extract import _postgres_engine, qualified_table

# Style par sport : verbe d'action (pour les sports à distance) ou nom
# d'activité (sinon), plus un emoji — dans l'esprit des exemples de la
# note de cadrage ("Bravo Juliette Mendes ! Tu viens de courir...").
_SPORT_STYLE = {
    "Runing": {"verbe": "courir", "emoji": "🏃"},
    "Randonnée": {"nom_activite": "randonnée", "emoji": "🥾"},
    "Natation": {"verbe": "nager", "emoji": "🏊"},
    "Voile": {"nom_activite": "sortie voile", "emoji": "⛵"},
    "Équitation": {"nom_activite": "sortie équitation", "emoji": "🐎"},
    "Triathlon": {"nom_activite": "triathlon", "emoji": "🏅"},
    "Tennis": {"nom_activite": "match de tennis", "emoji": "🎾"},
    "Badminton": {"nom_activite": "match de badminton", "emoji": "🏸"},
    "Escalade": {"nom_activite": "séance d'escalade", "emoji": "🧗"},
    "Tennis de table": {"nom_activite": "match de tennis de table", "emoji": "🏓"},
    "Football": {"nom_activite": "match de foot", "emoji": "⚽"},
    "Judo": {"nom_activite": "séance de judo", "emoji": "🥋"},
    "Basketball": {"nom_activite": "match de basket", "emoji": "🏀"},
    "Rugby": {"nom_activite": "match de rugby", "emoji": "🏉"},
    "Boxe": {"nom_activite": "séance de boxe", "emoji": "🥊"},
}


def _as_datetime(value):
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    return value


def _duration_minutes(activity: dict) -> float:
    debut = _as_datetime(activity["date_debut"])
    fin = _as_datetime(activity["date_fin"])
    return (fin - debut).total_seconds() / 60


def format_slack_message(activity: dict, employee: dict) -> str:
    """Génère un message personnalisé et encourageant à partir d'une
    activité (« Bravo X ! Tu viens de courir... »).
    """
    prenom = employee.get("prenom", "")
    nom = employee.get("nom", "")
    sport = activity.get("type", "activité")
    style = _SPORT_STYLE.get(sport, {})
    emoji = style.get("emoji", "💪")
    duree_min = _duration_minutes(activity)

    if activity.get("distance_m"):
        distance_km = activity["distance_m"] / 1000
        if "verbe" in style:
            base = (
                f"Bravo {prenom} {nom} ! Tu viens de {style['verbe']} "
                f"{distance_km:.1f} km en {duree_min:.0f} min !"
            )
        else:
            nom_activite = style.get("nom_activite", sport.lower())
            base = (
                f"Bravo {prenom} {nom} ! {distance_km:.1f} km parcourus en "
                f"{duree_min:.0f} min ({nom_activite}) !"
            )
    else:
        nom_activite = style.get("nom_activite", sport.lower())
        base = f"Bravo {prenom} {nom} ! {nom_activite.capitalize()} : {duree_min:.0f} min d'effort !"

    message = f"{base} {emoji}"
    commentaire = activity.get("commentaire")
    if commentaire:
        message += f' ("{commentaire}")'
    return message


def send_slack_message(channel: str, message: str, _client=None) -> None:
    """Poste le message sur le channel Slack dédié via l'API/webhook."""
    if _client is None:
        from slack_sdk import WebClient

        token = os.environ["SLACK_BOT_TOKEN"]
        _client = WebClient(token=token)

    from slack_sdk.errors import SlackApiError

    try:
        _client.chat_postMessage(channel=channel, text=message)
    except SlackApiError as e:
        raise RuntimeError(f"Échec d'envoi Slack sur {channel} : {e.response['error']}") from e


def _get_employee(id_salarie: int) -> dict:
    """Récupère nom/prénom depuis le référentiel salarié en base
    opérationnelle (alimenté par load.upsert_employee_referential)."""
    from sqlalchemy import text

    engine = _postgres_engine()
    schema = get_param("postgres.schema_operational", default="public")
    table = qualified_table(schema, "employees")
    query = text(f"select nom, prenom from {table} where id_salarie = :id")

    with engine.connect() as conn:
        row = conn.execute(query, {"id": id_salarie}).mappings().first()

    if row is None:
        return {"nom": "", "prenom": f"#{id_salarie}"}
    return dict(row)


async def on_new_activity_event(event: dict) -> None:
    """Callback du subscriber NATS sur le sujet `activities.new` :
    enchaîne formatage + envoi, sans intervention manuelle.
    """
    employee = _get_employee(event["id_salarie"])
    message = format_slack_message(event, employee)
    channel = get_param("slack.channel", default="#pratique-sportive")
    send_slack_message(channel, message)


async def subscribe_to_activity_stream(nats_url: Optional[str] = None) -> None:
    """Ouvre la connexion NATS et s'abonne au sujet `activities.new`,
    en appelant `on_new_activity_event` pour chaque message reçu.

    Bloque indéfiniment (processus long-vivant) — à lancer séparément
    du pipeline batch, ex. `python -m src.notifier`.
    """
    import nats

    nats_url = nats_url or get_param("event_bus.url", default="nats://localhost:4222")
    subject = get_param("event_bus.subject_activities", default="activities.new")
    nc = await nats.connect(nats_url)

    async def _handler(msg):
        event = json.loads(msg.data.decode("utf-8"))
        await on_new_activity_event(event)

    await nc.subscribe(subject, cb=_handler)
    try:
        await asyncio.Event().wait()
    finally:
        await nc.close()


if __name__ == "__main__":
    asyncio.run(subscribe_to_activity_stream())
