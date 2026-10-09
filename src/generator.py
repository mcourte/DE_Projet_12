"""M1 - Génération de données.

Simule l'historique Strava des 12 derniers mois, en attendant la
connexion à l'API réelle. S'appuie sur le sport déclaré par chaque
salarié (fichier Données Sportive) pour produire un historique cohérent.

Pas de CDC (Debezium) ici : c'est nous qui écrivons ce générateur, donc
il publie l'événement directement sur le bus (NATS) au moment de
l'écriture en base — inutile de capter des changements "externes".
"""

import asyncio
import json
import random
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd

from src.config import get_param
from src.extract import _postgres_engine, qualified_table

# Sports où une distance a un sens (cf. note de cadrage : "à laisser vide
# si non pertinent, par exemple pour l'escalade"). Les valeurs viennent
# du fichier Données Sportive réel (15 sports déclarés).
_DISTANCE_RELEVANT_SPORTS = {
    "Randonnée": {"distance_km": (5, 20), "duree_min": (60, 240)},
    "Triathlon": {"distance_km": (10, 40), "duree_min": (60, 180)},
    "Runing": {"distance_km": (3, 15), "duree_min": (20, 90)},
    "Natation": {"distance_km": (0.5, 3), "duree_min": (20, 60)},
    "Voile": {"distance_km": (5, 30), "duree_min": (60, 240)},
    "Équitation": {"distance_km": (2, 10), "duree_min": (30, 90)},
}
_NON_DISTANCE_SPORTS = {
    "Tennis": (45, 120),
    "Badminton": (30, 90),
    "Escalade": (60, 180),
    "Tennis de table": (30, 90),
    "Football": (60, 120),
    "Judo": (45, 90),
    "Basketball": (45, 120),
    "Rugby": (60, 120),
    "Boxe": (30, 90),
}

_COMMENT_POOL = [
    "", "", "", "",
    "Reprise du sport :)",
    "Belle sortie !",
    "Petite forme aujourd'hui",
    "Nouveau record personnel !",
]

_ACTIVITY_COLUMNS = ["id_salarie", "date_debut", "date_fin", "type", "distance_m", "commentaire"]


def generate_activity_record(
    employee_id: int, sport_type: str, date: datetime, rng: Optional[random.Random] = None
) -> dict:
    """Construit une ligne d'activité au format attendu :
    id_salarie, date début/fin, type, distance, commentaire.

    Le champ "id" n'est volontairement pas ici : il est assigné par
    PostgreSQL à l'insertion (colonne auto-incrémentée).
    """
    rng = rng or random.Random()

    if sport_type in _DISTANCE_RELEVANT_SPORTS:
        params = _DISTANCE_RELEVANT_SPORTS[sport_type]
        distance_km = rng.uniform(*params["distance_km"])
        duree_min = rng.uniform(*params["duree_min"])
        distance_m = round(distance_km * 1000)
    else:
        duree_range = _NON_DISTANCE_SPORTS.get(sport_type, (30, 90))
        duree_min = rng.uniform(*duree_range)
        distance_m = None

    date_fin = date + timedelta(minutes=duree_min)

    return {
        "id_salarie": employee_id,
        "date_debut": date,
        "date_fin": date_fin,
        "type": sport_type,
        "distance_m": distance_m,
        "commentaire": rng.choice(_COMMENT_POOL),
    }


def generate_activity_history(
    employee_id: int,
    sport: Optional[str],
    start: datetime,
    end: datetime,
    rng: Optional[random.Random] = None,
) -> pd.DataFrame:
    """Produit un historique réaliste d'activités pour un salarié sur une
    période donnée, cohérent avec son sport déclaré dans le fichier RH.

    Un salarié sans sport déclaré (pd.isna(sport) ou chaîne vide) n'a
    aucun historique généré — on ne peut pas inventer une pratique
    qu'il n'a jamais déclarée.
    """
    if sport is None or (isinstance(sport, float) and pd.isna(sport)) or not str(sport).strip():
        return pd.DataFrame(columns=_ACTIVITY_COLUMNS)

    rng = rng or random.Random(employee_id)

    # Niveau d'activité propre au salarié : certains sont très réguliers,
    # d'autres occasionnels — fait varier la fréquence, pas juste le nombre
    # total, pour un historique plus crédible.
    activities_per_month = rng.uniform(0.5, 4.5)

    records = []
    current_month_start = datetime(start.year, start.month, 1)
    while current_month_start < end:
        next_month = (current_month_start.replace(day=28) + timedelta(days=4)).replace(day=1)
        month_end = min(next_month, end)

        nb_activities_this_month = int(round(rng.uniform(0, activities_per_month * 2)))
        for _ in range(nb_activities_this_month):
            span_days = max((month_end - current_month_start).days, 1)
            day_offset = rng.randint(0, span_days - 1)
            hour = rng.randint(6, 21)
            minute = rng.choice([0, 15, 30, 45])
            activity_date = current_month_start + timedelta(days=day_offset, hours=hour, minutes=minute)
            if start <= activity_date <= end:
                records.append(generate_activity_record(employee_id, sport, activity_date, rng=rng))

        current_month_start = next_month

    if not records:
        return pd.DataFrame(columns=_ACTIVITY_COLUMNS)

    records = _drop_overlapping_records(records)
    return pd.DataFrame(records, columns=_ACTIVITY_COLUMNS)


def _drop_overlapping_records(records: list) -> list:
    """Un salarié ne peut pas être sur deux activités en même temps —
    supprime (glouton, par ordre chronologique) les activités générées
    qui chevaucheraient la précédente. Trouvé en générant un historique
    réaliste à pleine échelle (5000+ activités) : sans ce filtre, le
    test dbt `no_overlapping_activities` détecte de vrais chevauchements.
    """
    records_sorted = sorted(records, key=lambda r: r["date_debut"])
    kept = [records_sorted[0]]
    for record in records_sorted[1:]:
        if record["date_debut"] >= kept[-1]["date_fin"]:
            kept.append(record)
    return kept


def seed_operational_db(activities_df: pd.DataFrame) -> int:
    """Insère le lot initial de plusieurs milliers de lignes générées
    dans la base opérationnelle (PostgreSQL). Retourne le nombre de
    lignes insérées.
    """
    if activities_df.empty:
        return 0

    engine = _postgres_engine()
    schema = get_param("postgres.schema_operational", default="public")
    activities_df.to_sql(
        "activities", engine, schema=schema or None, if_exists="append", index=False, method="multi"
    )
    return len(activities_df)


async def _publish_async(subject: str, payload: bytes, nats_url: str) -> None:
    import nats

    nc = await nats.connect(nats_url)
    try:
        await nc.publish(subject, payload)
        await nc.flush()
    finally:
        await nc.close()


def publish_activity_event(activity: dict) -> None:
    """Publie l'activité sur le sujet NATS `activities.new` (cf.
    config/config.yaml -> event_bus). Remplace le couple Debezium/CDC +
    Kafka de la note de cadrage par une publication directe, puisque le
    générateur maîtrise déjà l'écriture en base.
    """
    nats_url = get_param("event_bus.url", default="nats://localhost:4222")
    subject = get_param("event_bus.subject_activities", default="activities.new")
    payload = json.dumps(activity, default=str).encode("utf-8")
    asyncio.run(_publish_async(subject, payload, nats_url))


def emit_live_activity(employee_id: int, sport_type: str, rng: Optional[random.Random] = None) -> dict:
    """Simule l'arrivée d'une activité "en direct" : écrit la ligne en
    base ET publie l'événement (cf. publish_activity_event) pour tester
    la chaîne temps réel Slack sans attendre l'intégration Strava.
    """
    from sqlalchemy import text

    activity = generate_activity_record(employee_id, sport_type, datetime.now(), rng=rng)

    engine = _postgres_engine()
    schema = get_param("postgres.schema_operational", default="public")
    table = qualified_table(schema, "activities")
    columns = ", ".join(activity.keys())
    placeholders = ", ".join(f":{k}" for k in activity.keys())
    query = text(f"insert into {table} ({columns}) values ({placeholders}) returning id")
    with engine.begin() as conn:
        result = conn.execute(query, activity)
        activity["id"] = result.scalar_one()

    publish_activity_event(activity)
    return activity


def _declared_sport(employee_id: int) -> str:
    """Sport déclaré par le salarié dans le fichier Données Sportive."""
    from src.extract import extract_sport_referential

    sports = extract_sport_referential()
    row = sports[sports["id_salarie"] == employee_id]
    if row.empty or pd.isna(row.iloc[0]["pratique_sport"]):
        raise SystemExit(f"Aucun sport déclaré pour le salarié {employee_id} : préciser --sport.")
    return row.iloc[0]["pratique_sport"]


def main(argv=None) -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Génération d'activités pour la démonstration")
    sub = parser.add_subparsers(dest="command", required=True)
    live = sub.add_parser("live", help="ajoute une activité maintenant (base + NATS -> Slack)")
    live.add_argument("--salarie", type=int, required=True, help="ID du salarié")
    live.add_argument("--sport", help="sport de l'activité (par défaut : le sport déclaré du salarié)")
    args = parser.parse_args(argv)

    sport = args.sport or _declared_sport(args.salarie)
    activity = emit_live_activity(args.salarie, sport)
    distance = f"{activity['distance_m'] / 1000:.1f} km" if activity.get("distance_m") else "sans distance"
    print(f"Activité {activity['id']} ajoutée : salarié {args.salarie}, {sport}, {distance}.")


if __name__ == "__main__":
    main()
