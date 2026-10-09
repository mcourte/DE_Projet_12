"""M8 - Monitoring.

Surveille la volumétrie et la fraîcheur des données, alerte en cas
d'anomalie d'exécution du pipeline. Les métriques sont écrites dans le
schéma `monitoring` de PostgreSQL ; Grafana s'y connecte directement en
lecture (pas besoin de Prometheus : ce sont des métriques pipeline, pas
des métriques d'infrastructure).
"""

from datetime import datetime, timedelta, timezone
from typing import Tuple

from src.config import get_param
from src.extract import _postgres_engine, qualified_table

# Colonnes de date candidates, dans l'ordre où on les cherche, pour
# check_data_freshness : les différentes tables du projet n'ont pas
# toutes le même nom de colonne temporelle (created_at côté monitoring,
# calcule_le côté gold_kpi, date_debut côté activités...).
_FRESHNESS_CANDIDATE_COLUMNS = ["created_at", "calcule_le", "date_debut"]


def _pipeline_runs_table():
    schema = get_param("postgres.schema_monitoring", default="monitoring")
    return qualified_table(schema, "pipeline_runs")


def _ensure_pipeline_runs_table(engine, table: str) -> None:
    from sqlalchemy import text

    is_postgres = engine.dialect.name == "postgresql"
    id_column = "id serial primary key" if is_postgres else "id integer primary key autoincrement"
    ddl = f"""
        create table if not exists {table} (
            {id_column},
            run_id text,
            step text,
            row_count integer,
            duration_seconds double precision,
            status text default 'success',
            created_at timestamp default current_timestamp
        )
    """
    with engine.begin() as conn:
        if is_postgres and "." in table:
            schema = table.split(".")[0].strip('"')
            conn.execute(text(f'create schema if not exists "{schema}"'))
        conn.execute(text(ddl))


def log_run_metrics(
    run_id: str, step: str, row_count: int, duration: float, status: str = "success"
) -> None:
    """Enregistre les métriques d'exécution de chaque étape (volume
    traité, durée, statut) dans monitoring.pipeline_runs (PostgreSQL).
    `status` vaut "success" ou "failed" — c'est ce qui permet à Grafana
    d'afficher un taux de succès des exécutions.
    """
    from sqlalchemy import text

    engine = _postgres_engine()
    table = _pipeline_runs_table()
    _ensure_pipeline_runs_table(engine, table)

    query = text(
        f"insert into {table} (run_id, step, row_count, duration_seconds, status) "
        f"values (:run_id, :step, :row_count, :duration, :status)"
    )
    with engine.begin() as conn:
        conn.execute(
            query,
            {"run_id": run_id, "step": step, "row_count": row_count, "duration": duration, "status": status},
        )


def check_data_freshness(table: str) -> bool:
    """Vérifie que les données les plus récentes ne dépassent pas le
    délai attendu (config.yaml -> monitoring.freshness_max_delay_hours).

    Retourne True si les données sont fraîches, False sinon (ou si
    aucune colonne de date connue n'a été trouvée sur `table`).
    """
    from sqlalchemy import text

    engine = _postgres_engine()
    max_delay_hours = get_param("monitoring.freshness_max_delay_hours", default=24)

    last_ts = None
    for col in _FRESHNESS_CANDIDATE_COLUMNS:
        try:
            with engine.connect() as conn:
                last_ts = conn.execute(text(f"select max({col}) from {table}")).scalar_one()
            if last_ts is not None:
                break
        except Exception:
            continue

    if last_ts is None:
        return False

    if isinstance(last_ts, str):
        last_ts = datetime.fromisoformat(last_ts)
    if last_ts.tzinfo is not None:
        last_ts = last_ts.astimezone(timezone.utc).replace(tzinfo=None)

    delay = datetime.utcnow() - last_ts
    return delay <= timedelta(hours=max_delay_hours)


def check_volumetry_drift(table: str, expected_range: Tuple[int, int]) -> bool:
    """Détecte une variation anormale du volume de lignes traitées
    d'une exécution à l'autre. Retourne True si le volume actuel est
    dans `expected_range` (min, max inclus), False sinon.
    """
    from sqlalchemy import text

    engine = _postgres_engine()
    with engine.connect() as conn:
        count = conn.execute(text(f"select count(*) from {table}")).scalar_one()

    low, high = expected_range
    return low <= count <= high


# AXE D'AMÉLIORATION (hors périmètre initial du POC) :
# notify_geocoding_anomalies : après chaque run dbt, interroger silver_employees_validated
# où anomalie_distance = true, et si des lignes existent, envoyer un email récapitulatif à
# Juliette (id salarié, mode déclaré, distance calculée, seuil) — PAS sur Slack (channel
# public/motivationnel, inadapté à une question RH nominative). Formule A ne rejette jamais
# une déclaration automatiquement, elle la "remonte" : cette fonction est le mécanisme concret
# qui manquait pour que "remontée" veuille dire quelque chose de plus qu'une colonne en base.
def notify_geocoding_anomalies(anomalies_df):
    raise NotImplementedError


def send_alert(message: str, severity: str = "warning") -> None:
    """Notifie l'équipe dès qu'une anomalie est détectée par le
    monitoring, ou qu'un test dbt échoue (cf. quality_checks.py) — sur
    un channel Slack dédié, distinct de #pratique-sportive.
    """
    from src.notifier import send_slack_message

    channel = get_param("slack.alert_channel", default="#pipeline-alertes")
    send_slack_message(channel, f"[{severity.upper()}] {message}")
