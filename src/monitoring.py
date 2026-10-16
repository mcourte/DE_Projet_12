"""M8 - Monitoring.

Journalise chaque étape du pipeline (volume, durée, statut) et alerte
en cas d'échec. Les métriques sont écrites dans le schéma `monitoring`
de PostgreSQL ; Grafana s'y connecte directement en lecture (pas besoin
de Prometheus : ce sont des métriques pipeline, pas des métriques
d'infrastructure).
"""

from src.config import get_param
from src.extract import _postgres_engine, qualified_table


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


def send_alert(message: str, severity: str = "warning") -> None:
    """Notifie l'équipe dès qu'une anomalie est détectée par le
    monitoring, ou qu'un test dbt échoue (cf. quality_checks.py) — sur
    un channel Slack dédié, distinct de #pratique-sportive.
    """
    from src.notifier import send_slack_message

    channel = get_param("slack.alert_channel", default="#pipeline_alerte")
    send_slack_message(channel, f"[{severity.upper()}] {message}")
