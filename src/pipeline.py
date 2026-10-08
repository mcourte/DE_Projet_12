"""M7 - Orchestration (point d'entrée simple).

Séquence complète en CLI, sans planification ni reprise sur erreur
avancée — pratique pour un run manuel ou un test local. Le pilotage
"sérieux" (planification, retries, dépendances entre étapes visibles)
est délégué à Dagster : voir src/orchestration/dagster_definitions.py.
"""

import time
import uuid
from datetime import datetime
from typing import Optional

from src import extract, load, monitoring, quality_checks, transform


def _new_run_id(run_date: datetime) -> str:
    return f"{run_date.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"


def _row_count(result) -> int:
    return len(result) if hasattr(result, "__len__") else 0


def _run_step(run_id: str, step_name: str, func, *args, **kwargs):
    """Exécute une étape, journalise sa durée/volumétrie si elle réussit,
    ou route l'échec vers handle_pipeline_failure et relaie l'exception
    pour que l'appelant arrête proprement la suite du pipeline.
    """
    start = time.monotonic()
    try:
        result = func(*args, **kwargs)
    except Exception as exc:
        handle_pipeline_failure(step_name, exc)
        raise

    duration = time.monotonic() - start
    monitoring.log_run_metrics(run_id, step_name, _row_count(result), duration)
    return result


def run_pipeline(run_date: Optional[datetime] = None) -> None:
    """Exécute la séquence complète : extraction → dbt run (qualité +
    transformation) → export gold vers Postgres. La notification Slack
    ne fait pas partie de cette séquence : elle tourne en continu,
    déclenchée par le bus NATS (cf. src/notifier.py).

    S'arrête à la première étape en échec, sans exécuter les suivantes.
    """
    run_date = run_date or datetime.now()
    run_id = _new_run_id(run_date)

    try:
        rh = _run_step(run_id, "extract_rh", extract.extract_rh_referential)
        extract.write_bronze_parquet(rh, "employees")

        sport = _run_step(run_id, "extract_sport", extract.extract_sport_referential)
        extract.write_bronze_parquet(sport, "sport_declare")

        activities = _run_step(run_id, "extract_activities", extract.extract_activities_from_postgres)
        extract.write_bronze_parquet(activities, "activities")

        _run_step(run_id, "dbt_run", transform.run_dbt_transform)
    except Exception:
        return  # déjà journalisé par _run_step -> handle_pipeline_failure

    if not quality_checks.run_dbt_tests():
        failures = quality_checks.parse_dbt_run_results()
        handle_pipeline_failure("dbt_test", RuntimeError(f"{len(failures)} test(s) dbt en échec"))
        return

    try:
        _run_step(run_id, "export_gold", load.export_gold_to_postgres)
    except Exception:
        return


def handle_pipeline_failure(step: str, error: Exception) -> None:
    """Capture l'échec d'une étape, journalise le détail et arrête
    proprement sans corrompre les couches en aval.
    """
    monitoring.send_alert(f"Étape '{step}' en échec : {error}", severity="critical")


if __name__ == "__main__":
    run_pipeline()
