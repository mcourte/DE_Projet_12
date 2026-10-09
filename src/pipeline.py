"""M7 - Orchestration (point d'entrée simple).

Séquence complète en CLI, sans planification ni reprise sur erreur
avancée — pratique pour un run manuel ou un test local. Le pilotage
"sérieux" (planification, retries, dépendances entre étapes visibles)
est délégué à Dagster : voir src/orchestration/dagster_definitions.py.
"""

import sys
import time
import uuid
from datetime import datetime
from typing import Optional

from src import extract, load, monitoring, quality_checks, transform


def _new_run_id(run_date: datetime) -> str:
    return f"{run_date.strftime('%Y%m%dT%H%M%S')}-{uuid.uuid4().hex[:6]}"


def _row_count(result) -> int:
    if isinstance(result, int):
        return result
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
        _log_failure(run_id, step_name, time.monotonic() - start)
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
        rh = _run_step(run_id, "extract_rh", extract.extract_employees_with_distance)
        extract.write_bronze_parquet(rh, "employees")

        sport = _run_step(run_id, "extract_sport", extract.extract_sport_referential)
        extract.write_bronze_parquet(sport, "sport_declare")

        activities = _run_step(run_id, "extract_activities", extract.extract_activities_from_postgres)
        extract.write_bronze_parquet(activities, "activities")

        _run_step(run_id, "dbt_run", transform.run_dbt_transform)
    except Exception:
        return  # déjà journalisé par _run_step -> handle_pipeline_failure

    start = time.monotonic()
    if not quality_checks.run_dbt_tests():
        failures = quality_checks.parse_dbt_run_results()
        _log_failure(run_id, "dbt_test", time.monotonic() - start)
        handle_pipeline_failure("dbt_test", RuntimeError(f"{len(failures)} test(s) dbt en échec"))
        return
    monitoring.log_run_metrics(run_id, "dbt_test", 0, time.monotonic() - start)

    try:
        _run_step(run_id, "export_gold", load.export_gold_to_postgres)
    except Exception:
        return


def replay_pipeline(new_params: dict, run_date: Optional[datetime] = None) -> None:
    """Rejoue tout l'historique des KPI avec de nouveaux paramètres (ex.
    {"taux_prime": 0.10}) PUIS exporte le résultat vers PostgreSQL : sans
    cet export, Power BI continuerait d'afficher les anciennes valeurs.
    """
    run_id = _new_run_id(run_date or datetime.now())
    try:
        _run_step(run_id, "replay_kpis", transform.replay_historical_kpis, new_params)
        _run_step(run_id, "export_gold", load.export_gold_to_postgres)
    except Exception:
        return


def _log_failure(run_id: str, step: str, duration: float) -> None:
    try:
        monitoring.log_run_metrics(run_id, step, 0, duration, status="failed")
    except Exception:
        pass  # le journal ne doit jamais masquer l'erreur d'origine


def handle_pipeline_failure(step: str, error: Exception) -> None:
    """Capture l'échec d'une étape, journalise le détail et arrête
    proprement sans corrompre les couches en aval.

    Si l'alerte Slack elle-même échoue (channel absent, token invalide),
    l'erreur d'origine est quand même affichée : un échec d'alerte ne
    doit pas en cacher un autre.
    """
    message = f"Étape '{step}' en échec : {error}"
    print(f"[PIPELINE] {message}", file=sys.stderr)
    try:
        monitoring.send_alert(message, severity="critical")
    except Exception as alert_error:
        print(f"[PIPELINE] alerte Slack non envoyée : {alert_error}", file=sys.stderr)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Pipeline POC Avantages Sportifs")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("run", help="extraction -> dbt -> export gold (par défaut)")
    replay = sub.add_parser("replay", help="rejoue l'historique avec un nouveau taux, puis exporte")
    replay.add_argument("--taux-prime", type=float, required=True, help="ex. 0.10 pour 10 %%")
    args = parser.parse_args()

    if args.command == "replay":
        replay_pipeline({"taux_prime": args.taux_prime})
    else:
        run_pipeline()
