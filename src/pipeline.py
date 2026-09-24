"""M7 - Orchestration (point d'entrée simple).

Séquence complète en CLI, sans planification ni reprise sur erreur
avancée — pratique pour un run manuel ou un test local. Le pilotage
"sérieux" (planification, retries, dépendances entre étapes visibles)
est délégué à Dagster : voir src/orchestration/dagster_definitions.py.
"""

# TODO :
# 1. run_pipeline : enchaîner extract -> dbt run -> dbt test -> export, logger chaque étape (monitoring.log_run_metrics)
# 2. Ne pas exporter en gold si dbt test échoue en amont
# 3. handle_pipeline_failure : logger l'erreur (monitoring.send_alert) et arrêter proprement

from datetime import datetime


def run_pipeline(run_date: datetime) -> None:
    """Exécute la séquence complète : extraction → dbt run (qualité +
    transformation) → export gold vers Postgres. La notification Slack
    ne fait pas partie de cette séquence : elle tourne en continu,
    déclenchée par le bus NATS (cf. src/notifier.py).
    """
    raise NotImplementedError


def handle_pipeline_failure(step: str, error: Exception) -> None:
    """Capture l'échec d'une étape, journalise le détail et arrête
    proprement sans corrompre les couches en aval.
    """
    raise NotImplementedError
