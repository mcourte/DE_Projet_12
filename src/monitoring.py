"""M8 - Monitoring.

Surveille la volumétrie et la fraîcheur des données, alerte en cas
d'anomalie d'exécution du pipeline. Les métriques sont écrites dans le
schéma `monitoring` de PostgreSQL ; Grafana s'y connecte directement en
lecture (pas besoin de Prometheus : ce sont des métriques pipeline, pas
des métriques d'infrastructure).
"""


# TODO :
# 1. Créer la table monitoring.pipeline_runs (run_id, step, row_count, duration, status, created_at)
# 2. log_run_metrics : INSERT dans cette table à la fin de chaque étape
# 3. check_data_freshness : comparer MAX(date) de la table à now() - freshness_max_delay_hours (config.yaml)
# 4. check_volumetry_drift : comparer le row_count du run courant à la moyenne des runs précédents
# 5. send_alert : réutiliser notifier.send_slack_message, sur un channel dédié aux alertes (différent de #pratique-sportive)


def log_run_metrics(run_id: str, step: str, row_count: int, duration: float) -> None:
    """Enregistre les métriques d'exécution de chaque étape (volume
    traité, durée, statut) dans monitoring.pipeline_runs (PostgreSQL).
    """
    raise NotImplementedError


def check_data_freshness(table: str) -> bool:
    """Vérifie que les données les plus récentes ne dépassent pas le
    délai attendu (config.yaml -> monitoring.freshness_max_delay_hours).
    """
    raise NotImplementedError


def check_volumetry_drift(table: str, expected_range: tuple[int, int]) -> bool:
    """Détecte une variation anormale du volume de lignes traitées
    d'une exécution à l'autre.
    """
    raise NotImplementedError


# AXE D'AMÉLIORATION (hors périmètre initial du POC) :
# notify_geocoding_anomalies : après chaque run dbt, interroger silver_employees_validated
# où anomalie_distance = true, et si des lignes existent, envoyer un email récapitulatif à
# Juliette (id salarié, mode déclaré, distance calculée, seuil) — PAS sur Slack (channel
# public/motivationnel, inadapté à une question RH nominative). Formule A ne rejette jamais
# une déclaration automatiquement, elle la "remonte" : cette fonction est le mécanisme concret
# qui manquait pour que "remontée" veuille dire quelque chose de plus qu'une colonne en base.
def notify_geocoding_anomalies(anomalies_df):
    raise NotImplementedError


def send_alert(message: str, severity: str) -> None:
    """Notifie l'équipe (Slack/email) dès qu'une anomalie est détectée
    par le monitoring, ou qu'un test dbt échoue (cf. quality_checks.py).
    """
    raise NotImplementedError
