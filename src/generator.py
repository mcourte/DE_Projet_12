"""M1 - Génération de données.

Simule l'historique Strava des 12 derniers mois, en attendant la
connexion à l'API réelle. S'appuie sur le sport déclaré par chaque
salarié (fichier Données Sportive) pour produire un historique cohérent.

Pas de CDC (Debezium) ici : c'est nous qui écrivons ce générateur, donc
il publie l'événement directement sur le bus (NATS) au moment de
l'écriture en base — inutile de capter des changements "externes".
"""

# TODO :
# 1. generate_activity_record : distance/durée plausibles selon sport_type (vide si non pertinent, ex. escalade)
# 2. generate_activity_history : boucle sur 12 mois, fréquence variable selon le sport (ex. 1 à 3 activités/semaine)
# 3. Gérer les 66/161 salariés sans sport déclaré (fichier Sportif) : pas d'historique, ou fréquence minimale à définir avec Juliette
# 4. seed_operational_db : insertion en batch (executemany / to_sql), pas ligne par ligne
# 5. publish_activity_event : connexion nats-py, publier en JSON sur le sujet config.event_bus.subject_activities
# 6. emit_live_activity : generate_activity_record + écriture DB + publish_activity_event, pour tester la chaîne Slack en direct

from datetime import datetime

import pandas as pd


def generate_activity_history(
    employee_id: int, sport: str, start: datetime, end: datetime
) -> pd.DataFrame:
    """Produit un historique réaliste d'activités pour un salarié sur une
    période donnée, cohérent avec son sport déclaré dans le fichier RH.
    """
    raise NotImplementedError


def generate_activity_record(employee_id: int, sport_type: str, date: datetime) -> dict:
    """Construit une ligne d'activité au format attendu :
    id, salarié, date début/fin, type, distance, commentaire.
    """
    raise NotImplementedError


def seed_operational_db(activities_df: pd.DataFrame) -> None:
    """Insère le lot initial de plusieurs milliers de lignes générées
    dans la base opérationnelle (PostgreSQL).
    """
    raise NotImplementedError


def emit_live_activity(employee_id: int) -> dict:
    """Simule l'arrivée d'une activité "en direct" : écrit la ligne en
    base ET publie l'événement (cf. publish_activity_event) pour tester
    la chaîne temps réel Slack sans attendre l'intégration Strava.
    """
    raise NotImplementedError


def publish_activity_event(activity: dict) -> None:
    """Publie l'activité sur le sujet NATS `activities.new` (cf.
    config/config.yaml -> event_bus). Remplace le couple Debezium/CDC +
    Kafka de la note de cadrage par une publication directe, puisque le
    générateur maîtrise déjà l'écriture en base.
    """
    raise NotImplementedError
