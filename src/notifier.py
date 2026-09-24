"""M6 - Notifications Slack.

Consommateur NATS : transforme chaque activité publiée sur le sujet
`activities.new` en publication Slack, pour créer de l'émulation entre
salariés. Découplé du pipeline analytique (dbt), qui tourne en batch
sur son propre planning via Dagster.
"""


# TODO :
# 1. subscribe_to_activity_stream : nats-py, nc.subscribe(sujet, cb=on_new_activity_event)
# 2. on_new_activity_event : parser le JSON du message, appeler format_slack_message puis send_slack_message
# 3. format_slack_message : varier le ton selon sport_type (cf. exemples de la note de cadrage)
# 4. send_slack_message : slack_sdk.WebClient.chat_postMessage, gérer rate limit / channel introuvable


def format_slack_message(activity: dict, employee: dict) -> str:
    """Génère un message personnalisé et encourageant à partir d'une
    activité (« Bravo X ! Tu viens de courir... »).
    """
    raise NotImplementedError


def send_slack_message(channel: str, message: str) -> None:
    """Poste le message sur le channel Slack dédié via l'API/webhook."""
    raise NotImplementedError


def on_new_activity_event(event: dict) -> None:
    """Callback du subscriber NATS sur le sujet `activities.new` :
    enchaîne formatage + envoi, sans intervention manuelle.
    """
    raise NotImplementedError


def subscribe_to_activity_stream() -> None:
    """Ouvre la connexion NATS et s'abonne au sujet `activities.new`,
    en appelant `on_new_activity_event` pour chaque message reçu.
    """
    raise NotImplementedError
