"""M0 - Configuration.

Centralise tous les paramètres susceptibles d'évoluer (taux, seuils,
distances) pour qu'aucun ne soit codé en dur dans la logique métier.
Cf. config/config.yaml.
"""

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

DEFAULT_CONFIG_PATH = "config/config.yaml"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Charge .env une seule fois, au premier import de ce module — c'est le
# point d'entrée commun à tout le reste (generator, extract, notifier...),
# donc le seul endroit nécessaire pour que SLACK_BOT_TOKEN,
# GOOGLE_MAPS_API_KEY, POSTGRES_* soient disponibles partout ailleurs.
load_dotenv(_PROJECT_ROOT / ".env")

_cache: dict[str, dict] = {}


def _resolve_path(path: str) -> Path:
    """Un chemin relatif est cherché depuis le répertoire courant, puis
    depuis la racine du projet — pour que ça fonctionne qu'on lance le
    script depuis la racine ou depuis un sous-dossier (ex. dbt/).
    """
    candidate = Path(path)
    if candidate.is_absolute() and candidate.exists():
        return candidate
    if candidate.exists():
        return candidate.resolve()
    from_root = _PROJECT_ROOT / path
    if from_root.exists():
        return from_root
    # Aucune des deux ne marche : on laisse l'appel à open() lever
    # l'erreur, avec un message clair sur le chemin réellement testé.
    return candidate


def load_config(path: str = DEFAULT_CONFIG_PATH) -> dict:
    """Charge le fichier de paramètres (taux de prime, seuil d'activités,
    jours bien-être, distances max) depuis un YAML versionné à part du code.

    Le résultat est mis en cache par chemin résolu : un appel répété avec
    le même `path` ne relit pas le fichier.
    """
    resolved = _resolve_path(path)
    key = str(resolved)
    if key in _cache:
        return _cache[key]

    with open(resolved, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    _cache[key] = data
    return data


def get_param(key: str, default: Any = None, path: str = DEFAULT_CONFIG_PATH) -> Any:
    """Point d'accès unique à un paramètre, utilisé par tous les autres
    modules — évite qu'une valeur soit dupliquée à plusieurs endroits.

    `key` utilise la notation pointée, ex. "formule_prime.taux_prime".
    Ne lève jamais si une clé manque : retourne `default`.
    """
    try:
        config = load_config(path)
    except (FileNotFoundError, OSError):
        return default

    node: Any = config
    for part in key.split("."):
        if not isinstance(node, dict) or part not in node:
            return default
        node = node[part]
    return node
