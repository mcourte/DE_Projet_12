"""M0 - Configuration.

Centralise tous les paramètres susceptibles d'évoluer (taux, seuils,
distances) pour qu'aucun ne soit codé en dur dans la logique métier.
Cf. config/config.yaml.
"""

# TODO :
# 1. load_config : lire config/config.yaml avec PyYAML (yaml.safe_load)
# 2. Mettre le résultat en cache module-level pour ne pas relire le fichier à chaque appel
# 3. get_param : accès par clé pointée (ex. "formule_prime.taux_prime") + valeur par défaut
# 4. Ne jamais faire planter l'appelant si une clé manque : retourner `default`

from pathlib import Path
from typing import Any


def load_config(path: str = "config/config.yaml") -> dict:
    """Charge le fichier de paramètres (taux de prime, seuil d'activités,
    jours bien-être, distances max) depuis un YAML versionné à part du code.
    """
    raise NotImplementedError


def get_param(key: str, default: Any = None) -> Any:
    """Point d'accès unique à un paramètre, utilisé par tous les autres
    modules — évite qu'une valeur soit dupliquée à plusieurs endroits.
    """
    raise NotImplementedError
