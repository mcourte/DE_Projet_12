"""M3 - Contrôle qualité.

Les tests unitaires par ligne (schéma, distances négatives, dates
invalides, cf. models/schema.yml) sont désormais portés par dbt, pas
par du code Python bespoke — c'est le rôle natif de l'outil et ça évite
d'opérer un outil de test séparé (Great Expectations/SODA) en plus.

Ce module ne fait plus que déclencher les tests dbt depuis Python
(pour les besoins de l'orchestration Dagster et du monitoring) et
interpréter leur résultat.
"""

# TODO :
# 1. run_dbt_tests : subprocess.run(["dbt", "test", ...], cwd="dbt"), gérer le paramètre --select
# 2. parse_dbt_run_results : lire dbt/target/run_results.json, filtrer les entrées avec status != "pass"
# 3. Retourner une structure exploitable directement par monitoring.send_alert

import subprocess


def run_dbt_tests(select: str | None = None) -> bool:
    """Exécute `dbt test` (éventuellement restreint à `select`, ex.
    "gold_kpi") depuis le dossier dbt/, et retourne True si tous les
    tests passent.
    """
    raise NotImplementedError


def parse_dbt_run_results(run_results_path: str = "dbt/target/run_results.json") -> list[dict]:
    """Lit le run_results.json généré par dbt après `dbt test` et
    retourne la liste des tests en échec avec leur détail, pour
    alimenter le monitoring (cf. src/monitoring.py -> send_alert).
    """
    raise NotImplementedError
