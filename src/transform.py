"""M4 - Transformation & règles métier.

Les formules B et C sont désormais implémentées en SQL dans les modèles
dbt (dbt/models/gold/gold_prime.sql, gold_wellbeing.sql, gold_kpi.sql) :
c'est là qu'elles doivent être lues/modifiées en premier lieu.

Ce module ne fait plus que déclencher le run dbt et relire ses résultats
depuis DuckDB pour les besoins hors-BI (ex. tests, notebooks d'analyse).
"""

# TODO :
# 1. run_dbt_transform : subprocess.run(["dbt", "run"] + (["--full-refresh"] si demandé), cwd="dbt")
# 2. read_gold_kpi : duckdb.connect(path).sql("select * from gold_kpi").df()
# 3. replay_historical_kpis : injecter new_params (--vars ou réécriture de dbt_project.yml), puis run_dbt_transform(full_refresh=True)
# 4. Vérifier que gold_kpi.calcule_le / taux_prime_applique tracent bien le changement, pas d'écrasement silencieux

import duckdb
import pandas as pd


def run_dbt_transform(full_refresh: bool = False) -> None:
    """Déclenche `dbt run` (avec --full-refresh si demandé) pour
    matérialiser bronze -> silver -> gold dans le fichier DuckDB.
    """
    raise NotImplementedError


def read_gold_kpi() -> pd.DataFrame:
    """Relit la table gold_kpi depuis DuckDB, pour inspection ou export
    hors du flux dbt -> Postgres (ex. notebook, tests d'intégration).
    """
    raise NotImplementedError


def replay_historical_kpis(new_params: dict) -> pd.DataFrame:
    """Recalcule l'intégralité des KPI historiques avec de nouveaux
    paramètres (taux, seuils) : met à jour dbt/dbt_project.yml `vars`
    (ou les passe en `--vars` à l'exécution) puis relance
    `run_dbt_transform(full_refresh=True)`. Répond explicitement à la
    demande de Juliette de pouvoir "relancer l'historique" si un taux
    ou une source change.
    """
    raise NotImplementedError
