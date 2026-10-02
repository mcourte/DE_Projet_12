"""M4 - Transformation & règles métier.

Les formules B et C sont désormais implémentées en SQL dans les modèles
dbt (dbt/models/gold/gold_prime.sql, gold_wellbeing.sql, gold_kpi.sql) :
c'est là qu'elles doivent être lues/modifiées en premier lieu.

Ce module ne fait plus que déclencher le run dbt et relire ses résultats
depuis DuckDB pour les besoins hors-BI (ex. tests, notebooks d'analyse).
"""

import json
import subprocess
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

from src.config import get_param
from src.quality_checks import _dbt_executable, _dbt_project_dir

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def run_dbt_transform(full_refresh: bool = False, dbt_vars: Optional[dict] = None) -> None:
    """Déclenche `dbt run` (avec --full-refresh si demandé) pour
    matérialiser bronze -> silver -> gold dans le fichier DuckDB.

    Lève une RuntimeError si dbt échoue, avec la sortie du process pour
    diagnostiquer sans avoir à relancer la commande à la main.
    """
    cmd = [_dbt_executable(), "run"]
    if full_refresh:
        cmd.append("--full-refresh")
    if dbt_vars:
        cmd += ["--vars", json.dumps(dbt_vars)]

    result = subprocess.run(cmd, cwd=_dbt_project_dir(), capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"`dbt run` a échoué :\n{result.stdout}\n{result.stderr}")


def _duckdb_path() -> Path:
    return _PROJECT_ROOT / get_param("warehouse.duckdb_path", default="data/warehouse.duckdb")


def read_gold_kpi() -> pd.DataFrame:
    """Relit la table gold_kpi depuis DuckDB, pour inspection ou export
    hors du flux dbt -> Postgres (ex. notebook, tests d'intégration).
    """
    con = duckdb.connect(str(_duckdb_path()), read_only=True)
    try:
        return con.sql("select * from gold_kpi").df()
    finally:
        con.close()


def replay_historical_kpis(new_params: dict) -> pd.DataFrame:
    """Recalcule l'intégralité des KPI historiques avec de nouveaux
    paramètres (taux, seuils) via `dbt run --full-refresh --vars`, puis
    relit le résultat. Répond explicitement à la demande de Juliette de
    pouvoir "relancer l'historique" si un taux ou une source change :
    --full-refresh recrée entièrement gold_prime/gold_wellbeing/gold_kpi
    plutôt que d'ajouter des lignes à côté de l'ancien taux.
    """
    run_dbt_transform(full_refresh=True, dbt_vars=new_params)
    return read_gold_kpi()
