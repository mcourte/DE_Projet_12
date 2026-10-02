"""M3 - Contrôle qualité.

Les tests unitaires par ligne (schéma, distances négatives, dates
invalides, cf. models/schema.yml) sont désormais portés par dbt, pas
par du code Python bespoke — c'est le rôle natif de l'outil et ça évite
d'opérer un outil de test séparé (Great Expectations/SODA) en plus.

Ce module ne fait plus que déclencher les tests dbt depuis Python
(pour les besoins de l'orchestration Dagster et du monitoring) et
interpréter leur résultat.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path
from typing import List, Optional

from src.config import get_param

_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _dbt_project_dir() -> Path:
    return _PROJECT_ROOT / get_param("warehouse.dbt_project_dir", default="dbt")


def _dbt_executable() -> str:
    """dbt est installé comme script dans le même environnement Python
    que ce projet (cf. requirements.txt) : on le cherche d'abord à côté
    de l'interpréteur courant, puis sur le PATH.
    """
    candidate = Path(sys.executable).parent / ("dbt.exe" if sys.platform == "win32" else "dbt")
    if candidate.exists():
        return str(candidate)

    found = shutil.which("dbt")
    if found:
        return found

    raise RuntimeError(
        "Exécutable dbt introuvable — installer dbt-core et dbt-duckdb "
        "(pip install -r requirements.txt)"
    )


def run_dbt_tests(select: Optional[str] = None) -> bool:
    """Exécute `dbt test` (éventuellement restreint à `select`, ex.
    "gold_kpi") depuis le dossier dbt/, et retourne True si tous les
    tests passent.
    """
    cmd = [_dbt_executable(), "test"]
    if select:
        cmd += ["--select", select]

    result = subprocess.run(cmd, cwd=_dbt_project_dir(), capture_output=True, text=True)
    return result.returncode == 0


def parse_dbt_run_results(run_results_path: Optional[str] = None) -> List[dict]:
    """Lit le run_results.json généré par dbt après `dbt test` et
    retourne la liste des tests en échec avec leur détail, pour
    alimenter le monitoring (cf. src/monitoring.py -> send_alert).
    """
    path = Path(run_results_path) if run_results_path else _dbt_project_dir() / "target" / "run_results.json"

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    failures = []
    for r in data.get("results", []):
        status = r.get("status")
        if status not in ("pass", "success"):
            failures.append(
                {
                    "unique_id": r.get("unique_id"),
                    "status": status,
                    "message": r.get("message"),
                }
            )
    return failures
