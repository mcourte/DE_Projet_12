import shutil
import socket
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_FIXTURES_BRONZE = _PROJECT_ROOT / "tests" / "fixtures" / "bronze"
_REAL_BRONZE = _PROJECT_ROOT / "data" / "bronze"


def _postgres_is_reachable(host: str = "localhost", port: int = 5432, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


postgres_available = pytest.mark.skipif(
    not _postgres_is_reachable(),
    reason="PostgreSQL non joignable sur localhost:5432 (lancer `docker compose up -d postgres`)",
)


@pytest.fixture
def dbt_bronze_fixtures():
    """Copie le petit jeu de données bronze synthétique (tests/fixtures/bronze)
    dans data/bronze/ (gitignoré) le temps du test, pour que les modèles
    dbt aient quelque chose de réel à lire — puis nettoie derrière lui.
    """
    copied_dirs = []
    for source_dir in _FIXTURES_BRONZE.iterdir():
        if not source_dir.is_dir():
            continue
        dest_dir = _REAL_BRONZE / source_dir.name
        already_existed = dest_dir.exists()
        shutil.copytree(source_dir, dest_dir, dirs_exist_ok=True)
        if not already_existed:
            copied_dirs.append(dest_dir)

    yield

    for d in copied_dirs:
        shutil.rmtree(d, ignore_errors=True)
