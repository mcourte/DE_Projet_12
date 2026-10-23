"""M2 - Extraction.

Lit les sources brutes (RH, Sportif, base opérationnelle), enrichit les
activités avec les données géographiques nécessaires à la validation
(formule A), et matérialise le tout en Parquet dans data/bronze/ pour
que dbt (moteur DuckDB) puisse les lire comme sources.
"""

import json
import os
from pathlib import Path
from typing import Optional, Tuple

import pandas as pd

from src.config import get_param

_PROJECT_ROOT = Path(__file__).resolve().parent.parent

_RH_COLUMNS = {
    "ID salarié": "id_salarie",
    "Nom": "nom",
    "Prénom": "prenom",
    "Date de naissance": "date_naissance",
    "BU": "bu",
    "Date d'embauche": "date_embauche",
    "Salaire brut": "salaire_brut",
    "Type de contrat": "type_contrat",
    "Nombre de jours de CP": "nombre_jours_cp",
    "Adresse du domicile": "adresse_domicile",
    "Moyen de déplacement": "moyen_deplacement",
}

_SPORT_COLUMNS = {
    "ID salarié": "id_salarie",
    "Pratique d'un sport": "pratique_sport",
}

# Mode de déplacement déclaré (fichier RH) -> mode attendu par l'API
# Google Maps Distance Matrix.
_MODE_TO_GOOGLE_TRAVEL_MODE = {
    "Marche/running": "walking",
    "Vélo/Trottinette/Autres": "bicycling",
    "Transports en commun": "transit",
    "véhicule thermique/électrique": "driving",
}

_GEOCODE_CACHE_PATH = _PROJECT_ROOT / "data" / "bronze" / ".geocode_cache.json"


def _resolve(path: str) -> Path:
    """Un chemin relatif est cherché depuis le répertoire courant, puis
    depuis la racine du projet (même logique que src/config.py)."""
    candidate = Path(path)
    if candidate.exists():
        return candidate.resolve()
    from_root = _PROJECT_ROOT / path
    if from_root.exists():
        return from_root
    return candidate


def extract_rh_referential(path: Optional[str] = None) -> pd.DataFrame:
    """Charge et nettoie le fichier RH (salaire, adresse, mode de
    déplacement, BU).
    """
    resolved_path = path or get_param("sources.fichier_rh", default="data/raw/Donnees_RH.xlsx")
    df = pd.read_excel(_resolve(resolved_path))
    df = df.rename(columns=_RH_COLUMNS)

    missing = set(_RH_COLUMNS.values()) - set(df.columns)
    if missing:
        raise ValueError(f"Colonnes RH manquantes après renommage : {sorted(missing)}")

    return df


def extract_sport_referential(path: Optional[str] = None) -> pd.DataFrame:
    """Charge le fichier des pratiques sportives déclarées, utilisé pour
    générer un historique cohérent. 66/161 salariés n'ont rien déclaré :
    à gérer explicitement (pas de sport -> pas d'historique généré).
    """
    resolved_path = path or get_param(
        "sources.fichier_sportif", default="data/raw/Donnees_Sportive.xlsx"
    )
    df = pd.read_excel(_resolve(resolved_path))
    df = df.rename(columns=_SPORT_COLUMNS)
    return df


def _postgres_engine():
    """Construit l'engine SQLAlchemy à partir de config.yaml (host/port/db)
    et des identifiants dans l'environnement (.env : POSTGRES_USER,
    POSTGRES_PASSWORD).
    """
    from sqlalchemy import create_engine

    host = get_param("postgres.host", default="localhost")
    port = get_param("postgres.port", default=5432)
    database = get_param("postgres.database", default="sportdata")
    user = os.environ.get("POSTGRES_USER", "sportdata")
    password = os.environ.get("POSTGRES_PASSWORD", "")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
    return create_engine(url)


def qualified_table(schema: Optional[str], table: str) -> str:
    """Qualifie un nom de table par son schéma quand il y en a un — évite
    de casser sur les moteurs (ex. SQLite en test) qui n'ont pas de
    notion de schéma.
    """
    return f'"{schema}".{table}' if schema else table


def extract_activities_from_postgres(schema: Optional[str] = None) -> pd.DataFrame:
    """Lit l'historique d'activités depuis la base opérationnelle
    (PostgreSQL) pour matérialisation en bronze.
    """
    schema = schema if schema is not None else get_param(
        "postgres.schema_operational", default="public"
    )
    engine = _postgres_engine()
    query = f"select * from {qualified_table(schema, 'activities')}"
    return pd.read_sql(query, engine)


def _read_json(path: Path) -> dict:
    """Lit un cache JSON ; un fichier absent donne un cache vide."""
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _write_json(path: Path, data: dict, indent: Optional[int] = None) -> None:
    """Écrit un cache JSON, en créant le dossier si besoin."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=indent)


def _google_client(_client=None):
    """Client Google Maps : celui fourni (tests) ou un client réel, avec
    la clé `GOOGLE_MAPS_API_KEY` de l'environnement."""
    if _client is not None:
        return _client

    import googlemaps

    return googlemaps.Client(key=os.environ["GOOGLE_MAPS_API_KEY"])


def _request_coordinates(address: str, _client=None) -> Tuple[float, float]:
    """Appelle l'API de géocodage Google Maps pour une adresse."""
    results = _google_client(_client).geocode(address)
    if not results:
        raise ValueError(f"Adresse introuvable via l'API Google Maps : {address!r}")

    location = results[0]["geometry"]["location"]
    return (location["lat"], location["lng"])


def geocode_address(address: str, _client=None) -> Tuple[float, float]:
    """Convertit une adresse en coordonnées, avec mise en cache disque
    pour limiter les appels redondants (facturés au-delà du quota gratuit).

    `_client` permet d'injecter un client googlemaps factice dans les
    tests, sans appel réseau réel.
    """
    cache = _read_json(_GEOCODE_CACHE_PATH)
    if address in cache:
        lat, lng = cache[address]
        return (lat, lng)

    coords = _request_coordinates(address, _client)
    cache[address] = coords
    _write_json(_GEOCODE_CACHE_PATH, cache, indent=2)
    return coords


def _travel_mode(mode: str) -> str:
    """Traduit le mode déclaré (fichier RH) en mode Google Maps."""
    travel_mode = _MODE_TO_GOOGLE_TRAVEL_MODE.get(mode)
    if travel_mode is None:
        raise ValueError(f"Mode de déplacement inconnu : {mode!r}")
    return travel_mode


def compute_commute_distance(
    home_coords: Tuple[float, float], mode: str, _client=None
) -> float:
    """Calcule la distance domicile → bureau selon le mode déclaré
    (API Google Maps Distance Matrix), en kilomètres.
    """
    travel_mode = _travel_mode(mode)
    office_address = get_param(
        "entreprise.adresse", default="1362 Av. des Platanes, 34970 Lattes"
    )

    result = _google_client(_client).distance_matrix(
        origins=[home_coords], destinations=[office_address], mode=travel_mode
    )
    element = result["rows"][0]["elements"][0]
    if element["status"] != "OK":
        raise ValueError(
            f"Distance non calculable pour {home_coords} -> {office_address} "
            f"({travel_mode}) : statut {element['status']}"
        )

    return element["distance"]["value"] / 1000.0


_DISTANCE_CACHE_PATH = _PROJECT_ROOT / "data" / "bronze" / ".distance_cache.json"
_MAX_DISTANCE_FAILURE_RATIO = 0.10


def _resolve_distance_km(row, cache: dict, _client=None) -> float:
    """Distance domicile-bureau d'un salarié : lue dans le cache (adresse
    + mode) si elle y est, sinon calculée via Google Maps puis mise en cache.
    """
    key = f"{row['adresse_domicile']}|{row['moyen_deplacement']}"
    if key in cache:
        return cache[key]

    coords = geocode_address(row["adresse_domicile"], _client=_client)
    km = compute_commute_distance(coords, row["moyen_deplacement"], _client=_client)
    cache[key] = km
    return km


def _raise_if_too_many_failures(failures: int, total: int) -> None:
    """Arrête le pipeline si trop de distances n'ont pas pu être calculées :
    mieux vaut ne rien calculer que verser des primes non vérifiées."""
    if total and failures / total > _MAX_DISTANCE_FAILURE_RATIO:
        raise RuntimeError(
            f"Géocodage en échec pour {failures}/{total} salariés "
            f"(seuil {_MAX_DISTANCE_FAILURE_RATIO:.0%}) — vérifier GOOGLE_MAPS_API_KEY et le quota."
        )


def enrich_with_commute_distance(df: pd.DataFrame, _client=None) -> pd.DataFrame:
    """Ajoute `distance_domicile_bureau_km` au référentiel RH. Une adresse
    non résolue donne une distance vide ; au-delà de 10 % d'échecs, une
    erreur est levée.
    """
    cache = _read_json(_DISTANCE_CACHE_PATH)

    distances = []
    failures = 0
    for _, row in df.iterrows():
        try:
            distances.append(_resolve_distance_km(row, cache, _client))
        except Exception:
            failures += 1
            distances.append(None)

    _write_json(_DISTANCE_CACHE_PATH, cache)
    _raise_if_too_many_failures(failures, len(df))

    result = df.copy()
    result["distance_domicile_bureau_km"] = distances
    return result


def extract_employees_with_distance(path: Optional[str] = None, _client=None) -> pd.DataFrame:
    """Référentiel RH complet prêt pour la couche bronze : fichier RH
    nettoyé + distance domicile-bureau réelle."""
    return enrich_with_commute_distance(extract_rh_referential(path), _client=_client)


def _bronze_dir(name: str) -> Path:
    bronze_root = _PROJECT_ROOT / get_param("warehouse.bronze_path", default="data/bronze")
    return bronze_root / name


def read_bronze_parquet(name: str) -> pd.DataFrame:
    """Relit un extrait bronze déjà écrit par write_bronze_parquet."""
    return pd.read_parquet(_bronze_dir(name) / "part-0.parquet")


def write_bronze_parquet(df: pd.DataFrame, name: str) -> str:
    """Écrit un DataFrame en Parquet sous data/bronze/<name>/, lu
    ensuite par les modèles dbt `bronze_*` via read_parquet(). Renvoie
    le chemin écrit.
    """
    out_dir = _bronze_dir(name)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-0.parquet"
    df.to_parquet(out_path, index=False)
    return str(out_path)
