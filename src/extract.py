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


def extract_activities_from_postgres(schema: Optional[str] = None) -> pd.DataFrame:
    """Lit l'historique d'activités depuis la base opérationnelle
    (PostgreSQL) pour matérialisation en bronze.
    """
    schema = schema or get_param("postgres.schema_operational", default="public")
    engine = _postgres_engine()
    query = f'select * from "{schema}".activities'
    return pd.read_sql(query, engine)


def _load_geocode_cache() -> dict:
    if not _GEOCODE_CACHE_PATH.exists():
        return {}
    with open(_GEOCODE_CACHE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def _save_geocode_cache(cache: dict) -> None:
    _GEOCODE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_GEOCODE_CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def geocode_address(address: str, _client=None) -> Tuple[float, float]:
    """Convertit une adresse en coordonnées via l'API Google Maps, avec
    mise en cache disque pour limiter les appels redondants (facturés
    au-delà du quota gratuit).

    `_client` permet d'injecter un client googlemaps factice dans les
    tests, sans appel réseau réel.
    """
    cache = _load_geocode_cache()
    if address in cache:
        lat, lng = cache[address]
        return (lat, lng)

    if _client is None:
        import googlemaps

        api_key = os.environ["GOOGLE_MAPS_API_KEY"]
        _client = googlemaps.Client(key=api_key)

    results = _client.geocode(address)
    if not results:
        raise ValueError(f"Adresse introuvable via l'API Google Maps : {address!r}")

    location = results[0]["geometry"]["location"]
    coords = (location["lat"], location["lng"])

    cache[address] = coords
    _save_geocode_cache(cache)
    return coords


def compute_commute_distance(
    home_coords: Tuple[float, float], mode: str, _client=None
) -> float:
    """Calcule la distance domicile → bureau selon le mode déclaré
    (API Google Maps Distance Matrix), en kilomètres.
    """
    travel_mode = _MODE_TO_GOOGLE_TRAVEL_MODE.get(mode)
    if travel_mode is None:
        raise ValueError(f"Mode de déplacement inconnu : {mode!r}")

    office_address = get_param(
        "entreprise.adresse", default="1362 Av. des Platanes, 34970 Lattes"
    )

    if _client is None:
        import googlemaps

        api_key = os.environ["GOOGLE_MAPS_API_KEY"]
        _client = googlemaps.Client(key=api_key)

    result = _client.distance_matrix(
        origins=[home_coords], destinations=[office_address], mode=travel_mode
    )
    element = result["rows"][0]["elements"][0]
    if element["status"] != "OK":
        raise ValueError(
            f"Distance non calculable pour {home_coords} -> {office_address} "
            f"({travel_mode}) : statut {element['status']}"
        )

    return element["distance"]["value"] / 1000.0


def write_bronze_parquet(df: pd.DataFrame, name: str) -> str:
    """Écrit un DataFrame en Parquet sous data/bronze/<name>/, lu
    ensuite par les modèles dbt `bronze_*` via read_parquet(). Renvoie
    le chemin écrit.
    """
    bronze_root = _PROJECT_ROOT / get_param("warehouse.bronze_path", default="data/bronze")
    out_dir = bronze_root / name
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "part-0.parquet"
    df.to_parquet(out_path, index=False)
    return str(out_path)
