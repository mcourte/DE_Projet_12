"""M2 - Extraction.

Lit les sources brutes (RH, Sportif, base opérationnelle), enrichit les
activités avec les données géographiques nécessaires à la validation
(formule A), et matérialise le tout en Parquet dans data/bronze/ pour
que dbt (moteur DuckDB) puisse les lire comme sources.
"""

# TODO :
# 1. extract_rh_referential / extract_sport_referential : pandas.read_excel, nettoyer les en-têtes de colonnes
# 2. extract_activities_from_postgres : requête SQL via SQLAlchemy sur la table d'activités
# 3. geocode_address : appel Google Maps Geocoding API, mettre en cache (dict ou table Postgres) pour ne pas refacturer la même adresse
# 4. compute_commute_distance : appel Distance Matrix API, gérer adresse introuvable / quota dépassé
# 5. write_bronze_parquet : df.to_parquet(), créer le dossier data/bronze/<name>/ si besoin

import pandas as pd


def extract_rh_referential() -> pd.DataFrame:
    """Charge et nettoie le fichier RH (salaire, adresse, mode de
    déplacement, BU).
    """
    raise NotImplementedError


def extract_sport_referential() -> pd.DataFrame:
    """Charge le fichier des pratiques sportives déclarées, utilisé pour
    générer un historique cohérent. 66/161 salariés n'ont rien déclaré :
    à gérer explicitement (pas de sport -> pas d'historique généré).
    """
    raise NotImplementedError


def extract_activities_from_postgres() -> pd.DataFrame:
    """Lit l'historique d'activités depuis la base opérationnelle
    (PostgreSQL) pour matérialisation en bronze."""
    raise NotImplementedError


def geocode_address(address: str) -> tuple[float, float]:
    """Convertit une adresse en coordonnées via l'API Google Maps, avec
    mise en cache pour limiter les appels redondants. (Imposé par la
    note de cadrage - pas une brique substituable par une alternative
    open source dans le cadre de ce POC.)
    """
    raise NotImplementedError


def compute_commute_distance(home_coords: tuple[float, float], mode: str) -> float:
    """Calcule la distance domicile → bureau selon le mode déclaré
    (API Google Maps Distance Matrix).
    """
    raise NotImplementedError


def write_bronze_parquet(df: pd.DataFrame, name: str) -> str:
    """Écrit un DataFrame en Parquet sous data/bronze/<name>/, lu
    ensuite par les modèles dbt `bronze_*` via read_parquet(). Renvoie
    le chemin écrit.
    """
    raise NotImplementedError
