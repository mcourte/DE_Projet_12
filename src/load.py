"""M5 - Chargement.

Le stockage bronze/silver/gold en tant que tel est géré par dbt à
l'intérieur du fichier DuckDB (data/warehouse.duckdb) : ce module ne
gère que ce qui reste hors du périmètre dbt : écrire les extraits
sources en Parquet (bronze), et exporter la couche gold finale vers
PostgreSQL pour que Power BI s'y connecte nativement.
"""

# TODO :
# 1. upsert_employee_referential : "INSERT ... ON CONFLICT (id_salarie) DO UPDATE" côté PostgreSQL
# 2. export_gold_to_postgres : lire la table DuckDB, pandas.to_sql(if_exists="replace") dans le schéma gold
# 3. Créer le schéma gold au besoin : CREATE SCHEMA IF NOT EXISTS gold

import pandas as pd


def upsert_employee_referential(df: pd.DataFrame) -> None:
    """Met à jour le référentiel salarié dans PostgreSQL (schéma public)
    sans dupliquer les lignes déjà présentes.
    """
    raise NotImplementedError


def export_gold_to_postgres(table: str = "gold_kpi") -> None:
    """Lit une table gold depuis DuckDB et l'écrit dans le schéma
    `gold` de PostgreSQL (cf. config.yaml -> postgres.schema_gold),
    seul point de sortie que Power BI a besoin de connaître.
    """
    raise NotImplementedError
