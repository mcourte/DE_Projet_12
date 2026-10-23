"""M5 - Chargement.

Le stockage bronze/silver/gold en tant que tel est géré par dbt à
l'intérieur du fichier DuckDB (data/warehouse.duckdb) : ce module ne
gère que ce qui reste hors du périmètre dbt : écrire les extraits
sources en Parquet (bronze), et exporter la couche gold finale vers
PostgreSQL pour que Power BI s'y connecte nativement.
"""

from typing import Optional

import duckdb
import pandas as pd

from src import security
from src.config import get_param
from src.extract import _postgres_engine, qualified_table
from src.transform import _duckdb_path

# Colonnes de la table `employees` (le référentiel RH en contient d'autres,
# comme la distance domicile-bureau, qui ne vont pas dans cette table).
_EMPLOYEE_TABLE_COLUMNS = [
    "id_salarie", "nom", "prenom", "date_naissance", "bu", "date_embauche",
    "salaire_brut", "type_contrat", "nombre_jours_cp", "adresse_domicile", "moyen_deplacement",
]


def ensure_employees_table() -> None:
    """Crée la table du référentiel salarié si elle n'existe pas encore
    (nouvelle installation). Les colonnes sensibles sont ensuite
    converties en chiffré par security.ensure_encrypted_columns.
    """
    from sqlalchemy import text

    engine = _postgres_engine()
    schema = get_param("postgres.schema_operational", default="public")
    table = qualified_table(schema, "employees")
    ddl = f"""
        create table if not exists {table} (
            id_salarie integer primary key,
            nom text,
            prenom text,
            date_naissance date,
            bu text,
            date_embauche date,
            salaire_brut integer,
            type_contrat text,
            nombre_jours_cp integer,
            adresse_domicile text,
            moyen_deplacement text
        )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def upsert_employee_referential(df: pd.DataFrame) -> int:
    """Met à jour le référentiel salarié dans PostgreSQL (schéma public)
    sans dupliquer les lignes déjà présentes. Retourne le nombre de
    lignes traitées.
    """
    from sqlalchemy import text

    if df.empty:
        return 0

    engine = _postgres_engine()
    schema = get_param("postgres.schema_operational", default="public")
    table = qualified_table(schema, "employees")

    columns = list(df.columns)
    non_key_columns = [c for c in columns if c != "id_salarie"]
    col_list = ", ".join(columns)
    placeholders = ", ".join(f":{c}" for c in columns)
    update_clause = ", ".join(f"{c} = excluded.{c}" for c in non_key_columns)

    query = text(
        f"insert into {table} ({col_list}) values ({placeholders}) "
        f"on conflict (id_salarie) do update set {update_clause}"
    )
    with engine.begin() as conn:
        conn.execute(query, df.to_dict(orient="records"))

    return len(df)


def _prepare_employee_rows(rh: pd.DataFrame) -> pd.DataFrame:
    """Ne garde que les colonnes de la table `employees` et remplace les
    valeurs manquantes (NaN, NaT) par `None`, que PostgreSQL accepte."""
    rows = rh[_EMPLOYEE_TABLE_COLUMNS].astype(object)
    return rows.where(rows.notna(), None)


def sync_employee_referential(rh: pd.DataFrame) -> int:
    """Met à jour le référentiel salarié de PostgreSQL (lu par le notifier
    Slack) avec salaire et adresse chiffrés. Retourne le nombre de lignes.
    """
    rows = _prepare_employee_rows(rh)
    ensure_employees_table()
    fields = security.sensitive_fields()
    security.ensure_encrypted_columns("employees", fields)
    return upsert_employee_referential(security.encrypt_sensitive_fields(rows, fields))


def _read_duckdb_table(table: str) -> pd.DataFrame:
    """Lit une table de l'entrepôt DuckDB (ouvert en lecture seule)."""
    con = duckdb.connect(str(_duckdb_path()), read_only=True)
    try:
        return con.sql(f"select * from {table}").df()
    finally:
        con.close()


def _ensure_schema(engine, schema: Optional[str]) -> None:
    """Crée le schéma PostgreSQL s'il n'existe pas (sans effet si `schema` est vide)."""
    from sqlalchemy import text

    if schema:
        with engine.begin() as conn:
            conn.execute(text(f'create schema if not exists "{schema}"'))


def export_gold_to_postgres(table: str = "gold_kpi") -> int:
    """Copie une table gold de DuckDB vers le schéma `gold` de PostgreSQL
    — seul point que Power BI a besoin de connaître. Retourne le nombre
    de lignes exportées.
    """
    df = _read_duckdb_table(table)
    engine = _postgres_engine()
    schema = get_param("postgres.schema_gold", default="gold")
    _ensure_schema(engine, schema)
    df.to_sql(table, engine, schema=schema or None, if_exists="replace", index=False)
    return len(df)
