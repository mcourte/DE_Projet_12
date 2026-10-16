"""M5 - Chargement.

Le stockage bronze/silver/gold en tant que tel est géré par dbt à
l'intérieur du fichier DuckDB (data/warehouse.duckdb) : ce module ne
gère que ce qui reste hors du périmètre dbt : écrire les extraits
sources en Parquet (bronze), et exporter la couche gold finale vers
PostgreSQL pour que Power BI s'y connecte nativement.
"""

import duckdb
import pandas as pd

from src.config import get_param
from src.extract import _postgres_engine, qualified_table
from src.transform import _duckdb_path


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


def export_gold_to_postgres(table: str = "gold_kpi") -> int:
    """Lit une table gold depuis DuckDB et l'écrit dans le schéma
    `gold` de PostgreSQL — seul point que Power BI a besoin de connaître.
    Retourne le nombre de lignes exportées.
    """
    from sqlalchemy import text

    con = duckdb.connect(str(_duckdb_path()), read_only=True)
    try:
        df = con.sql(f"select * from {table}").df()
    finally:
        con.close()

    engine = _postgres_engine()
    schema = get_param("postgres.schema_gold", default="gold")

    if schema:
        with engine.begin() as conn:
            conn.execute(text(f'create schema if not exists "{schema}"'))

    df.to_sql(table, engine, schema=schema or None, if_exists="replace", index=False)
    return len(df)
