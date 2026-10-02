"""M9 - Sécurité & gouvernance.

Protège les données RH sensibles en s'appuyant sur les mécanismes
natifs de PostgreSQL (rôles + extension pgcrypto), sans outil tiers
supplémentaire à opérer pour un POC de ce périmètre.
"""

import os
from typing import List

import pandas as pd

from src.config import get_param
from src.extract import _postgres_engine, qualified_table


def encrypt_sensitive_fields(df: pd.DataFrame, fields: List[str]) -> pd.DataFrame:
    """Chiffre les champs sensibles (salaire, adresse) avant tout
    stockage, via `pgp_sym_encrypt` (extension pgcrypto de PostgreSQL).

    Nécessite une connexion PostgreSQL réelle avec l'extension pgcrypto
    activée (`CREATE EXTENSION IF NOT EXISTS pgcrypto;`) — pas de
    substitut Python : c'est délibérément PostgreSQL qui fait le
    chiffrement, pas une bibliothèque applicative en plus à maintenir.
    """
    from sqlalchemy import text

    passphrase = os.environ.get("PGCRYPTO_PASSPHRASE")
    if not passphrase:
        raise RuntimeError("PGCRYPTO_PASSPHRASE manquant dans l'environnement (.env)")

    engine = _postgres_engine()
    result = df.copy()

    with engine.connect() as conn:
        for field in fields:
            if field not in result.columns:
                continue
            result[field] = [
                None
                if value is None
                else conn.execute(
                    text("select pgp_sym_encrypt(:value, :passphrase)"),
                    {"value": str(value), "passphrase": passphrase},
                ).scalar_one()
                for value in result[field]
            ]

    return result


def apply_access_control(user: str, resource: str) -> bool:
    """Vérifie les droits d'accès aux données RH selon le rôle
    PostgreSQL de l'utilisateur ou du service, via la fonction native
    `has_table_privilege` (GRANT/REVOKE natifs, pas de couche à part).
    """
    from sqlalchemy import text

    engine = _postgres_engine()
    schema = get_param("postgres.schema_operational", default="public")
    table = qualified_table(schema, resource)

    with engine.connect() as conn:
        return bool(
            conn.execute(
                text("select has_table_privilege(:user, :table, 'SELECT')"),
                {"user": user, "table": table},
            ).scalar_one()
        )


def _audit_log_table():
    schema = get_param("postgres.schema_monitoring", default="monitoring")
    return qualified_table(schema, "audit_log")


def _ensure_audit_log_table(engine, table: str) -> None:
    from sqlalchemy import text

    id_column = (
        "id serial primary key" if engine.dialect.name == "postgresql"
        else "id integer primary key autoincrement"
    )
    ddl = f"""
        create table if not exists {table} (
            {id_column},
            action text,
            "user" text,
            resource text,
            created_at timestamp default current_timestamp
        )
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def audit_log(action: str, user: str, resource: str) -> None:
    """Trace chaque accès ou modification des données sensibles, pour
    la conformité — jamais dans les logs applicatifs en clair.
    """
    from sqlalchemy import text

    engine = _postgres_engine()
    table = _audit_log_table()
    _ensure_audit_log_table(engine, table)

    query = text(f'insert into {table} (action, "user", resource) values (:action, :user, :resource)')
    with engine.begin() as conn:
        conn.execute(query, {"action": action, "user": user, "resource": resource})
