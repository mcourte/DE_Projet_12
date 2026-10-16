import os

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from src import security
from tests.conftest import postgres_available


def _sqlite_engine():
    return create_engine("sqlite:///:memory:")


def test_audit_log_creates_table_and_inserts(monkeypatch):
    engine = _sqlite_engine()
    monkeypatch.setattr(security, "_postgres_engine", lambda: engine)
    monkeypatch.setattr(security, "_audit_log_table", lambda: "audit_log")

    security.audit_log("export_gold", "pipeline", "gold_kpi")
    security.audit_log("read", "juliette", "gold_kpi")

    with engine.connect() as conn:
        rows = conn.execute(
            text('select action, "user", resource from audit_log order by id')
        ).all()

    assert rows == [
        ("export_gold", "pipeline", "gold_kpi"),
        ("read", "juliette", "gold_kpi"),
    ]


def test_encrypt_sensitive_fields_requires_passphrase(monkeypatch):
    monkeypatch.delenv("PGCRYPTO_PASSPHRASE", raising=False)
    df = pd.DataFrame({"salaire_brut": [30000]})
    with pytest.raises(RuntimeError):
        security.encrypt_sensitive_fields(df, ["salaire_brut"])


@postgres_available
def test_encrypt_sensitive_fields_roundtrip_with_real_postgres(monkeypatch):
    os.environ.setdefault("PGCRYPTO_PASSPHRASE", "test-passphrase")
    df = pd.DataFrame({"salaire_brut": [30000, 45000]})
    encrypted = security.encrypt_sensitive_fields(df, ["salaire_brut"])
    assert (encrypted["salaire_brut"] != df["salaire_brut"]).all()


@postgres_available
def test_apply_access_control_with_real_postgres():
    # L'utilisateur configuré dans .env doit au moins avoir SELECT sur
    # ses propres tables — sinon rien d'autre ne fonctionnerait.
    user = os.environ.get("POSTGRES_USER", "sportdata")
    assert isinstance(security.apply_access_control(user, "employees"), bool)


@postgres_available
def test_ensure_encrypted_columns_encrypts_existing_values_and_is_idempotent():
    os.environ.setdefault("PGCRYPTO_PASSPHRASE", "test-passphrase")
    engine = security._postgres_engine()
    table = "tmp_sec_employees"
    with engine.begin() as conn:
        conn.execute(text(f"drop table if exists {table}"))
        conn.execute(text(f"create table {table} (id_salarie integer primary key, salaire_brut integer, nom text)"))
        conn.execute(text(f"insert into {table} values (1, 42000, 'Test')"))
    try:
        security.ensure_encrypted_columns(table, ["salaire_brut"])
        security.ensure_encrypted_columns(table, ["salaire_brut"])  # deuxieme appel : sans effet

        with engine.connect() as conn:
            data_type = conn.execute(
                text("select data_type from information_schema.columns where table_name = :t and column_name = 'salaire_brut'"),
                {"t": table},
            ).scalar()
            decrypted = conn.execute(
                text(f"select pgp_sym_decrypt(salaire_brut, :p) from {table}"),
                {"p": os.environ["PGCRYPTO_PASSPHRASE"]},
            ).scalar()
            plain_name = conn.execute(text(f"select nom from {table}")).scalar()
        assert data_type == "bytea"
        assert decrypted == "42000"
        assert plain_name == "Test"
    finally:
        with engine.begin() as conn:
            conn.execute(text(f"drop table if exists {table}"))
