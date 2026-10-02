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
