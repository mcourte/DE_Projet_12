"""M9 - Sécurité & gouvernance.

Protège les données RH sensibles en s'appuyant sur les mécanismes
natifs de PostgreSQL (rôles + extension pgcrypto), sans outil tiers
supplémentaire à opérer pour un POC de ce périmètre.
"""

# TODO :
# 1. encrypt_sensitive_fields : privilégier pgcrypto côté SQL (pgp_sym_encrypt) plutôt qu'un chiffrement en Python
# 2. apply_access_control : définir des rôles PostgreSQL (lecture seule / écriture) et vérifier via pg_has_role
# 3. audit_log : écrire dans une table dédiée (ex. security.audit_log), jamais dans les logs applicatifs en clair

import pandas as pd


def encrypt_sensitive_fields(df: pd.DataFrame, fields: list[str]) -> pd.DataFrame:
    """Chiffre ou masque les champs sensibles (salaire, adresse) avant
    tout stockage — via pgcrypto (`pgp_sym_encrypt`) côté PostgreSQL.
    """
    raise NotImplementedError


def apply_access_control(user: str, resource: str) -> bool:
    """Vérifie les droits d'accès aux données RH selon le rôle
    PostgreSQL de l'utilisateur ou du service (GRANT/REVOKE natifs).
    """
    raise NotImplementedError


def audit_log(action: str, user: str, resource: str) -> None:
    """Trace chaque accès ou modification des données sensibles, pour
    la conformité.
    """
    raise NotImplementedError
