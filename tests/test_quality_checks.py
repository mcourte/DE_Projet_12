from pathlib import Path

import pandas as pd
import pytest

from src import quality_checks, transform

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_PROFILES_EXISTS = (_PROJECT_ROOT / "dbt" / "profiles.yml").exists()

pytestmark = pytest.mark.skipif(
    not _PROFILES_EXISTS,
    reason="dbt/profiles.yml absent (copier dbt/profiles.yml.example) : tests d'integration dbt ignores",
)


def test_run_dbt_tests_passes_on_clean_fixtures(dbt_bronze_fixtures):
    transform.run_dbt_transform(full_refresh=True)
    assert quality_checks.run_dbt_tests() is True


def test_parse_dbt_run_results_no_failures_on_clean_fixtures(dbt_bronze_fixtures):
    transform.run_dbt_transform(full_refresh=True)
    quality_checks.run_dbt_tests()
    assert quality_checks.parse_dbt_run_results() == []


def test_run_dbt_tests_detects_invalid_moyen_deplacement(dbt_bronze_fixtures):
    # Corrompt volontairement la fixture employees pour vérifier que le
    # test accepted_values (schema.yml) est bien celui qui échoue.
    bad_employees = pd.DataFrame([{
        "id_salarie": 1, "nom": "X", "prenom": "Y", "bu": "Z",
        "date_embauche": "2020-01-01", "salaire_brut": 30000, "type_contrat": "CDI",
        "adresse_domicile": "1 rue de test", "moyen_deplacement": "Téléportation",
        "distance_domicile_bureau_km": 1.0,
    }])
    dest = _PROJECT_ROOT / "data" / "bronze" / "employees" / "part-0.parquet"
    bad_employees.to_parquet(dest, index=False)

    transform.run_dbt_transform(full_refresh=True)
    assert quality_checks.run_dbt_tests(select="bronze_employees") is False

    failures = quality_checks.parse_dbt_run_results()
    assert any("accepted_values" in (f["unique_id"] or "") for f in failures)
