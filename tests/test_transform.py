from pathlib import Path

import pytest

from src import transform

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_PROFILES_EXISTS = (_PROJECT_ROOT / "dbt" / "profiles.yml").exists()

pytestmark = pytest.mark.skipif(
    not _PROFILES_EXISTS,
    reason="dbt/profiles.yml absent (copier dbt/profiles.yml.example) : tests d'integration dbt ignores",
)


def test_run_dbt_transform_and_read_gold_kpi(dbt_bronze_fixtures):
    transform.run_dbt_transform(full_refresh=True)
    df = transform.read_gold_kpi()

    assert set(df["id_salarie"]) == {1, 2, 3, 4, 5}

    row1 = df[df.id_salarie == 1].iloc[0]
    assert bool(row1.eligible_prime) is True
    assert row1.montant_prime == pytest.approx(2250.0)  # 45000 * 0.05
    assert row1.nb_activites_12_mois == 20
    assert bool(row1.eligible_bien_etre) is True

    row2 = df[df.id_salarie == 2].iloc[0]  # distance 20km en marche > seuil 15km
    assert bool(row2.eligible_prime) is False

    row4 = df[df.id_salarie == 4].iloc[0]  # aucun sport declare
    assert row4.nb_activites_12_mois == 0
    assert bool(row4.eligible_bien_etre) is False


def test_replay_historical_kpis_applies_new_rate(dbt_bronze_fixtures):
    try:
        df = transform.replay_historical_kpis({"taux_prime": 0.10})
        row1 = df[df.id_salarie == 1].iloc[0]
        assert row1.montant_prime == pytest.approx(4500.0)  # 45000 * 0.10
        assert row1.taux_prime_applique == pytest.approx(0.10)
    finally:
        # ne pas laisser le taux modifie affecter les autres tests
        transform.replay_historical_kpis({"taux_prime": 0.05})
