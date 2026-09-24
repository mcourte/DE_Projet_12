import pandas as pd
import pytest

from src import extract


def test_extract_rh_referential_renames_columns():
    df = extract.extract_rh_referential()
    expected = {
        "id_salarie", "nom", "prenom", "date_naissance", "bu",
        "date_embauche", "salaire_brut", "type_contrat",
        "nombre_jours_cp", "adresse_domicile", "moyen_deplacement",
    }
    assert expected.issubset(df.columns)
    assert len(df) == 161


def test_extract_sport_referential_renames_columns():
    df = extract.extract_sport_referential()
    assert set(df.columns) == {"id_salarie", "pratique_sport"}
    assert len(df) == 161


def test_write_bronze_parquet_roundtrip(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "_PROJECT_ROOT", tmp_path)
    df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
    out_path = extract.write_bronze_parquet(df, "demo")
    assert (tmp_path / "data" / "bronze" / "demo" / "part-0.parquet").exists()
    back = pd.read_parquet(out_path)
    pd.testing.assert_frame_equal(back, df)


class _FakeGeocodeClient:
    def __init__(self, lat=43.6, lng=3.9):
        self.lat, self.lng = lat, lng
        self.calls = 0

    def geocode(self, address):
        self.calls += 1
        return [{"geometry": {"location": {"lat": self.lat, "lng": self.lng}}}]


def test_geocode_address_uses_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "_GEOCODE_CACHE_PATH", tmp_path / "cache.json")
    client = _FakeGeocodeClient()

    coords1 = extract.geocode_address("1 rue de test", _client=client)
    coords2 = extract.geocode_address("1 rue de test", _client=client)

    assert coords1 == (43.6, 3.9)
    assert coords2 == coords1
    # Le deuxième appel doit venir du cache, pas d'un nouvel appel API.
    assert client.calls == 1


def test_geocode_address_raises_when_not_found(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "_GEOCODE_CACHE_PATH", tmp_path / "cache.json")

    class _EmptyClient:
        def geocode(self, address):
            return []

    with pytest.raises(ValueError):
        extract.geocode_address("adresse introuvable", _client=_EmptyClient())


class _FakeDistanceClient:
    def __init__(self, status="OK", meters=8400):
        self.status, self.meters = status, meters

    def distance_matrix(self, origins, destinations, mode):
        return {
            "rows": [{"elements": [{"status": self.status, "distance": {"value": self.meters}}]}]
        }


def test_compute_commute_distance_converts_to_km():
    client = _FakeDistanceClient(meters=8400)
    km = extract.compute_commute_distance((43.6, 3.9), "Marche/running", _client=client)
    assert km == 8.4


def test_compute_commute_distance_unknown_mode_raises():
    with pytest.raises(ValueError):
        extract.compute_commute_distance((43.6, 3.9), "Téléportation", _client=_FakeDistanceClient())


def test_compute_commute_distance_not_found_raises():
    client = _FakeDistanceClient(status="NOT_FOUND")
    with pytest.raises(ValueError):
        extract.compute_commute_distance((43.6, 3.9), "Marche/running", _client=client)
