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


class _FakeMapsClient(_FakeGeocodeClient, _FakeDistanceClient):
    def __init__(self, fail_addresses=()):
        _FakeGeocodeClient.__init__(self)
        _FakeDistanceClient.__init__(self, meters=12000)
        self.fail_addresses = set(fail_addresses)

    def geocode(self, address):
        if address in self.fail_addresses:
            return []
        return _FakeGeocodeClient.geocode(self, address)


def _employees(addresses):
    return pd.DataFrame(
        {"adresse_domicile": addresses, "moyen_deplacement": ["Vélo/Trottinette/Autres"] * len(addresses)}
    )


def test_enrich_with_commute_distance_adds_km_column_and_caches(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "_GEOCODE_CACHE_PATH", tmp_path / "geo.json")
    monkeypatch.setattr(extract, "_DISTANCE_CACHE_PATH", tmp_path / "dist.json")
    client = _FakeMapsClient()

    result = extract.enrich_with_commute_distance(_employees(["a", "b"]), _client=client)
    assert list(result["distance_domicile_bureau_km"]) == [12.0, 12.0]
    assert client.calls == 2

    extract.enrich_with_commute_distance(_employees(["a", "b"]), _client=client)
    assert client.calls == 2


def test_enrich_with_commute_distance_tolerates_few_failures(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "_GEOCODE_CACHE_PATH", tmp_path / "geo.json")
    monkeypatch.setattr(extract, "_DISTANCE_CACHE_PATH", tmp_path / "dist.json")
    client = _FakeMapsClient(fail_addresses=["introuvable"])

    addresses = [f"adresse {i}" for i in range(19)] + ["introuvable"]
    result = extract.enrich_with_commute_distance(_employees(addresses), _client=client)

    assert result["distance_domicile_bureau_km"].isna().sum() == 1


def test_enrich_with_commute_distance_raises_when_too_many_failures(tmp_path, monkeypatch):
    monkeypatch.setattr(extract, "_GEOCODE_CACHE_PATH", tmp_path / "geo.json")
    monkeypatch.setattr(extract, "_DISTANCE_CACHE_PATH", tmp_path / "dist.json")
    client = _FakeMapsClient(fail_addresses=["x", "y"])

    with pytest.raises(RuntimeError):
        extract.enrich_with_commute_distance(_employees(["x", "y", "ok"]), _client=client)


def test_raise_if_too_many_failures_only_above_threshold():
    extract._raise_if_too_many_failures(1, 20)
    extract._raise_if_too_many_failures(0, 0)
    with pytest.raises(RuntimeError):
        extract._raise_if_too_many_failures(3, 20)


def test_resolve_distance_km_reads_cache_without_calling_api(monkeypatch):
    def boom(*args, **kwargs):
        raise AssertionError("l'API ne doit pas être appelée")

    monkeypatch.setattr(extract, "geocode_address", boom)
    row = {"adresse_domicile": "1 rue A", "moyen_deplacement": "Marche/running"}

    assert extract._resolve_distance_km(row, {"1 rue A|Marche/running": 4.2}) == 4.2


def test_resolve_distance_km_stores_result_in_cache(monkeypatch):
    monkeypatch.setattr(extract, "geocode_address", lambda address, _client=None: (43.6, 3.9))
    monkeypatch.setattr(extract, "compute_commute_distance", lambda coords, mode, _client=None: 7.5)
    row = {"adresse_domicile": "2 rue B", "moyen_deplacement": "Vélo/Trottinette/Autres"}
    cache = {}

    assert extract._resolve_distance_km(row, cache) == 7.5
    assert cache == {"2 rue B|Vélo/Trottinette/Autres": 7.5}


def test_travel_mode_translates_and_rejects_unknown():
    assert extract._travel_mode("Marche/running") == "walking"
    with pytest.raises(ValueError):
        extract._travel_mode("Téléportation")
