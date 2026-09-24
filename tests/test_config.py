import textwrap

from src import config


def _write_config(tmp_path, content):
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content), encoding="utf-8")
    return str(p)


def test_get_param_reads_nested_key(tmp_path):
    path = _write_config(
        tmp_path,
        """
        formule_prime:
          taux_prime: 0.05
        """,
    )
    assert config.get_param("formule_prime.taux_prime", path=path) == 0.05


def test_get_param_missing_key_returns_default(tmp_path):
    path = _write_config(tmp_path, "formule_prime:\n  taux_prime: 0.05\n")
    assert config.get_param("formule_prime.inexistant", default="x", path=path) == "x"


def test_get_param_missing_file_returns_default():
    assert config.get_param("a.b", default="fallback", path="does/not/exist.yaml") == "fallback"


def test_load_config_is_cached(tmp_path, monkeypatch):
    path = _write_config(tmp_path, "a: 1\n")
    first = config.load_config(path)
    # On modifie le fichier après le premier chargement : le cache ne doit
    # pas relire le disque tant que le chemin résolu est le même.
    with open(path, "w", encoding="utf-8") as f:
        f.write("a: 2\n")
    second = config.load_config(path)
    assert first is second
    assert second["a"] == 1
