"""M7 - Orchestration (Dagster).

Modélise le pipeline batch comme une chaîne d'assets : chaque étape
déclare ce qu'elle produit et de quoi elle dépend, Dagster se charge de
l'ordre d'exécution, du planning et de la reprise sur erreur — à la
place d'Airflow, pour une boucle de développement locale plus légère
sur un POC solo (les deux sont Apache 2.0, open source).

La notification Slack (src/notifier.py) n'est PAS un asset ici : elle
tourne en continu comme un processus séparé, abonné à NATS, découplée
du planning batch de ces assets.
"""

# TODO :
# 1. bronze_employees / bronze_activities : appeler les fonctions de extract.py (side-effect : écrit en Parquet)
# 2. dbt_gold_kpi : lancer dbt run + dbt test (subprocess, ou dagster-dbt si on veut le lignage dans l'UI Dagster)
# 3. gold_kpi_in_postgres : appeler load.export_gold_to_postgres()
# 4. Ajuster cron_schedule une fois la fréquence réelle validée avec Juliette (quotidien par défaut)
# 5. Tester en local avec `dagster dev` (installe dagster-webserver, déjà dans requirements.txt)

from dagster import asset, Definitions, ScheduleDefinition, define_asset_job


@asset
def bronze_employees() -> None:
    """Extrait le référentiel RH + géocodage/distance, écrit en Parquet
    (cf. src/extract.py -> extract_rh_referential, write_bronze_parquet).
    """
    raise NotImplementedError


@asset
def bronze_activities() -> None:
    """Extrait l'historique d'activités depuis PostgreSQL, écrit en
    Parquet (cf. src/extract.py -> extract_activities_from_postgres).
    """
    raise NotImplementedError


@asset(deps=[bronze_employees, bronze_activities])
def dbt_gold_kpi() -> None:
    """Déclenche `dbt run` + `dbt test` (cf. src/transform.py ->
    run_dbt_transform, src/quality_checks.py -> run_dbt_tests) pour
    matérialiser bronze -> silver -> gold dans DuckDB.
    """
    raise NotImplementedError


@asset(deps=[dbt_gold_kpi])
def gold_kpi_in_postgres() -> None:
    """Exporte gold_kpi de DuckDB vers PostgreSQL pour Power BI
    (cf. src/load.py -> export_gold_to_postgres).
    """
    raise NotImplementedError


daily_kpi_job = define_asset_job("daily_kpi_job", selection="*")

daily_kpi_schedule = ScheduleDefinition(
    job=daily_kpi_job,
    cron_schedule="0 6 * * *",   # tous les jours à 6h — à ajuster selon le besoin réel
)

defs = Definitions(
    assets=[bronze_employees, bronze_activities, dbt_gold_kpi, gold_kpi_in_postgres],
    schedules=[daily_kpi_schedule],
)
