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

from dagster import AssetExecutionContext, Definitions, ScheduleDefinition, asset, define_asset_job

from src import extract, load, pipeline, quality_checks, transform


@asset
def access_check() -> None:
    """Vérifie que le rôle PostgreSQL du pipeline a le droit de lire les
    données avant toute extraction (cf. src/security.py -> apply_access_control).
    """
    pipeline.check_access()


@asset(deps=[access_check])
def bronze_employees(context: AssetExecutionContext) -> None:
    """Extrait le référentiel RH + géocodage/distance, met à jour le
    référentiel PostgreSQL (salaire et adresse chiffrés), écrit en Parquet
    (cf. src/extract.py -> extract_rh_referential, write_bronze_parquet).
    """
    df = extract.extract_employees_with_distance()
    pipeline.audit("read", "referentiel_rh")
    pipeline.sync_employees(df)
    pipeline.audit("write_encrypted", "employees")
    extract.write_bronze_parquet(df, "employees")
    context.add_output_metadata({"row_count": len(df)})


@asset
def bronze_sport_declare(context: AssetExecutionContext) -> None:
    """Extrait le sport déclaré par salarié, écrit en Parquet."""
    df = extract.extract_sport_referential()
    extract.write_bronze_parquet(df, "sport_declare")
    context.add_output_metadata({"row_count": len(df)})


@asset(deps=[access_check])
def bronze_activities(context: AssetExecutionContext) -> None:
    """Extrait l'historique d'activités depuis PostgreSQL, écrit en
    Parquet (cf. src/extract.py -> extract_activities_from_postgres).
    """
    df = extract.extract_activities_from_postgres()
    pipeline.audit("read", "activities")
    extract.write_bronze_parquet(df, "activities")
    context.add_output_metadata({"row_count": len(df)})


@asset(deps=[bronze_employees, bronze_sport_declare, bronze_activities])
def dbt_gold_kpi() -> None:
    """Déclenche `dbt run` + `dbt test` (cf. src/transform.py ->
    run_dbt_transform, src/quality_checks.py -> run_dbt_tests) pour
    matérialiser bronze -> silver -> gold dans DuckDB.

    Si les tests échouent, cet asset échoue et Dagster n'exécute pas
    `gold_kpi_in_postgres` : une donnée invalide n'atteint jamais
    Power BI silencieusement.
    """
    transform.run_dbt_transform()
    if not quality_checks.run_dbt_tests():
        failures = quality_checks.parse_dbt_run_results()
        raise RuntimeError(f"{len(failures)} test(s) dbt en échec : {failures}")


@asset(deps=[dbt_gold_kpi])
def gold_kpi_in_postgres(context: AssetExecutionContext) -> None:
    """Exporte gold_kpi de DuckDB vers PostgreSQL pour Power BI
    (cf. src/load.py -> export_gold_to_postgres).
    """
    row_count = load.export_gold_to_postgres()
    pipeline.audit("export", "gold.gold_kpi")
    context.add_output_metadata({"row_count": row_count})


daily_kpi_job = define_asset_job("daily_kpi_job", selection="*")

daily_kpi_schedule = ScheduleDefinition(
    job=daily_kpi_job,
    cron_schedule="0 6 * * *",  # tous les jours à 6h — à ajuster selon le besoin réel
)

defs = Definitions(
    assets=[access_check, bronze_employees, bronze_sport_declare, bronze_activities, dbt_gold_kpi, gold_kpi_in_postgres],
    schedules=[daily_kpi_schedule],
)
