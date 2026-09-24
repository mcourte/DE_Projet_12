# POC Avantages Sportifs — Sport Data Solution

Pipeline de données de bout en bout pour tester la faisabilité d'un
système de récompenses sportives pour les salariés (prime sportive,
journées bien-être), avec calcul de l'impact financier et restitution
Power BI.

Stack 100% open source (hors Power BI, imposé par la mission) : voir
la justification des choix dans [`docs/RAPPORT.md`](docs/RAPPORT.md).

## Structure du projet

```
config/
  config.yaml              Paramètres métier (taux, seuils, distances) — non figés
docker-compose.yml          Infra locale : PostgreSQL, NATS, Grafana (open source)
data/
  raw/                      Fichiers sources (Données RH.xlsx, Données Sportive.xlsx)
  bronze/                   Extraits Parquet lus par dbt
  warehouse.duckdb          Entrepôt analytique DuckDB (silver/gold, généré par dbt)
dbt/
  dbt_project.yml           Paramètres métier exposés en Jinja (vars)
  profiles.yml.example      A copier en profiles.yml (non versionné)
  models/
    bronze/                 Copie brute des sources (vue)
    silver/                 Formule A (anomalies de distance), activités nettoyées
    gold/                   Formules B et C, table finale gold_kpi
    schema.yml               Tests dbt (unicité, valeurs acceptées, plages)
src/
  config.py                 M0 - Chargement des paramètres
  generator.py               M1 - Génération de l'historique d'activités + publication NATS
  extract.py                 M2 - Extraction RH/sportif/Postgres/géocodage -> Parquet bronze
  quality_checks.py          M3 - Déclenchement et lecture des tests dbt
  transform.py                M4 - Déclenchement dbt run, replay historique
  load.py                     M5 - Upsert référentiel, export gold -> PostgreSQL
  notifier.py                  M6 - Abonnement NATS -> publication Slack
  pipeline.py                  M7 - Point d'entrée CLI simple
  orchestration/
    dagster_definitions.py    M7 - Orchestration Dagster (assets, planning)
  monitoring.py                M8 - Métriques d'exécution -> PostgreSQL (lu par Grafana)
  security.py                  M9 - Chiffrement/accès/audit (PostgreSQL natif)
tests/                        Tests unitaires (mirroring de src/)
notebooks/                    Exploration / prototypage
powerbi/                      Modèle et rapport Power BI
docs/
  RAPPORT.md                  Note de synthèse (contexte, formules, architecture, modules)
  A_FAIRE_notion.md            Checklist + formules + données manquantes (format Notion)
```

## Installation

```bash
docker compose up -d          # PostgreSQL, NATS, Grafana
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
cp dbt/profiles.yml.example dbt/profiles.yml
cd dbt && dbt deps && dbt build
```

## Paramètres

Tous les seuils métier (taux de prime, nombre de jours bien-être,
seuil d'activités, distances maximales) sont définis dans
`config/config.yaml` **et** dans `dbt/dbt_project.yml` (vars) — aucun
ne doit être codé en dur dans `src/` ou dans les modèles SQL.
