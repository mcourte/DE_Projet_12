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
.github/workflows/          CI : dbt build sur jeu de données de test à chaque modification de dbt/
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
  tests/                    Test dbt anti-triche : pas d'activités qui se chevauchent
src/
  config.py                 M0 - Chargement des paramètres
  generator.py               M1 - Génération de l'historique d'activités + publication NATS
  extract.py                 M2 - Extraction RH/sportif/Postgres/géocodage -> Parquet bronze
  quality_checks.py          M3 - Déclenchement et lecture des tests dbt
  transform.py                M4 - Déclenchement dbt run, replay historique
  load.py                     M5 - Upsert référentiel, export gold -> PostgreSQL
  notifier.py                  M6 - Abonnement NATS -> publication Slack
  pipeline.py                  M7 - Point d'entrée CLI (run / replay)
  orchestration/
    dagster_definitions.py    M7 - Orchestration Dagster (assets, planning quotidien 6h)
  monitoring.py                M8 - Métriques d'exécution -> PostgreSQL (lu par Grafana), alertes Slack
  security.py                  M9 - Chiffrement/accès/audit (PostgreSQL natif)
tests/                        65 tests unitaires (mirroring de src/) + jeu de test tests/fixtures/bronze
POC_Avantages_Sportifs.pbix      Rapport Power BI (connecté à gold.gold_kpi)
grafana/
  provisioning/              Source de données PostgreSQL + chargeur de dashboards (auto)
  dashboards/                Dashboard de monitoring du pipeline (JSON versionné)
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

## Utilisation

```bash
cp .env.example .env                      # puis renseigner SLACK_BOT_TOKEN et GOOGLE_MAPS_API_KEY
python -m src.pipeline run                # extraction + distances Google Maps + dbt + tests + export gold
python -m src.pipeline replay --taux-prime 0.10   # rejoue l'historique avec un nouveau taux, puis exporte
python -m src.notifier                    # processus long : écoute NATS et publie sur Slack
python -m src.generator live --salarie 18918      # insère une activité (démo) : base + NATS -> Slack
pytest                                    # 65 tests (attention : remplace les données chargées par un jeu de test)
```

`python -m src.pipeline run` utilise toujours le taux officiel de `config/config.yaml` ; `replay --taux-prime`
ne sert qu'à simuler un autre taux. Un orchestrateur Dagster est aussi fourni (`src/orchestration/`), planifié
tous les jours à 6h.

- **Power BI** : ouvrir `POC_Avantages_Sportifs.pbix` (ou se connecter à PostgreSQL `localhost:5432`, base `sportdata`,
  table `gold.gold_kpi`), puis *Actualiser*.
- **Grafana** : http://localhost:3000 (dashboard « Monitoring du pipeline »). La source de données et le
  dashboard sont provisionnés automatiquement depuis `grafana/` ; les métriques viennent de `monitoring.pipeline_runs`.
- **Alertes d'échec** : message `[CRITICAL] …` dans le channel Slack privé `#pipeline_alerte`. Un channel privé
  s'adresse par son identifiant (`slack.alert_channel` dans `config/config.yaml`), et le bot doit y être invité
  (`/invite @nom-du-bot`).
- **Clés** : `.env` (non versionné) contient `SLACK_BOT_TOKEN`, `GOOGLE_MAPS_API_KEY`, `POSTGRES_*` et
  `PGCRYPTO_PASSPHRASE`.

## Résultats du POC (données générées, vraies distances Google Maps)

161 salariés · 5 240 activités · 68 éligibles à la prime (172 482,50 € au taux de 5 %) · 83 éligibles aux
journées bien-être (415 jours) · 20/20 tests dbt · 0 anomalie de distance.

## Pistes d'évolution (hors périmètre du POC)

- Email à la RH en cas d'anomalie de distance (`monitoring.notify_geocoding_anomalies`, non implémentée).
- Vérification de la régularité des trajets avec l'historique Strava réel (formule A).
- Politique de nouvelle tentative automatique en cas d'échec de l'API Google Maps.

## Paramètres

Tous les seuils métier (taux de prime, nombre de jours bien-être,
seuil d'activités, distances maximales) sont définis dans
`config/config.yaml` **et** dans `dbt/dbt_project.yml` (vars) — aucun
ne doit être codé en dur dans `src/` ou dans les modèles SQL.
