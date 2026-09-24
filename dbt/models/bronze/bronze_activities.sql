-- Copie brute de l'historique d'activités (généré façon Strava par
-- src/generator.py, ou à terme importé depuis l'API Strava réelle).

select
    id,
    id_salarie,
    date_debut,
    date_fin,
    type as sport_type,
    distance_m,
    commentaire
from read_parquet('{{ var("bronze_activities_path", "../data/bronze/activities/*.parquet") }}')
