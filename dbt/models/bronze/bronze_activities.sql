select
    id,
    id_salarie,
    date_debut,
    date_fin,
    type as sport_type,
    distance_m,
    commentaire
from read_parquet('{{ var("bronze_activities_path", "../data/bronze/activities/*.parquet") }}')
