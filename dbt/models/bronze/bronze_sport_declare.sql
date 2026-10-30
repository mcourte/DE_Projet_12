select
    id_salarie,
    pratique_sport
from read_parquet('{{ var("bronze_sport_path", "../data/bronze/sport_declare/*.parquet") }}')
