-- Copie brute du référentiel RH, enrichi des coordonnées et de la
-- distance domicile-bureau calculées côté Python (src/extract.py,
-- geocode_address + compute_commute_distance) avant écriture en Parquet.
-- Aucune règle métier ici : c'est juste l'entrée, telle quelle.

select
    id_salarie,
    nom,
    prenom,
    bu,
    date_embauche,
    salaire_brut,
    type_contrat,
    adresse_domicile,
    moyen_deplacement,
    distance_domicile_bureau_km   -- calculée en amont via l'API Google Maps
from read_parquet('{{ var("bronze_employees_path", "../data/bronze/employees/*.parquet") }}')
