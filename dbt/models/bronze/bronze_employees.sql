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
    distance_domicile_bureau_km
from read_parquet('{{ var("bronze_employees_path", "../data/bronze/employees/*.parquet") }}')
