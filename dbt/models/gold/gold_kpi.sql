-- Table finale consommée par Power BI : une ligne par salarié, les deux
-- formules réunies, plus la version des paramètres utilisée pour ce run.
-- C'est cette colonne `run_id`/`taux_prime_applique` qui permet de
-- relancer l'historique (replay_historical_kpis) sans écraser le passé.

select
    e.id_salarie,
    e.bu,
    e.moyen_deplacement,
    e.distance_domicile_bureau_km,
    -- Formule A : true quand la distance domicile-bureau est impossible
    -- pour le mode déclaré (ex. 50 km à pied). Remontée telle quelle pour
    -- que Power BI / Postgres puissent lister les déclarations à vérifier.
    e.anomalie_distance,
    p.eligible_prime,
    p.montant_prime,
    p.taux_prime_applique,
    w.nb_activites_12_mois,
    w.eligible_bien_etre,
    w.jours_bien_etre_accordes,
    current_timestamp as calcule_le
from {{ ref('silver_employees_validated') }} e
left join {{ ref('gold_prime') }} p on p.id_salarie = e.id_salarie
left join {{ ref('gold_wellbeing') }} w on w.id_salarie = e.id_salarie
