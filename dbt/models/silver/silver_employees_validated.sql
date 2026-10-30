select
    e.*,
    case
        when e.moyen_deplacement = 'Marche/running'
             and e.distance_domicile_bureau_km > {{ var('max_km_marche') }}
            then true
        when e.moyen_deplacement = 'Vélo/Trottinette/Autres'
             and e.distance_domicile_bureau_km > {{ var('max_km_velo') }}
            then true
        else false
    end as anomalie_distance
from {{ ref('bronze_employees') }} e
