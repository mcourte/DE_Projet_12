-- Formule B : prime sportive = 5% du salaire brut annuel, si mode de
-- déplacement actif déclaré ET distance validée par la formule A.

select
    id_salarie,
    bu,
    salaire_brut,
    moyen_deplacement,
    anomalie_distance,
    (
        moyen_deplacement in ('Marche/running', 'Vélo/Trottinette/Autres')
        and not anomalie_distance
    ) as eligible_prime,
    case
        when moyen_deplacement in ('Marche/running', 'Vélo/Trottinette/Autres')
             and not anomalie_distance
        then salaire_brut * {{ var('taux_prime') }}
        else 0
    end as montant_prime,
    {{ var('taux_prime') }} as taux_prime_applique
from {{ ref('silver_employees_validated') }}
