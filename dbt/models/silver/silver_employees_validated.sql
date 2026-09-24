-- Formule A : marque les déclarations de mode de déplacement dont la
-- distance domicile-bureau dépasse le seuil du mode déclaré.
-- On NE supprime PAS la ligne : l'anomalie doit être remontée, pas cachée.
--
-- AXE D'AMÉLIORATION (bloqué tant que Strava n'est pas connecté) : cette
-- formule ne valide que la COHÉRENCE géographique de la déclaration, pas
-- sa RÉGULARITÉ. La note de cadrage exige un mode actif "la majorité du
-- temps", mais bronze_employees ne contient qu'une déclaration statique,
-- pas un journal quotidien des trajets. Une fois l'historique Strava réel
-- disponible, ajouter ici une jointure sur les trajets réels pour calculer
-- une fréquence, sur le même principe que gold_wellbeing.sql pour la C.

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
