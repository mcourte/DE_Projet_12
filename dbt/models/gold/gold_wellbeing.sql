-- Formule C : 5 jours bien-être si au moins 15 activités sur les
-- 12 derniers mois glissants (par rapport à la date du run).

-- Basé sur la liste COMPLÈTE des salariés (silver_employees_validated),
-- pas seulement ceux qui ont au moins une activité : un salarié sans
-- aucune activité doit obtenir 0/non-éligible, pas une ligne absente
-- (qui deviendrait NULL après la jointure dans gold_kpi.sql — ambigu
-- entre "non calculé" et "vraiment zéro").

with activity_counts as (
    select
        id_salarie,
        count(*) as nb_activites_12_mois
    from {{ ref('silver_activities_clean') }}
    where date_debut >= current_date - interval '{{ var("fenetre_glissante_mois", 12) }} months'
    group by id_salarie
),

employees as (
    select id_salarie from {{ ref('silver_employees_validated') }}
)

select
    e.id_salarie,
    coalesce(ac.nb_activites_12_mois, 0) as nb_activites_12_mois,
    coalesce(ac.nb_activites_12_mois, 0) >= {{ var('seuil_activites_par_an') }} as eligible_bien_etre,
    case
        when coalesce(ac.nb_activites_12_mois, 0) >= {{ var('seuil_activites_par_an') }}
        then {{ var('jours_bien_etre') }}
        else 0
    end as jours_bien_etre_accordes
from employees e
left join activity_counts ac on ac.id_salarie = e.id_salarie
