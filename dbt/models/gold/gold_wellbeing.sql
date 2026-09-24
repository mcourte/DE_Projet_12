-- Formule C : 5 jours bien-être si au moins 15 activités sur les
-- 12 derniers mois glissants (par rapport à la date du run).

with activity_counts as (
    select
        id_salarie,
        count(*) as nb_activites_12_mois
    from {{ ref('silver_activities_clean') }}
    where date_debut >= current_date - interval '{{ var("fenetre_glissante_mois", 12) }} months'
    group by id_salarie
)

select
    id_salarie,
    coalesce(nb_activites_12_mois, 0) as nb_activites_12_mois,
    coalesce(nb_activites_12_mois, 0) >= {{ var('seuil_activites_par_an') }} as eligible_bien_etre,
    case
        when coalesce(nb_activites_12_mois, 0) >= {{ var('seuil_activites_par_an') }}
        then {{ var('jours_bien_etre') }}
        else 0
    end as jours_bien_etre_accordes
from activity_counts
