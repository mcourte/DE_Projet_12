-- Anti-triche sur la formule C (5 journées bien-être).
-- Un salarié ne peut pas faire deux activités en même temps : ce test échoue
-- (renvoie des lignes) si deux activités du même salarié se chevauchent dans
-- le temps, symptôme d'une déclaration frauduleuse visant à gonfler le
-- nombre d'activités sur 12 mois au-delà du seuil de gold_wellbeing.sql.
--
-- Un test dbt doit renvoyer zéro ligne pour passer.

select
    a.id_salarie,
    a.id as activite_1,
    b.id as activite_2,
    a.date_debut as debut_1,
    a.date_fin as fin_1,
    b.date_debut as debut_2,
    b.date_fin as fin_2
from {{ ref('silver_activities_clean') }} a
join {{ ref('silver_activities_clean') }} b
    on a.id_salarie = b.id_salarie
    and a.id < b.id
    and a.date_debut < b.date_fin
    and b.date_debut < a.date_fin
