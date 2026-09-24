-- Couche silver des activités : exclut ce que les tests dbt (schema.yml)
-- ont déjà signalé comme invalide (distance négative, dates incohérentes),
-- pour que le comptage de la formule C ne compte que des activités saines.

select *
from {{ ref('bronze_activities') }}
where (distance_m is null or distance_m >= 0)
  and date_fin >= date_debut
  -- current_timestamp, pas current_date : sinon une activité du jour même
  -- (qui a une heure > 00:00:00) est exclue à tort par comparaison à minuit.
  and date_debut <= cast(current_timestamp as timestamp)
