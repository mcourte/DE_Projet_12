select *
from {{ ref('bronze_activities') }}
where (distance_m is null or distance_m >= 0)
  and date_fin >= date_debut
  and date_debut <= cast(current_timestamp as timestamp)
