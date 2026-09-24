-- Sport favori déclaré par salarié (fichier "Données Sportive").
-- Rappel : ce n'est PAS un historique d'activités, seulement un point de
-- départ pour rendre la simulation cohérente — 66/161 salariés n'ont
-- rien déclaré, ce qui doit être géré en amont par le générateur.

select
    id_salarie,
    pratique_sport
from read_parquet('{{ var("bronze_sport_path", "../data/bronze/sport_declare/*.parquet") }}')
