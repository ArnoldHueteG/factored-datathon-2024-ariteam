
-- Tabla para noticias sociales
CREATE OR REPLACE TABLE
  factored-hackaton-2024.gdelt.t_social_news AS
SELECT
  *,
  news_category AS Social
FROM
  factored-hackaton-2024.gdelt.extended_gkg_raw
WHERE
  Main_Category = 'Social';