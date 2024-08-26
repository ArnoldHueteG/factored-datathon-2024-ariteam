CREATE OR REPLACE TABLE
  factored-hackaton-2024.gdelt.political_news AS
SELECT
  *,
  news_category AS Political
FROM
  factored-hackaton-2024.gdelt.extended_gkg_raw
WHERE
  Main_Category = 'Political';