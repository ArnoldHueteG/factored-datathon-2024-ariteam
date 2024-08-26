CREATE OR REPLACE TABLE
  factored-hackaton-2024.gdelt.t_economic_news AS
SELECT
  *,
  news_category AS Economics
FROM
  factored-hackaton-2024.gdelt.extended_gkg_raw
WHERE
  Main_Category = 'Economics';