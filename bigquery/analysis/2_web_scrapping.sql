drop table `factored-hackaton-2024`.gdelt.gkg_sources;
create table `factored-hackaton-2024`.gdelt.gkg_sources as
select ROW_NUMBER() OVER (ORDER BY total_numarts desc) AS id, first_sourceurl,total_numarts
from (select first_sourceurl,sum(numarts) total_numarts from `factored-hackaton-2024`.gdelt.gkg_transformed
group by first_sourceurl) as gkg;

CREATE OR REPLACE FUNCTION
	`factored-hackaton-2024.gdelt.get_articles`(url STRING, threads INT64) RETURNS JSON
REMOTE WITH CONNECTION `factored-hackaton-2024.us-east1.remote-functions`
OPTIONS (endpoint = 'https://bq-functions-did3mj6ema-ue.a.run.app', max_batching_rows = 100);

CREATE OR REPLACE PROCEDURE `factored-hackaton-2024.gdelt.process_gkg_sources`()
BEGIN
  DECLARE batch_size INT64 DEFAULT 600;
  DECLARE max_id INT64;
  DECLARE current_start_id INT64 DEFAULT 0;
  DECLARE current_end_id INT64;

  -- Get the maximum ID
  SET max_id = (
    SELECT CAST(MAX(id) AS INT64)
    FROM `factored-hackaton-2024.gdelt.gkg_sources`
  );
  -- Set the starting ID based on the max ID in the result table
  SET current_start_id = (
    SELECT IFNULL(CAST(MAX(id) AS INT64), 0)
    FROM `factored-hackaton-2024.gdelt.gkg_sources_result`
  );
  -- Process batches
  WHILE current_start_id < max_id DO
    -- Calculate the end ID for the current batch
    SET current_end_id = LEAST(current_start_id + batch_size, max_id);

    -- Process the current batch
    INSERT INTO `factored-hackaton-2024.gdelt.gkg_sources_result`
    SELECT
      id,
      first_sourceurl,
      JSON_EXTRACT_SCALAR(textdata, '$.title') AS title,
      JSON_EXTRACT_SCALAR(textdata, '$.text') AS text,
      JSON_EXTRACT_SCALAR(textdata, '$.summary') AS summary,
      CAST(JSON_EXTRACT_SCALAR(textdata, '$.download') AS FLOAT64) AS download,
      CAST(JSON_EXTRACT_SCALAR(textdata, '$.parse') AS FLOAT64) AS parse,
      CAST(JSON_EXTRACT_SCALAR(textdata, '$.nlp') AS FLOAT64) AS nlp,
      CAST(JSON_EXTRACT_SCALAR(textdata, '$.total') AS FLOAT64) AS total
    FROM (
      SELECT id, first_sourceurl, `factored-hackaton-2024.gdelt.get_articles`(first_sourceurl, 20) AS textdata
      FROM `factored-hackaton-2024.gdelt.gkg_sources`
      WHERE CAST(id AS INT64) > current_start_id AND CAST(id AS INT64) <= current_end_id
      ORDER BY CAST(id AS INT64)
    );

    -- Move to the next batch
    SET current_start_id = current_end_id;
  END WHILE;
END;

call `factored-hackaton-2024.gdelt.process_gkg_sources`();

create or replace table factored-hackaton-2024.gdelt.gkg_sources_result_with_entities as
select
*
from (
SELECT c.total_numarts,a.*,b.numarts,b.themes,b.locations,b.persons,b.organizations,b.tone,b.sources,
row_number() over(partition by a.first_sourceurl order by b.numarts desc) rn
FROM factored-hackaton-2024.gdelt.gkg_sources_result a
left join factored-hackaton-2024.gdelt.gkg_transformed b
on a.first_sourceurl=b.first_sourceurl
left join factored-hackaton-2024.gdelt.gkg_sources c
on a.id = c.id
where a.title <> 'ERROR' and a.text <>''
) a where a.rn=1
order by a.id;

