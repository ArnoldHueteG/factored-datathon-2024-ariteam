
CREATE OR REPLACE MODEL `gdelt.text-embedding-004`
  REMOTE WITH CONNECTION `us-east1.connection-for-embeddings`
  OPTIONS (ENDPOINT = 'text-embedding-004');

CREATE OR REPLACE PROCEDURE `factored-hackaton-2024.gdelt.process_gkg_embeddings`()
BEGIN
  DECLARE batch_size INT64 DEFAULT 600;
  DECLARE max_id INT64;
  DECLARE current_start_id INT64 DEFAULT 0;
  DECLARE current_end_id INT64;

  -- Get the maximum ID
  SET max_id = (
    SELECT CAST(MAX(id) AS INT64)
    FROM `factored-hackaton-2024.gdelt.gkg_sources_result_with_entities`
  );

  -- Set the starting ID based on the max ID in the result table
  SET current_start_id = (
    SELECT IFNULL(CAST(MAX(id) AS INT64), 0)
    FROM `factored-hackaton-2024.gdelt.gkg_with_embeddings_v2`
  );

  -- Process batches
  WHILE current_start_id < max_id DO
    -- Calculate the end ID for the current batch
    SET current_end_id = LEAST(current_start_id + batch_size, max_id);

    -- If it's the first batch, create the table instead of inserting
    IF current_start_id = 0 THEN
      EXECUTE IMMEDIATE '''
        CREATE OR REPLACE TABLE `factored-hackaton-2024.gdelt.gkg_with_embeddings_v2` AS
        SELECT id, content, first_sourceurl, numarts, ml_generate_embedding_result, ml_generate_embedding_statistics
        FROM
          ML.GENERATE_EMBEDDING(
            MODEL `factored-hackaton-2024.gdelt.embedding_model`,
            (SELECT text as content, id, first_sourceurl, numarts
             FROM `factored-hackaton-2024.gdelt.gkg_sources_result_with_entities`
             WHERE CAST(id AS INT64) > 0 AND CAST(id AS INT64) <= ?
             ORDER BY CAST(id AS INT64)),
            STRUCT(TRUE AS flatten_json_output, 'SEMANTIC_SIMILARITY' as task_type)
          )
      ''' USING current_end_id;
    ELSE
      -- For subsequent batches, insert into the existing table
      INSERT INTO `factored-hackaton-2024.gdelt.gkg_with_embeddings_v2` (id, content, first_sourceurl, numarts, ml_generate_embedding_result, ml_generate_embedding_statistics)
      SELECT id, content, first_sourceurl, numarts, ml_generate_embedding_result, ml_generate_embedding_statistics
      FROM
        ML.GENERATE_EMBEDDING(
          MODEL `factored-hackaton-2024.gdelt.embedding_model`,
          (SELECT text as content, id, first_sourceurl, numarts
           FROM `factored-hackaton-2024.gdelt.gkg_sources_result_with_entities`
           WHERE CAST(id AS INT64) > current_start_id AND CAST(id AS INT64) <= current_end_id
           ORDER BY CAST(id AS INT64)),
          STRUCT(TRUE AS flatten_json_output, 'SEMANTIC_SIMILARITY' as task_type)
        );
    END IF;

    -- Move to the next batch
    SET current_start_id = current_end_id;
  END WHILE;
END;