from google.cloud import bigquery
from common.log_config import setup_logger

# Create a logger for this script
logger = setup_logger(__name__)

schema = [
  {
    "name": "FILEDATE",
    "type": "DATE"
  },
  {
    "name": "DATE",
    "type": "STRING"
  },
  {
    "name": "NUMARTS",
    "type": "INTEGER"
  },
  {
    "name": "COUNTS",
    "type": "STRING"
  },
  {
    "name": "THEMES",
    "type": "STRING"
  },
  {
    "name": "LOCATIONS",
    "type": "STRING"
  },
  {
    "name": "PERSONS",
    "type": "STRING"
  },
  {
    "name": "ORGANIZATIONS",
    "type": "STRING"
  },
  {
    "name": "TONE",
    "type": "STRING"
  },
  {
    "name": "CAMEOEVENTIDS",
    "type": "STRING"
  },
  {
    "name": "SOURCES",
    "type": "STRING"
  },
  {
    "name": "SOURCEURLS",
    "type": "STRING"
  }
]
def load_csv_to_bigquery(gcs_uri, table_id, project_id, schema=None, partition_field="FILEDATE"):
    """
    Loads a CSV file from Google Cloud Storage into a BigQuery table.

    Parameters:
    - bucket_name: str, name of the GCS bucket.
    - file_name: str, name of the file in the GCS bucket.
    - dataset_id: str, BigQuery dataset ID.
    - table_id: str, BigQuery table ID.
    - schema: list of bigquery.SchemaField, optional, BigQuery table schema. Default is provided in the function.
    - partition_field: str, optional, the field to partition the table by. Default is 'FILEDATE'.
    """

    try:
        logger.info("Creating BigQuery client...")
        with bigquery.Client(project=project_id) as bq_client:

            # Configure the load job
            job_config = bigquery.LoadJobConfig(
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                autodetect=False,
                skip_leading_rows=1,
                source_format=bigquery.SourceFormat.CSV,
                schema=schema,
                time_partitioning=bigquery.TimePartitioning(
                    type_=bigquery.TimePartitioningType.DAY,
                    field=partition_field
                ),
                field_delimiter='\t',
                ignore_unknown_values=True,
                allow_quoted_newlines=True
            )

            logger.info(f"Starting load job for {gcs_uri} into {table_id}...")

            # Start the load job
            load_job = bq_client.load_table_from_uri(
                gcs_uri, table_id, job_config=job_config
            )

            # Wait for the job to complete
            load_job.result()

            logger.info(f"Successfully loaded {load_job.output_rows} rows into {table_id}")
            return load_job.output_rows
    except Exception as e:
        logger.error(f"Error loading data into BigQuery: {str(e)}")
        raise
# Usage
if __name__ == "__main__":
    bucket_name = "factored-hackaton-2024"
    file_name = "gdelt_gkg/20240822.gkg.csv"
    gcs_uri = f"gs://{bucket_name}/{file_name}"
    table_id = "gdelt.gkg_raw$20240822"

    load_csv_to_bigquery(gcs_uri, table_id,schema=schema)
