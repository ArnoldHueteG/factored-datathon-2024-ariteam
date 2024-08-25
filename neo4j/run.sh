gsutil cp neo4j/credentials_enterprise.json gs://factored-hackaton-2024/neo4j/credentials_enterprise.json
gsutil cp neo4j/gkg.json gs://factored-hackaton-2024/neo4j/gkg.json

gcloud dataflow flex-template run load-to-neo4j-v1 \
--template-file-gcs-location gs://dataflow-templates-us-east1/latest/flex/Google_Cloud_to_Neo4j \
--region us-east1 --worker-region us-east1 \
--parameters jobSpecUri=gs://factored-hackaton-2024/neo4j/gkg.json,neo4jConnectionUri=gs://factored-hackaton-2024/neo4j/credentials_enterprise.json



https://neo4j.com/docs/dataflow-bigquery/current/prerequisites/

https://github.com/neo4j-partners/neo4j-google-cloud-dataflow/blob/main/01-bigquery_to_neo4j/README.md