



bq mk --connection --location=us-east1 --project_id=factored-hackaton-2024 \
    --connection_type=CLOUD_RESOURCE remote-functions

bq show --connection factored-hackaton-2024.us-east1.remote-functions

# bqcx-485805833933-zgtc@gcp-sa-bigquery-condel.iam.gserviceaccount.com