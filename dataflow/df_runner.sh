gcloud dataflow jobs run gdelt_gkg_extract \
    --project factored-hackaton-2024 \
    --gcs-location "gs://factored-hackaton-2024/dataflow/templates/gdelt_gkg_extract_main" \
    --region us-east1 \
    --service-account-email github-cicd@factored-hackaton-2024.iam.gserviceaccount.com \
    --worker-machine-type n1-standard-4 \
    --parameters start_date="2024-08-01" \
    --parameters end_date="2024-08-23" \
    --max-workers 4