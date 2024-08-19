docker build -t bq-functions .
docker run -p 8080:8080 bq-functions
gcloud run deploy bq-functions --source . --project factored-hackaton-2024 \
    --region us-east1 --allow-unauthenticated \
    --cpu 4 --memory 4Gi \
    --concurrency 4 --max-instances 2

# enter docker container with bash

docker run -it bq-functions /bin/bash


us-east1-docker.pkg.dev/factored-hackaton-2024/cloud-run-source-deploy/bq-functions

