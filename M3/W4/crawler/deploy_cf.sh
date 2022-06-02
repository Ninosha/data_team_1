gcloud functions deploy crawler \
    --project=cedar-heaven-349107 \
    --region=europe-west1 \
    --entry-point=metadata_to_bgq \
    --memory=512MB \
    --runtime=python38 \
    --service-account=cedar-heaven-349107@appspot.gserviceaccount.com \
    --env-vars-file=./vars.yaml \
    --trigger-http \
    --timeout=540s