# change to the directory where code present
cd ~/NEPTUNE/build

gcloud functions deploy function_pb_bq --gen2 \
    --region=us-central1 --runtime=python312 \
    --trigger-topic=neptune-activities \
    --entry-point=pubsub_to_bigquery \
    --memory=256MB
