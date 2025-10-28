# create the neptune dataset and table
bq mk neptune

bq mk --table --schema message:string $GOOGLE_CLOUD_PROJECT:neptune.rawmessages
bq mk --table --schema id:string,ipaddress:string,action:string,accountnumber:string,actionid:integer,name:string,actionby:string $GOOGLE_CLOUD_PROJECT:neptune.processed_table
bq mk --table --schema message:string,error:string $GOOGLE_CLOUD_PROJECT:neptune.error_table

# enable event ark and pubsub api
gcloud services enable eventarc.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com


<<COMMENT_BLOCK
# create the pubsub topics
gcloud pubsub topics create neptune-activities

# create the pubsub subscription
gcloud pubsub subscriptions create neptune-activities-test --topic=neptune-activities

#publish a message to topic
gcloud pubsub topics publish neptune-activites --message='Hello World!'

#example json message
gcloud pubsub topics publish neptune-activities \
  --message='{"id":"20200812040801981475","ipaddress":"195.174.170.81","action":"UPDATE","accountnumber":"GB25BZMX47593824219489","actionid":4,"name":"Emily Blair","actionby":"STAFF"}'

# create the cloud functions
gcloud functions deploy function_pb_bq --gen2 --region=us-central1 --runtime=python312 --trigger-topic=neptune-activities --entry-point=pubsub_to_bigquery --memory=256MB

# list the cloud functions
gcloud functions list

# delete the cloud functions
gcloud functions delete function_pb_bq --quiet

COMMENT_BLOCK
