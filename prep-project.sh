# project code url
# git clone https://github.com/Venki124/NEPTUNE.git

# set the project
gcloud config set project $(gcloud projects list --format='value(PROJECT_ID)' | grep playground)

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
gcloud services enable dataflow.googleapis.com
gcloud services enable run.googleapis.com

# Create the storage bucket
gcloud storage buckets create gs://$GOOGLE_CLOUD_PROJECT"-bucket" --soft-delete-duration=0


# create the pubsub topics
gcloud pubsub topics create neptune-activities

# create the pubsub subscription
gcloud pubsub subscriptions create neptune-activities-test --topic=neptune-activities

#publish a message to topic
gcloud pubsub topics publish neptune-activities --message='Hello World!'

# Dataflow bridge to get messages from Moonbank to Pluralsight

cat > pipeline.yaml <<EOF

pipeline:

  transforms:

    - name: Source

      type: ReadFromPubSub

      config:

        format: raw

        topic: projects/moonbank-neptune/topics/activities

    - name: Sink

      type: WriteToPubSub

      input: Source

      config:

        format: raw

        topic: projects/${DEVSHELL_PROJECT_ID}/topics/neptune-activities

  windowing:

    type: fixed

    size: 1

options:

  streaming: true

EOF



gcloud dataflow yaml run neptune --region us-central1 --yaml-pipeline-file=pipeline.yaml

if(( $? > 0 ))
then 
  echo "Script failed to trigger the pipeline"
else
  echo "creating the Cloud run function"
  echo "changing directory to build folder"
  cd ~/NEPTUNE/build
  gcloud functions deploy function_pb_bq --gen2 --region=us-central1 --runtime=python312 --trigger-topic=neptune-activities --entry-point=pubsub_to_bigquery --memory=256MB --service-account=$(gcloud iam service-accounts list --filter="EMAIL  ~ compute" --format='value(EMAIL)')
  # --run-service-account=$(gcloud iam service-accounts list --filter="EMAIL  ~ compute" --format='value(EMAIL)')
fi


<<COMMENT_BLOCK

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
