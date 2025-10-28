# Project Neptune Starter Code
# Assumes a dataset called neptune exists
# Assumes a table call rawmessages
# rawmessages schema - single column:  message:string
# TODO - break out based on schema
import base64
from google.cloud import bigquery
import json
table_id = "neptune.rawmessages"
processed_table_id = "neptune.processed_table"
error_table_id = "neptune.error_table"


def pubsub_to_bigquery(event, context):
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print("Row To Insert: " + pubsub_message)

    try:

        client = bigquery.Client()
        raw_table = client.get_table(table_id)
        error_table=client.get_table(processed_table_id)
        error_table=client.get_table(error_table_id)
        row_to_insert = [(pubsub_message,)]     # NOTE - the trailing comma is required for this case - it expects a tuple
        errors = client.insert_rows(raw_table, row_to_insert)

        if errors == []:
            print(f"Row successfully inserted: {row_to_insert}")
        else:
            print(f"BigQuery insert errors: {errors}")

    except Exception as e:
        print('failed to connect and get the table details')

    # process the rows and insert
    try:
        jsonmessage = json.loads(pubsub_message)
    except Exception as e:
        print(f'json decode error:{e}')
        #write to the pubsub message and error message to the bq
        row_to_insert = [(pubsub_message,e)]
        erros = client.insert_rows(error_table,row_to_insert)

        if errors == []:
            print(f"Error Row successfully inserted: {row_to_insert}")
        else:
            print(f"BigQuery insert errors: {errors}")

    row_to_insert = [{
            "id": jsonmessage.get("id"),
            "ipaddress": jsonmessage.get("ipaddress"),
            "action": jsonmessage.get("action"),
            "accountnumber": jsonmessage.get("accountnumber"),
            "actionid": jsonmessage.get("actionid"),
            "name": jsonmessage.get("name"),
            "actionby": jsonmessage.get("actionby")
        }]
    
    try:
        errors = client.insert_rows_json(table_id, row_to_insert)
        if errors == []:
            print(f"Row successfully inserted: {row_to_insert}")
        else:
            print(f"BigQuery insert errors: {errors}")
    except Exception as e:
        print(f"BigQuery exception: {e}")
