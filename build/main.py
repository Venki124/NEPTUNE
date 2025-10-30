# Project Neptune Starter Code (Fixed & Production Safe)
# - Dataset: neptune
# - Tables: rawmessages, processed_table, error_table

import base64
import datetime
import json
from google.cloud import bigquery
from util import detect_format, safe_int, validate_message

# BigQuery table IDs (Update with your project if needed)
table_id = "neptune.rawmessages"
processed_table_id = "neptune.processed_table"
error_table_id = "neptune.error_table"

# Expected schema for processed_table
EXPECTED_FIELDS = {
    "id": str,
    "ipaddress": str,
    "action": str,
    "accountnumber": str,
    "actionid": int,
    "name": str,
    "actionby": str
}

# ---- Main Cloud Function ----

def pubsub_to_bigquery(event, context):
    """Triggered from Pub/Sub; processes one message at a time."""
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(f"Raw Pub/Sub message: {pubsub_message}")
    # ---- step 0: Pre-check the data csv or json
    

    # ---- Step 1: Connect to BigQuery ----
    try:
        client = bigquery.Client()
        raw_table = client.get_table(table_id)
        processed_table = client.get_table(processed_table_id)
        error_table = client.get_table(error_table_id)
    except Exception as e:
        print(f"Failed to connect or get table details: {e}")
        return  # stop further execution

    # ---- Step 2: Insert raw message ----
    try:
        raw_insert = [{'message':pubsub_message,'ingestion_ts': datetime.datetime.now(datetime.timezone.utc)}]
        errors = client.insert_rows(raw_table, raw_insert)
        if not errors:
            print("Raw message inserted into neptune.rawmessages")
        else:
            print(f"BigQuery raw insert errors: {errors}")
    except Exception as e:
        print(f"Failed to insert into rawmessages: {e}")
        return

    # ---- Step 3: Parse JSON/CSV ----
    # try:
    #     jsonmessage = json.loads(pubsub_message)
    #     print(f"Parsed JSON message: {jsonmessage}")
    # except Exception as e:
    #     print(f"JSON decode error: {e}")
    #     row_to_insert = [{"message": pubsub_message, "error": str(e)}]
    #     client.insert_rows_json(error_table, row_to_insert)
    #     return  # stop processing invalid JSON

    msg_type = detect_format(pubsub_message)
    if msg_type =='csv':
        try:
            parts = [p.strip() for p in pubsub_message.split(",")]
            expected_count = len(EXPECTED_FIELDS)
            # Check field count
            if len(parts) == expected_count:
                jsonmessage = dict(zip(EXPECTED_FIELDS.keys(), parts))
                print(f"Converted CSV → JSON dict: {jsonmessage}")
            else:
                row_to_insert = [{"message": pubsub_message, "error":"Length Mismatch",
                                  'ingestion_ts': datetime.datetime.now(datetime.timezone.utc)}]
                client.insert_rows_json(error_table, row_to_insert)
                return
        except Exception as e:
            print(f"CSV decode error: {e}")
            row_to_insert = [{"message": pubsub_message, "error": str(e)
                              ,'ingestion_ts': datetime.datetime.now(datetime.timezone.utc)}]
            client.insert_rows_json(error_table, row_to_insert)
            return  # stop processing invalid CSV
        
    elif msg_type=='json':
        jsonmessage = json.loads(pubsub_message)
        print(f"Parsed JSON message: {jsonmessage}")


    # ---- Step 4: Validate schema ----
    missing, extra, type_mismatch = validate_message(jsonmessage,EXPECTED_FIELDS)
    if missing or type_mismatch:
        print(f"Schema validation failed.\nMissing: {missing}\nExtra: {extra}\nType mismatch: {type_mismatch}")
        row_to_insert = [{"message": pubsub_message, "error": f"Schema error: {missing or type_mismatch}"
                          ,'ingestion_ts': datetime.datetime.now(datetime.timezone.utc)}]
        client.insert_rows_json(error_table, row_to_insert)
        return  # stop invalid schema

    # ---- Step 5: Prepare valid row for processed_table ----
    row_to_insert = [{
        "id": jsonmessage.get("id"),
        "ipaddress": jsonmessage.get("ipaddress"),
        "action": jsonmessage.get("action"),
        "accountnumber": jsonmessage.get("accountnumber"),
        "actionid": safe_int(jsonmessage.get("actionid")),
        "name": jsonmessage.get("name"),
        "actionby": jsonmessage.get("actionby"),
        'ingestion_ts': datetime.datetime.now(datetime.timezone.utc)
    }]
    print(f"Processed row to insert: {row_to_insert}")

    # ---- Step 6: Insert validated row into processed_table ----
    try:
        errors = client.insert_rows_json(processed_table, row_to_insert)
        if not errors:
            print(f"Row successfully inserted into neptune.processed_table")
        else:
            print(f"BigQuery insert errors: {errors}")
    except Exception as e:
        print(f"BigQuery exception during processed_table insert: {e}")
        row_to_insert = [{"message": pubsub_message, "error": str(e),
                          'ingestion_ts': datetime.datetime.now(datetime.timezone.utc)}]
        client.insert_rows_json(error_table, row_to_insert)
