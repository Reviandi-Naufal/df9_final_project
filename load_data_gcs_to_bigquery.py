import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'D:\DATA\Data Fellowship IYKRA\CODE\Final Project\ServiceKey_GoogleCloud_proudwoods.json'

# Construct a BigQuery client object.
client = bigquery.Client()

# TODO(developer): Set table_id to the ID of the table to create.
table_id = "{}.your_dataset.ML_online_payment_dataset".format(client.project)

job_config = bigquery.LoadJobConfig(
    schema=[
        bigquery.SchemaField("index", "INT64"),
        bigquery.SchemaField("step", "INT64"),
        bigquery.SchemaField("type", "STRING"),
        bigquery.SchemaField("amount", "FLOAT64"),
        bigquery.SchemaField("nameOrig", "STRING"),
        bigquery.SchemaField("oldbalanceOrg", "FLOAT64"),
        bigquery.SchemaField("newbalanceOrig", "FLOAT64"),
        bigquery.SchemaField("nameDest", "STRING"),
        bigquery.SchemaField("oldbalanceDest", "FLOAT64"),
        bigquery.SchemaField("newbalanceDest", "FLOAT64"),
        bigquery.SchemaField("isFraud", "INT64"),
        bigquery.SchemaField("isFlaggedFraud", "INT64"),
    ],
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    skip_leading_rows=1,
    # The source format defaults to CSV, so the line below is optional.
    source_format=bigquery.SourceFormat.CSV,
)
for counter in range(3, 5):
    uri = f"gs://online_payment_data_bucket/online_payment_{counter}.csv"

    load_job = client.load_table_from_uri(
        uri, table_id, location="asia-southeast1", job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))