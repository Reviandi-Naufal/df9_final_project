import os
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\DATA\Data Fellowship IYKRA\CODE\Final Project\fraud-project1-d4d121a8c66e.json'


def load_gcs_to_bq(dataset_name, table_name, dataset_loc, bq_schema, uri):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    # TODO(developer): Set table_id to the ID of the table to create.
    dataset_id = f"{client.project}.{dataset_name}"
    dataset_obj = bigquery.Dataset(dataset_id)
    dataset_obj.location = dataset_loc
    dataset_created = client.create_dataset(dataset_obj, timeout=30, exists_ok=True)
    table_id = f"{dataset_id}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        schema= bq_schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
        # The source format for CSV.
        # skip_leading_rows=1,
        # source_format=bigquery.SourceFormat.CSV,
    )
    # for counter in range(1, 4):

    load_job = client.load_table_from_uri(
        uri, table_id, location= dataset_loc, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

if __name__ == '__main__':

    dataset_name = "fraud_payment_dataset"
    table_name = "ML_fraud_payment_dataset"
    dataset_loc = "asia-southeast1"
    uri = f"gs://asia-southeast1-final-proje-f3d92743-bucket/dataset/test_dataset/online_payment_*.parquet"

    schema = [
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
    ]

    load_gcs_to_bq(dataset_name, table_name, dataset_loc, schema, uri)    