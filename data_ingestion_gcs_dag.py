import os
# from copy_bucket_to_bucket import copy_objects
# from load_data_gcs_to_bigquery import load_gcs_to_bq

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

from google.cloud import storage
from google.cloud import bigquery

def extract_number_source(f):
    s = re.findall(r"test_data/online_payment_(\d+).parquet",f)
    print(s)
    return (int(s[0]) if s else -1,f)

def extract_number_dest(f):
    s = re.findall(r"dataset_test/test_data/online_payment_(\d+).parquet",f)
    print(s)
    return (int(s[0]) if s else -1,f)

def copy_objects(source_bucket_name, destination_bucket_name, prefix=''):
    """Copies a blob from one bucket to another."""
    # source_bucket_name = "your-bucket-name"
    # destination_bucket_name = "destination-bucket-name"
    # prefix = define the folders path to the object 

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    source_blobs_list = []
    dest_blobs_list = []

    for blob_source in source_bucket.list_blobs(prefix=prefix):
        source_blobs_list.append(str(blob_source.name))
    
    max_file_source = max(source_blobs_list,key=extract_number_source)
    num_source = re.findall(r'cleaned_dataset/online_payment_(\d+).parquet', max_file_source)
    max_num_source = (int(num_source[0]) if num_source else -1)

    for blob_dest in destination_bucket.list_blobs(prefix="dataset_test/"):
        dest_blobs_list.append(str(blob_dest.name))

    max_file_dest = max(dest_blobs_list,key=extract_number_dest)
    num_dest = re.findall(r'dataset/cleaned_dataset/online_payment_(\d+).parquet', max_file_dest)
    max_num_dest = (int(num_dest[0]) if num_dest else -1)

    print(f"Max file number in source: {max_num_source}")
    print(f"Max file number in source: {max_num_dest}")

    if max_num_source > max_num_dest:
        for blob in source_bucket.list_blobs(prefix=prefix):
            source_blob = source_bucket.blob(blob.name)
            destination_blob_name = f"dataset_test/{blob.name}"

            blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

            print(
                "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                    source_blob.name,
                    source_bucket.name,
                    blob_copy.name,
                    destination_bucket.name,
                )
            )
    else:
        print("Your datalake is up to date")

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


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:

    ingest_to_dataLake = PythonOperator(
        task_id = "ingest_dataSource_to_dataLake",
        python_callable = copy_objects,
        # op_kwargs param : source_bucket_name, destination_bucket_name, prefix=''
        op_kwargs = {
            "source_bucket_name" : "dataset-fraud",
            "destination_bucket_name" : "asia-southeast1-final-proje-f3d92743-bucket",
            "prefix" : "cleaned_dataset/"
        },
    ) 

    load_to_bq = PythonOperator(
        task_id = "load_dataLake_to_BigQuery",
        python_callable = load_gcs_to_bq,
        # op_kwargs param : dataset_name, table_name, dataset_loc, bq_schema, uri
        op_kwargs = {
            "dataset_name" : "fraud_payment_dataset",
            "table_name" : "ML_fraud_payment_dataset",
            "dataset_loc" : "asia-southeast1",
            "bq_schema" : [
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
            "uri" : "gs://asia-southeast1-final-proje-f3d92743-bucket/dataset/cleaned_dataset/online_payment_*.parquet"
        },
    )

    ingest_to_dataLake >> load_to_bq