import os
# import glob
# import logging
# from copy_bucket_to_bucket import copy_objects
# from load_data_gcs_to_bigquery import load_gcs_to_bq

from airflow import DAG
from airflow.utils.dates import days_ago
# from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
# from airflow.models import Variable

from google.cloud import storage
from google.cloud import bigquery
# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

# PROJECT_ID = Variable.get("PROJECT_ID")
# BUCKET = Variable.get("BUCKET_NAME")

# path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'session2_bq')

# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\DATA\Data Fellowship IYKRA\CODE\Final Project\fraud-project1-d4d121a8c66e.json'

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
# def upload_to_gcs(bucket, object_dir, local_glob):
#     """
#     Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
#     :param bucket: GCS bucket name
#     :param object_name: target path & file-name
#     :param local_file: source path & file-name
#     :return:
#     """

#     # logging.info(f"bucket nama: {bucket}")
#     # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
#     # (Ref: https://github.com/googleapis/python-storage/issues/74)
#     storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
#     storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
#     # End of Workaround

#     client = storage.Client()
#     bucket = client.bucket(bucket)

#     for ifile in glob.glob(local_glob):
#         filename = os.path.basename(ifile)
#         blob = bucket.blob(f"{object_dir}/{filename}")
#         blob.upload_from_filename(ifile)

def copy_objects(source_bucket_name, destination_bucket_name, prefix=''):
    """Copies a blob from one bucket to another."""
    # source_bucket_name = "your-bucket-name"
    # destination_bucket_name = "destination-bucket-name"
    # prefix = define the folders path to the object 

    storage_client = storage.Client()

    source_bucket = storage_client.bucket(source_bucket_name)
    destination_bucket = storage_client.bucket(destination_bucket_name)

    for blob in source_bucket.list_blobs(prefix=prefix):
        source_blob = source_bucket.blob(blob.name)
        destination_blob_name = f"dataset/{blob.name}"

        blob_copy = source_bucket.copy_blob(source_blob, destination_bucket, destination_blob_name)

        print(
            "Blob {} in bucket {} copied to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )

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

    # download_dataset_task = BashOperator(
    #     task_id="download_dataset_task",
    #     #bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    #     bash_command=f"for m in {{1..743}}; do wget --no-check-certificate 'https://docs.google.com/uc?export=download&id=FILEID' -P {path_to_local_home}; done"
    # )

    # format_to_parquet_task = PythonOperator(
    #     task_id="format_to_parquet_task",
    #     python_callable=format_to_parquet,
    #     op_kwargs={
    #         "src_file": f"{path_to_local_home}/{dataset_file}",
    #     },
    # )

    # local_to_gcs_task = PythonOperator(
    #     task_id="local_to_gcs_task",
    #     python_callable=upload_to_gcs,
    #     op_kwargs={
    #         "bucket": BUCKET,
    #         "object_dir": f"raw_data",
    #         "local_glob": f"{path_to_local_home}/*.parquet",
    #     },
    # )

    # download_dataset_task >> local_to_gcs_task
    ingest_to_dataLake >> load_to_bq