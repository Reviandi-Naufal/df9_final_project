import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud_proudwoods.json'

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

if __name__ == '__main__': 
    copy_objects('online_payment_data_bucket', 'asia-southeast1-final-proje-293de5e1-bucket')