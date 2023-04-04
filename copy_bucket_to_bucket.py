import os
import re
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\DATA\Data Fellowship IYKRA\CODE\Final Project\fraud-project1-d4d121a8c66e.json'

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
    num_source = re.findall(r'test_data/online_payment_(\d+).parquet', max_file_source)
    max_num_source = (int(num_source[0]) if num_source else -1)

    for blob_dest in destination_bucket.list_blobs(prefix="dataset_test/"):
        dest_blobs_list.append(str(blob_dest.name))

    max_file_dest = max(dest_blobs_list,key=extract_number_dest)
    num_dest = re.findall(r'dataset_test/test_data/online_payment_(\d+).parquet', max_file_dest)
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

if __name__ == '__main__':
    source_bucket_name ='dataset-fraud'
    destination_bucket_name = 'asia-southeast1-final-proje-f3d92743-bucket'
    prefix = "test_data/" #optional, use when the data in the bucket is in another folder  
    copy_objects(source_bucket_name, destination_bucket_name, prefix)