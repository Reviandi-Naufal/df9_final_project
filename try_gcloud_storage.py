import os
import glob
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud_proudwoods.json'

storage_client = storage.Client()

# print(dir(storage_client))

"""
Create a New Bucket
"""

bucket_name = 'online_payment_data_bucket'
# bucket = storage_client.bucket(bucket_name)
# new_bucket = storage_client.create_bucket(bucket, location="asia-southeast1")

# print(
#     "Created bucket {} in {} with storage class {}".format(
#         new_bucket.name, new_bucket.location, new_bucket.storage_class
#     )
# )

"""
Print Bucket Detail
"""
# print(vars(bucket))


"""
Accessing a Spesific Bucket
"""
# my_bucket = storage_client.get_bucket(bucket)

"""
Upload Files
"""
def upload_to_bucket(file_path, bucket_name):

    # logging.info(f"bucket nama: {bucket}")
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround
    
    bucket = storage_client.get_bucket(bucket_name)

    for ifile in glob.glob(file_path):
        try:
            filename = os.path.basename(ifile)
            blob = bucket.blob(filename)
            blob.upload_from_filename(ifile)
        except Exception as e:
            print(e)
            return False

data_file_path = r'D:\DATA\Data Fellowship IYKRA\CODE\Final Project\Dataset\cleaned_dataset'
upload_to_bucket(data_file_path, bucket_name)