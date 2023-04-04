import re
import glob
import os
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'D:\DATA\Data Fellowship IYKRA\CODE\Final Project\fraud-project1-d4d121a8c66e.json'

storage_client = storage.Client()
source_bucket_name ="dataset-fraud"
prefix = "cleaned_dataset/"

source_bucket = storage_client.bucket(source_bucket_name)
blobs_list = []

for blob in source_bucket.list_blobs(prefix=prefix):
    blobs_list.append(str(blob.name))

# list_of_files = ["file1","file100","file4","file7"]

# s = re.findall("\d+$",blobs_list)

def extract_number(f):
    s = re.findall(r"cleaned_dataset/online_payment_(\d+).parquet",f)
    print(s)
    return (int(s[0]) if s else -1,f)

max_num_file = max(blobs_list,key=extract_number)
temp = re.findall(r'cleaned_dataset/online_payment_(\d+).parquet', max_num_file)
print(int(temp[0]))