# Make sure it's all done in the same venv (using WSL)

# Install Google Cloud client package
# pip install google-cloud-storage

# Install in WSL
# sudo snap install google-cloud-cli --classic
# gcloud init

# Command to GCP login
# gcloud auth application-default login

# Test to check auth
# Command to upload kitten photo
# gcloud storage cp test_kitten.png gs://edit-data-eng-project-group1


import sys
import os
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError


# The ID of your GCS bucket
bucket_name = "edit-data-eng-project-group1"
# The path to your file to upload
source_file_name = "test_source.txt"
# The ID of your GCS object
destination_blob_name = "test_gcp.txt"


def list_blobs(bucket_name):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)

    print(f"Blobs in bucket {bucket_name}:")
    for blob in blobs:
        print(f"- {blob.name}")

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    if not os.path.exists(source_file_name):
        print(f"Error: Source file {source_file_name} does not exist.")
        return

    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        generation_match_precondition = 0

        blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

        print(f"File {source_file_name} uploaded to {destination_blob_name} in bucket {bucket_name}.")
    except GoogleCloudError as e:
        print(f"An error occurred while uploading the file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")



#print(list_blobs(bucket_name))

upload_blob(bucket_name, source_file_name, destination_blob_name)
print('Uploaded file to Bucket.')