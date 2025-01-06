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


import sys, os
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError


# The ID of your GCS project
project_id = "data-eng-dev-437916"
# The ID of your GCS bucket
bucket_name = "edit-data-eng-project-group1/LandingZone"
# API response source path
source_path = "./LandingZone"


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


def upload_folder_to_gcs(source_folder, bucket_name):
    try:
        for root, dirs, files in os.walk(source_folder):
            for file in files:
                # Check if the file has a .txt or .json extension
                if file.endswith(('.txt', '.json')):
                    local_file_path = os.path.join(root, file)
                    relative_path = os.path.relpath(local_file_path, source_folder)
                    gcs_blob_name = relative_path.replace("\\", "/")  # Ensure proper path format for GCS
                    
                    upload_blob(bucket_name, local_file_path, gcs_blob_name)
                    print(f"File {file} uploaded to bucket {bucket_name}.")
                else:
                    print(f"Skipping file {file} as it's not a .txt or .json file.")
    except GoogleCloudError as e:
        print(f"An error occurred while uploading the file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

# Main execution
if __name__ == "__main__":
    upload_folder_to_gcs(source_path, bucket_name)
    print("Upload complete. Listing blobs in the bucket:")
    list_blobs(bucket_name)


#print(list_blobs(bucket_name))
#upload_blob(bucket_name, source_file_name, destination_blob_name)
#print('Uploaded file to Bucket.')