from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import zipfile
import io
import json
import warnings
from google.cloud import storage
from google.cloud.exceptions import GoogleCloudError

# GCP Configuration
project_id = "data-eng-dev-437916"
bucket_name = "edit-data-eng-project-group1"
def fetch_api_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")
    
def fetch_and_extract_gtfs_data(gcs_path):
    url = "https://api.carrismetropolitana.pt/gtfs"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        # Extract and upload each file
        for file_info in z.infolist():
            with z.open(file_info) as file:
                content = file.read()
                blob_name = f"{gcs_path}/{file_info.filename}"
                upload_to_gcs(content, blob_name, content_type='application/octet-stream')
            print(f"Extracted and uploaded GTFS file: {file_info.filename}")
        print(f"All GTFS files extracted and uploaded to {gcs_path}.")
    else:
        raise Exception(f"API request failed with status code {response.status_code}")
    
def upload_to_gcs(data, blob_name, content_type='application/json'):
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if content_type == 'application/json':
            blob.upload_from_string(json.dumps(data, indent=2), content_type=content_type)
        else:
            blob.upload_from_string(data, content_type=content_type)
        print(f"File {blob_name} uploaded to {bucket_name}.")
    except GoogleCloudError as e:
        print(f"An error occurred while uploading the file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def fetch_and_upload_api_data():
    api_endpoints = {
        "alerts": "https://api.carrismetropolitana.pt/alerts",
        "municipalities": "https://api.carrismetropolitana.pt/municipalities",
        "stops": "https://api.carrismetropolitana.pt/stops",
        "lines": "https://api.carrismetropolitana.pt/lines",
        "routes": "https://api.carrismetropolitana.pt/routes",
        "encm": "https://api.carrismetropolitana.pt/datasets/facilities/encm",
        "schools": "https://api.carrismetropolitana.pt/datasets/facilities/schools"
    }
    for endpoint, url in api_endpoints.items():
        data = fetch_api_data(url)
        warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
        upload_to_gcs(data, f"LandingZone/carris_{endpoint}_data.json")
        print(f"Uploaded API file: carris_{endpoint}_data.json")

def fetch_and_upload_gtfs_data():
    gtfs_folder_path = "LandingZone/GTFS"
    fetch_and_extract_gtfs_data(gtfs_folder_path)

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'group1_fetch_api_and_gtfs_data',
    default_args=default_args,
    description='Fetch data from Carris Metropolitana API and upload to GCS',
    schedule_interval=None,  # Run manually or set a cron expression
    start_date=datetime(2025, 1, 11),
    catchup=False,
)
fetch_api_task = PythonOperator(
    task_id='fetch_and_upload_api_data',
    python_callable=fetch_and_upload_api_data,
    dag=dag,
)
fetch_gtfs_task = PythonOperator(
    task_id='fetch_and_upload_gtfs_data',
    python_callable=fetch_and_upload_gtfs_data,
    dag=dag,
)
fetch_api_task >> fetch_gtfs_task