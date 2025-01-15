from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import zipfile
import io
import json
import warnings
from google.cloud import storage, bigquery
from google.cloud.exceptions import GoogleCloudError
from google.api_core.exceptions import NotFound
import pandas as pd
import os

# GCP Configuration
project_id = "data-eng-dev-437916"
bucket_name = "edit-data-eng-project-group1"
landing_zone_folder = "LandingZone"
dataset_id = "data_eng_project_group1"


# Fetch API data
def fetch_api_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

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

# Fetch and upload GTFS data
def fetch_and_extract_gtfs_data(gcs_path):
    url = "https://api.carrismetropolitana.pt/gtfs"
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        z = zipfile.ZipFile(io.BytesIO(response.content))
        for file_info in z.infolist():
            with z.open(file_info) as file:
                content = file.read()
                blob_name = f"{gcs_path}/{file_info.filename}"
                upload_to_gcs(content, blob_name, content_type='application/octet-stream')
            print(f"Extracted and uploaded GTFS file: {file_info.filename}")
        print(f"All GTFS files extracted and uploaded to {gcs_path}.")
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

def fetch_and_upload_gtfs_data():
    gtfs_folder_path = "LandingZone/GTFS"
    fetch_and_extract_gtfs_data(gtfs_folder_path)


# Upload to GCS
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

# Fetch IPMA data
def fetch_ipma_data():
    url = "https://api.ipma.pt/open-data/observation/climate/evapotranspiration/lisboa/"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"API request failed with status code {response.status_code}")

# Fetch and upload IPMA data to GCS
def fetch_and_upload_ipma_data():
    ipma_data = fetch_ipma_data()
    warnings.filterwarnings("ignore", message="Your application has authenticated using end user credentials")
    upload_to_gcs(ipma_data, "LandingZone/ipma_lisbon_evapotranspiration_data.json")
    print("Uploaded IPMA data to GCS.")


# GCS to BigQuery
def list_files_in_folder(bucket, folder_name):
    blobs = bucket.list_blobs(prefix=folder_name)
    return [blob for blob in blobs if blob.name.endswith((".json", ".txt"))]

def clean_column_name(name):
    return name.replace(",", "_").replace(" ", "_").replace(".", "_")[:300]

def replace_null_with_string(obj):
    if isinstance(obj, dict):
        return {k: replace_null_with_string(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [replace_null_with_string(v) for v in obj]
    elif obj is None:
        return "null"
    else:
        return obj
    
def transform_json_to_ndjson(data):
    if isinstance(data, dict) and 'entity' in data:
        return [json.dumps(entity) for entity in data['entity']]
    elif isinstance(data, list):
        return [json.dumps(record) for record in data]
    else:
        return [json.dumps(data)]
    
def read_file_content(blob):
    file_content = blob.download_as_text()
    file_extension = os.path.splitext(blob.name)[1]
    if file_extension == ".json":
        data = json.loads(file_content)
        data = replace_null_with_string(data)
        return transform_json_to_ndjson(data)
    elif file_extension == ".txt":
        df = pd.read_csv(io.StringIO(file_content), sep=",", dtype=str, na_values=[""], keep_default_na=False)
        df = df.fillna("null")
        df.columns = [clean_column_name(col) for col in df.columns]
        return df
    else:
        return None
    
def create_bigquery_table(client, dataset_ref, table_id, data):
    table_ref = dataset_ref.table(f"staging_{table_id}")
    try:
        client.get_table(table_ref)
        client.delete_table(table_ref)
        print(f"Table staging_{table_id} dropped.")
    except NotFound:
        print(f"Table staging_{table_id} does not exist. Creating new table.")
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.ignore_unknown_values = True
    if isinstance(data, pd.DataFrame):
        job_config.source_format = bigquery.SourceFormat.CSV
        load_job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)
    else:
        job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
        json_data = '\n'.join(data)
        load_job = client.load_table_from_file(io.StringIO(json_data), table_ref, job_config=job_config)
    try:
        load_job.result()
        table = client.get_table(table_ref)
        print(f"Loaded {table.num_rows} rows into staging_{table_id}")
    except Exception as e:
        print(f"Error loading {table_id}: {str(e)}")
        print(f"Error details: {load_job.errors}")

def etl_task():
    storage_client = storage.Client(project=project_id)
    bigquery_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    dataset_ref = bigquery_client.dataset(dataset_id)
    files = list_files_in_folder(bucket, landing_zone_folder)
    for file in files:
        print(f"Processing file: {file.name}")
        data = read_file_content(file)
        if data is not None:
            table_id = os.path.splitext(os.path.basename(file.name))[0]
            create_bigquery_table(bigquery_client, dataset_ref, table_id, data)

# Create and populate tables
def create_table(client, project_id, dataset_id, table_id, source_table):
    destination_table = f"{project_id}.{dataset_id}.staging_{table_id}"
    source_table = f"{project_id}.de_project_teachers.{source_table}"
    try:
        client.delete_table(destination_table)
        print(f"Table {destination_table} deleted.")
    except NotFound:
        print(f"Table {destination_table} does not exist.")
    table = bigquery.Table(destination_table, client.get_table(source_table).schema)
    client.create_table(table)
    query = f"INSERT INTO {destination_table} SELECT * FROM {source_table}"
    client.query(query).result()
    print(f"Data inserted into {destination_table} successfully.")
    
# Wrapper function for Airflow tasks
def create_historical_stop_times_table():
    client = bigquery.Client()
    project_id = "data-eng-dev-437916"
    dataset_id = "data_eng_project_group1"
    create_table(client, project_id, dataset_id, "historical_stop_times", "historical_stop_times")

def create_carris_vehicles_table():
    client = bigquery.Client()
    project_id = "data-eng-dev-437916"
    dataset_id = "data_eng_project_group1"
    create_table(client, project_id, dataset_id, "carris_vehicles", "carris_vehicles")
    

# --- DAG DEFINITION ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'group1_combined_dag',
    default_args=default_args,
    description='Combined DAG for fetching data, uploading to GCS, and ingesting into BigQuery',
    schedule_interval=None,
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

gcs_to_bigquery_task = PythonOperator(
    task_id='gcs_to_bigquery_etl',
    python_callable=etl_task,
    dag=dag,
)

create_historical_stop_times_task = PythonOperator(
    task_id='create_historical_stop_times_table',
    python_callable=create_historical_stop_times_table,
    dag=dag,
)
create_carris_vehicles_task = PythonOperator(
    task_id='create_carris_vehicles_table',
    python_callable=create_carris_vehicles_table,
    dag=dag,
)
fetch_ipma_task = PythonOperator(
    task_id='fetch_and_upload_ipma_data',
    python_callable=fetch_and_upload_ipma_data,
    dag=dag,
)
# Define dependencies
fetch_api_task >> fetch_ipma_task >> fetch_gtfs_task >> gcs_to_bigquery_task >> create_historical_stop_times_task >> create_carris_vehicles_task
