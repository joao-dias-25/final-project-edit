from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
import pandas as pd
import io
import os
import json

# GCP Configuration
project_id = "data-eng-dev-437916"
bucket_name = "edit-data-eng-project-group1"
landing_zone_folder = "LandingZone"
dataset_id = "data_eng_project_group1"

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

dag = DAG(
        "group1_gcs_to_bigquery_etl",
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
)
etl = PythonOperator(
    task_id="gcs_to_bigquery_etl",
    python_callable=etl_task
)

etl
