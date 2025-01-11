# pip install google-cloud-storage google-cloud-bigquery

from google.cloud import storage, bigquery
import os

# GCP Configuration
project_id = "data-eng-dev-437916"
bucket_name = "edit-data-eng-project-group1"
landing_zone_folder = "LandingZone_converted"
dataset_id = "data_eng_project_group1"

def list_parquet_files(bucket, folder_name):
    blobs = bucket.list_blobs(prefix=folder_name)
    return [blob for blob in blobs if blob.name.endswith('.parquet')]

def create_bigquery_table(client, dataset_ref, table_id, gcs_uri):
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.PARQUET
    job_config.autodetect = True
    job_config.use_avro_logical_types = True
    job_config.column_name_character_map = 'V2'

    load_job = client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete

    table = client.get_table(table_ref)
    print(f"Loaded {table.num_rows} rows into {table_id}")
    
    # Add this line to handle the 'properties' field issue
    job_config.ignore_unknown_values = True

    load_job = client.load_table_from_uri(
        gcs_uri, table_ref, job_config=job_config
    )
    
    try:
        load_job.result()  # Wait for the job to complete
        table = client.get_table(table_ref)
        print(f"Loaded {table.num_rows} rows into {table_id}")
    except Exception as e:
        print(f"Error loading {table_id}: {str(e)}")


def main():
    storage_client = storage.Client(project=project_id)
    bigquery_client = bigquery.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)

    dataset_ref = bigquery_client.dataset(dataset_id)

    parquet_files = list_parquet_files(bucket, landing_zone_folder)

    for parquet_file in parquet_files:
        file_name = os.path.basename(parquet_file.name)
        table_id = os.path.splitext(file_name)[0]
        gcs_uri = f"gs://{bucket_name}/{parquet_file.name}"

        print(f"Processing file: {file_name}")
        create_bigquery_table(bigquery_client, dataset_ref, table_id, gcs_uri)

if __name__ == "__main__":
    main()
