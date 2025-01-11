from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from google.cloud import bigquery

# Define the function to create and populate the table
def create_table(client, project_id, dataset_id, table_id, source_table):
    destination_table = f"{project_id}.{dataset_id}.staging_{table_id}"
    source_table = f"{project_id}.de_project_teachers.{source_table}"
    # Check if the table exists and delete it if it does
    try:
        client.delete_table(destination_table)
        print(f"Table {destination_table} deleted.")
    except Exception:
        print(f"Table {destination_table} does not exist.")
    # Create the table without data
    schema = client.get_table(source_table).schema
    table = bigquery.Table(destination_table, schema=schema)
    table = client.create_table(table)
    print(f"Table {destination_table} created.")
    # Insert data into the newly created table
    job_config = bigquery.QueryJobConfig()
    sql = f"INSERT INTO {destination_table} SELECT * FROM {source_table}"
    query_job = client.query(sql, job_config=job_config)
    query_job.result()
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

# Define the Airflow DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}
dag = DAG(
    'group1_ingestion_bigquery',
    default_args=default_args,
    description='Create and populate BigQuery tables',
    schedule_interval=None,  # Run manually or set a cron expression
    start_date=datetime(2025, 1, 11),
    catchup=False,
)
# Define tasks
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
# Set task dependencies
create_historical_stop_times_task >> create_carris_vehicles_task