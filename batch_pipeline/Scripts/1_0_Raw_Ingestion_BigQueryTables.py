from google.cloud import bigquery

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

def main():
    client = bigquery.Client()
    project_id = "data-eng-dev-437916"
    dataset_id = "data_eng_project_group1"

    # Create historical_stop_times table
    create_table(client, project_id, dataset_id, "historical_stop_times", "historical_stop_times")

    # Create carris_vehicles table
    create_table(client, project_id, dataset_id, "carris_vehicles", "carris_vehicles")

if __name__ == "__main__":
    main()
