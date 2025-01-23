from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2025, 1, 1),
}

with DAG('cloud_run_job_dag', default_args=default_args, schedule_interval=None) as dag:

    dbt_run = CloudRunExecuteJobOperator(
        task_id='dbt_run',
        project_id='data-eng-dev-437916',
        region='europe-west1',
        job_name='edit-de-project-dbt',
        overrides={
            "container_overrides": [
                {
                    "args": ["run"]
                }
            ]
        }
    )

    dbt_run_specific_model = CloudRunExecuteJobOperator(
        task_id='dbt_run_specific_model',
        project_id='data-eng-dev-437916',
        region='europe-west1',
        job_name='edit-de-project-dbt',
        overrides={
            "container_overrides": [
                {
                    "args": ["run", "--select", "my_model"]
                }
            ]
        }
    )


    dbt_test = CloudRunExecuteJobOperator(
        task_id='dbt_test',
        project_id='data-eng-dev-437916',
        region='europe-west1',
        job_name='edit-de-project-dbt',
        overrides={
            "container_overrides": [
                {
                    "args": ["test"]
                }
            ]
        }
    )



    dbt_run_target = CloudRunExecuteJobOperator(
        task_id='dbt_run',
        project_id='data-eng-dev-437916',
        region='europe-west1',
        job_name='edit-de-project-dbt',
        overrides={
            "container_overrides": [
                {
                    "args": ["run","--target","prd"]
                }
            ]
        }
    )


    [dbt_run_target, dbt_run_specific_model] >> dbt_test