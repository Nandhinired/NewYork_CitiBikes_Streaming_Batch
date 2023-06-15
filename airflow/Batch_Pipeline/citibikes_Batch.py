# citiBikes Batch Processing

import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from bigquery_Operators import (create_external_table,delete_external_table)
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryDeleteTableOperator 


GCP_PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'project-gcp-388913')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'citibikes_stream_ds')

EXECUTION_YEAR = '{{ logical_date.replace(month=logical_date.month-2).strftime("%Y") }}'
EXECUTION_MONTH = '{{ logical_date.replace(month=logical_date.month-2).strftime("%-m") }}'
EXECUTION_DAY = '{{ logical_date.replace(month=logical_date.month-2).strftime("%-d") }}'
EXECUTION_HOUR = '{{ logical_date.replace(month=logical_date.month-2).strftime("%-H") }}'
EXECUTION_DATETIME_STR = '{{ logical_date.replace(month=logical_date.month-2).strftime("%m%d%H") }}'
  

MACRO_VARS = {"GCP_PROJECT_ID":GCP_PROJECT_ID, 
              "BIGQUERY_DATASET": BIGQUERY_DATASET, 
              "EXECUTION_DATETIME_STR": EXECUTION_DATETIME_STR
              }


BATCH_PROCESS = ['citibikes_trips', 'climate']

create_external_table_tasks = []
delete_external_table_tasks = []

default_args = {
    "owner": "airflow",
    "dir": "/home/airflow/gcs/dags",
    "profiles_dir": "/home/airflow/gcs/data/profiles",
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id = f'citibikes_batch_processing',
    default_args = default_args,
    description = f'Monthly data pipeline to generate dims and facts for citibikes trips',
    schedule_interval='0 10 1 * *',  # Run on the 1st day of every month at 10 am
    start_date=datetime(2023, 6, 1, 10),  # Start on June 1st at 10 am
    catchup=False,
    max_active_runs=1,
    user_defined_macros=MACRO_VARS,
    tags=['citibikes']
) as dag:
  
    # DummyOperator to converge the external table tasks
    converge_task = DummyOperator(
    task_id='converge_external_tables',
    dag=dag
    )

    # DBT scripts 
    run_dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command='python -m virtualenv -p python3 tmp_venv > /dev/null && source tmp_venv/bin/activate && pip install markupsafe==2.0.1 && pip install dbt-bigquery==1.5.0 > /dev/null && pip3 install --force-reinstall MarkupSafe==2.0.1 && cd /home/airflow/gcs/dags && dbt deps && DBT_PROFILES_DIR=. dbt run && cd - && rm -rf tmp_venv',
    )

    for batch in BATCH_PROCESS:
        
        external_table_name = f'{batch}_external_table'
        gcp_gcs_bucket = f'{batch}_batch'
        object_path = f'year={EXECUTION_YEAR}/month={EXECUTION_MONTH}'

        create_external_table_task = create_external_table(batch,
                                                        GCP_PROJECT_ID, 
                                                        BIGQUERY_DATASET, 
                                                        external_table_name, 
                                                        gcp_gcs_bucket, 
                                                        object_path,
                                                        )
        create_external_table_tasks.append(create_external_table_task)
        
        delete_external_table_task = delete_external_table(batch,
                                                           GCP_PROJECT_ID, 
                                                           BIGQUERY_DATASET, 
                                                           external_table_name)
        delete_external_table_tasks.append(delete_external_table_task)
        

# Set dependencies
for create_task in create_external_table_tasks:
    create_task >> converge_task

converge_task >> run_dbt_task

for delete_task in delete_external_table_tasks:
    run_dbt_task >> delete_task

         





   
     
                    
        

