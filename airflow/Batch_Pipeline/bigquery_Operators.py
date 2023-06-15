from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator,BigQueryDeleteTableOperator 


def create_external_table(batch,
                          gcp_project_id, 
                          bigquery_dataset, 
                          external_table_name, 
                          gcp_gcs_bucket, 
                          object_path):
    """
    Create an external table using the BigQueryCreateExternalTableOperator

    Parameters :
        gcp_project_id : str
        bigquery_dataset : str
        external_table_name : str
        gcp_gcs_bucket : str
        object_path : str
    
    Returns :
        task
    """
    task = BigQueryCreateExternalTableOperator(
        task_id = f'{batch}_create_external_table',
        table_resource = {
            'tableReference': {
            'projectId': gcp_project_id,
            'datasetId': bigquery_dataset,
            'tableId': f'{external_table_name}',
            },
            'externalDataConfiguration': {
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{gcp_gcs_bucket}/{object_path}/*'],
            },
        }
    )

    return task

def delete_external_table(batch,
                          gcp_project_id, 
                          bigquery_dataset, 
                          external_table_name):

    """
    Delete table from Big Query using BigQueryDeleteTableOperator
    Parameters:
        gcp_project_id : str
        bigquery_dataset : str
        external_table_name : str

    Returns:
        task
    """
    
    task = BigQueryDeleteTableOperator(
        task_id = f'{batch}_delete_external_table',
        deletion_dataset_table = f'{gcp_project_id}.{bigquery_dataset}.{external_table_name}',
        ignore_if_missing = True
    )

    return task
