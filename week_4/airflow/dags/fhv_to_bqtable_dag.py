import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

dataset_file = 'fhv_tripdata_' + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
dataset_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}'

SERVICES = ['fhv','green','yellow']
DATATYPE = SERVICES[0]
DATASET = "tripdata"
INPUT_FILETYPE = "parquet"
COLUMN_NAMES = ['dropOff_datetime', 'dispatching_base_num']

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2019,2,1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="fhv_to_bqtable_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=4,
) as dag:
    
    # Creates one table for all (months) of tripdata in the GCP bucket folder
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATATYPE}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATATYPE}_casted_{DATASET}",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                # "sourceUris": [f"gs://{BUCKET}/{DATATYPE}/{DATATYPE}*"],
                "sourceUris": [f"gs://{BUCKET}/raw/{DATATYPE}*"],
            },
        },
    )
    
    # CREATE_BQ_TBL_QUERY = (
    #     f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATATYPE}_{DATASET}_partitioned \
    #     PARTITION BY DATE({COLUMN_NAMES[0]}) \
    #     CLUSTER BY {COLUMN_NAMES[1]} \
    #     AS \
    #     (SELECT * FROM {BIGQUERY_DATASET}.{DATATYPE}_external_table);"

    # )
    # # Create a partitioned table from external table
    # bq_create_partitioned_table_job = BigQueryInsertJobOperator(
    #     task_id=f"bq_create_{DATATYPE}_partitioned_clustered_table_task",
    #     configuration={
    #         "query": {
    #             "query": CREATE_BQ_TBL_QUERY,
    #             "useLegacySql": False,
    #         }
    #     }
    # )
    
    # create external_table on bq and partition/cluster it
    bigquery_external_table_task
    # bigquery_external_table_task >> bq_create_partitioned_table_job
