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

DATATYPE = 'fhv'
DATASET = "tripdata"
INPUT_FILETYPE = "parquet"

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023,2,19),
    "retries": 0,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="fhv_to_bqtable_dag",
    default_args=default_args,
) as dag:

    # Creates one table for 2019-2020 fhv tripdata in the GCP bucket folder
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATATYPE}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATATYPE}_{DATASET}_2019_2020",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": [f"gs://{BUCKET}/{DATATYPE}_parquet/*"],
                "schema": {
                    "fields": [
                        {"name": "dispatching_base_num", "type": "STRING"},
                        {"name": "pickup_datetime", "type": "TIMESTAMP"},
                        {"name": "dropoff_datetime", "type": "TIMESTAMP"},
                        {"name": "PUlocationID", "type": "INTEGER"},
                        {"name": "DOlocationID", "type": "INTEGER"},
                        {"name": "SRFlag", "type": "INTEGER"},
                        {"name": "Affiliated_base_number", "type": "STRING"}
                            ]
                        },
            },
                        },
            )      
    bigquery_external_table_task
