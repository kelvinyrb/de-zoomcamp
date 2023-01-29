import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

dataset_file = 'fhv_tripdata_' + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
dataset_url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}'

DATATYPE = "fhv"
DATASET = "tripdata"
INPUT_FILETYPE = "parquet"
COLUMN_NAMES = ['dropOff_datetime', 'dispatching_base_num']

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2019,12,1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_fhv_dag",
    schedule_interval="@monthly",
    default_args=default_args,
    catchup=True,
    max_active_runs=4,
    # tags=['dtc-de'],
) as dag:
    
    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id=f"bq_{DATATYPE}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{DATATYPE}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                "sourceUris": [f"gs://{BUCKET}/raw/{DATATYPE}*"],
            },
        },
    )
    
    CREATE_BQ_TBL_QUERY = (
        f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{DATATYPE}_{DATASET}_partitioned \
        PARTITION BY DATE({COLUMN_NAMES[0]}) \
        CLUSTER BY {COLUMN_NAMES[1]} \
        AS \
        (SELECT * FROM {BIGQUERY_DATASET}.{DATATYPE}_external_table);"

    )
    # Create a partitioned table from external table
    bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        task_id=f"bq_create_{DATATYPE}_partitioned_clustered_table_task",
        configuration={
            "query": {
                "query": CREATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )
    
    # create external_table on bq and partition/cluster it
    bigquery_external_table_task >> bq_create_partitioned_table_job
