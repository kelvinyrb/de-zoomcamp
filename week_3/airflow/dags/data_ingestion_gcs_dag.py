import os
import logging
from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

dataset_file = 'fhv_tripdata_' + '{{ execution_date.strftime(\'%Y-%m\') }}.parquet'
dataset_file_casted = 'fhv_tripdata_' + '{{ execution_date.strftime(\'%Y-%m\') }}_casted.parquet'
dataset_url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{dataset_file}"

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

table_schema_fhv = pa.schema(
        [
        ('dispatching_base_num', pa.string()), 
        ('pickup_datetime', pa.timestamp('s')), 
        ('dropOff_datetime', pa.timestamp('s')), 
        ('PUlocationID', pa.int64()), 
        ('DOlocationID', pa.int64()), 
        ('SR_Flag', pa.int64()), 
        ('Affiliated_base_number', pa.string()), 
        ]
)


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


def cast_parquet_columns(local_file, dataset_file):
    logging.info('local_file',local_file)
    table = pq.read_table(local_file)
    table = table.cast(table_schema_fhv)
    logging.info('table', table)
    pq.write_table(table, dataset_file.replace('.parquet', '_casted.parquet'))

default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2019,1,1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    default_args=default_args,
    schedule_interval="@monthly",
    catchup= True,
    max_active_runs=3,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {dataset_url} > {path_to_local_home}/{dataset_file}"
    )
    cast_parquet_columns_task = PythonOperator(
        task_id="cast_parquet_columns_task",
        python_callable=cast_parquet_columns,
        op_kwargs={
            "local_file": f"{path_to_local_home}/{dataset_file}", 
            "dataset_file": dataset_file
        },
    )

    local_to_gcs_casted_task = PythonOperator(
        task_id="local_to_gcs_casted_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{dataset_file_casted}",
            "local_file": f"{path_to_local_home}/{dataset_file_casted}",
        },
    )
    
    download_dataset_task >> cast_parquet_columns_task >> local_to_gcs_casted_task
    
    ## Docker Memory needed to be increased from 2GB to 5GB to process fhv_tripdata_2019-01.parquet
