import io
import os
import requests
import pandas as pd
from google.cloud import storage
import urllib.request
import pyarrow as pa

"""
Pre-reqs: 
1. Go to virtual environment with conda activate <env_name>
2. `pip install pandas pyarrow google-cloud-storage`
3. export GOOGLE_APPLICATION_CREDENTIALS=~/.google/credentials/google_credentials.json
4. Set GCP_GCS_BUCKET as your bucket or change default value of BUCKET
"""

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'

# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_ny-rides-kelvin")


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    """
    # # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # # (Ref: https://github.com/googleapis/python-storage/issues/74)
    # storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    # storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv.gz  file_name 
        file_name = service + '_tripdata_' + year + '-' + month + '.csv.gz'
        
        # # download csv.gz file
        # request_url = init_url + file_name
        # urllib.request.urlretrieve(request_url, '..//data/' + file_name)
        
        # read it back into a parquet file
        if file_name == 'fhv_tripdata_2020-02.csv.gz':
            df = pd.read_csv('..//data/' + file_name, encoding='latin1')
        else:
            df = pd.read_csv('..//data/' + file_name)
            
        # Define schema for parquet file
        if year == '2019':
            schema = pa.schema([
                pa.field('dispatching_base_num', pa.string()),
                pa.field('pickup_datetime', pa.string()),
                pa.field('dropOff_datetime', pa.string()),
                pa.field('PUlocationID', pa.int64()),
                pa.field('DOlocationID', pa.int64()),
                pa.field('SR_Flag', pa.int64()),
                pa.field('Affiliated_base_number', pa.string())
        ])
        
        elif year == '2020':
            schema = pa.schema([
                pa.field('dispatching_base_num', pa.string()),
                pa.field('pickup_datetime', pa.string()),
                pa.field('dropoff_datetime', pa.string()),
                pa.field('PULocationID', pa.int64()),
                pa.field('DOLocationID', pa.int64()),
                pa.field('SR_Flag', pa.int64()),
                pa.field('Affiliated_base_number', pa.string())
        ])

        file_name = file_name.replace('.csv.gz', '.parquet')
        df.to_parquet('..//data/' + file_name, engine='pyarrow', schema=schema)
        print(f"Parquet: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}_parquet/{file_name}", '..//data/' + file_name)
        print(f"GCS: {service}_parquet/{file_name}")


# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
# web_to_gcs('2019', 'fhv')
web_to_gcs('2020', 'fhv')
# web_to_gcs('2021', 'fhv')