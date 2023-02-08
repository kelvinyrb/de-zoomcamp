## Running Spark in the Cloud

### Connecting to Google Cloud Storage 

Uploading data to GCS:

Check GOOGLE_APPLICATION_CREDENTIALS environment variable is pointing to key by running export command in shell or adding to .bashrc

``` bash
export GOOGLE_APPLICATION_CREDENTIALS=~/.path_to_key/key.json
```

GCloud auth and follow the prompts

``` bash
gcloud auth login
```

Write data into GCS

```bash
gsutil -m cp -r pq/ gs://dtc_data_lake_de-zoomcamp-nytaxi/pq
```
*Note: -m flag is for multi-thread process to optimally utilise CPUs*

Download the jar for connecting to GCS to any location (e.g. the `lib` folder):

```bash
mkdir lib
cd lib
gsutil cp gs://hadoop-lib/gcs/gcs-connector-hadoop3-2.2.5.jar . 
```

See the notebook with configuration in [09_spark_gcs.ipynb](09_spark_gcs.ipynb)

(Thanks Alvin Do for the instructions!)


### Local Cluster and Spark-Submit

Creating a stand-alone cluster ([docs](https://spark.apache.org/docs/latest/spark-standalone.html)):

```bash
cd spark/spark-3.0.3-bin-hadoop3.2/  
./sbin/start-master.sh
```

Go to `localhost:8080`

Run `spark.builder()` on Jupyter Notebook, this will be reflected in `Running Applications` on Spark Master UI

Creating a worker:

```bash
URL="spark://de-zoomcamp.australia-southeast1-b.c.ny-rides-kelvin.internal:7077"
./sbin/start-slave.sh ${URL}

# for newer versions of spark use that:
#./sbin/start-worker.sh ${URL}
```

Turn the notebook into a script:

```bash
jupyter nbconvert --to=script 06_spark_sql.ipynb
```

Edit the script and then run it:

*Ensure that application that was running on Jupyter Notebook is killed on Spark Master UI before running python script*
```bash 
python 06_spark_sql.py \
    --input_green=data/pq/green/2020/*/ \
    --input_yellow=data/pq/yellow/2020/*/ \
    --output=data/report-2020
```

Use `spark-submit` for running the script on the cluster (submitting spark jobs to spark clusters)

```bash
URL="spark://de-zoomcamp.australia-southeast1-b.c.ny-rides-kelvin.internal:7077"

spark-submit \
    --master="${URL}" \
    06_spark_sql.py \
        --input_green=data/pq/green/2021/*/ \
        --input_yellow=data/pq/yellow/2021/*/ \
        --output=data/report-2021
```

After you finish:
```bash
cd spark/spark-3.0.3-bin-hadoop3.2/  
./sbin/stop-master.sh
./sbin/stop-slave.sh
```
### Data Proc
1. Go to Google Cloud > DataProc > Create Cluster
2. Check: Single node, Jupyter Notebook, Docker
3. Click Create
4. Upload the script to GCS:
```bash
gsutil cp 06_spark_sql.py gs://dtc_data_lake_ny-rides-kelvin/code/06_spark_sql.py
```
5. In the cluster, Submit Job
    - Jobtype: PySpark
    - Main python file: gs://dtc_data_lake_ny-rides-kelvin/code/06_spark_sql.py
    - Arguments:
        * `--input_green=gs://dtc_data_lake_ny-rides-kelvin/pq/green/2021/*/`
        * `--input_yellow=gs://dtc_data_lake_ny-rides-kelvin/pq/yellow/2021/*/`
        * `--output=gs://dtc_data_lake_ny-rides-kelvin/report-2021`
6. Submit

 *Note: Dataproc is already configured to accept GCS*
There are three ways to submit job to DataProc cluster
1. Google Cloud UI
2. Google Cloud SDK
3. REST API

Using Google Cloud SDK for submitting to dataproc
([link](https://cloud.google.com/dataproc/docs/guides/submit-job#dataproc-submit-job-gcloud))

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=australia-southeast1 \
    gs://dtc_data_lake_ny-rides-kelvin/code/06_spark_sql.py \
    -- \
        --input_green=gs://dtc_data_lake_ny-rides-kelvin/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_ny-rides-kelvin/pq/yellow/2020/*/ \
        --output=gs://dtc_data_lake_ny-rides-kelvin/report-2020
```

Ensure that service account has permissions to submit jobs using Google SDK

1. Google Cloud Console
2. IAM & Admin
3. Add Role > Dataproc Administrator

### Big Query

Upload the script to GCS:

```bash
gsutil cp 06_spark_sql_big_query.py gs://dtc_data_lake_ny-rides-kelvin/code/06_spark_sql_big_query.py
```

Write results to big query ([docs](https://cloud.google.com/dataproc/docs/tutorials/bigquery-connector-spark-example#pyspark)):

```bash
gcloud dataproc jobs submit pyspark \
    --cluster=de-zoomcamp-cluster \
    --region=australia-southeast1  \
    --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
    gs://dtc_data_lake_ny-rides-kelvin/code/06_spark_sql_big_query.py \
    -- \
        --input_green=gs://dtc_data_lake_ny-rides-kelvin/pq/green/2020/*/ \
        --input_yellow=gs://dtc_data_lake_ny-rides-kelvin/pq/yellow/2020/*/ \
        --output=trips_data_all.reports-2020
```

