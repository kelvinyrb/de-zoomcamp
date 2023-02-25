## Week 5 Homework

In this homework we'll put what we learned about Spark
in practice.

We'll use high volume for-hire vehicles (HVFHV) dataset for that.

## Question 1. Install Spark and PySpark

* Install Spark
* Run PySpark
* Create a local spark session 
* Execute `spark.version`

``` python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

spark.version
```
What's the output?

Answer: "3.0.3"

## Question 2. HVFHW February 2021

Download the HVFHV data for february 2021:

```bash
wget https://nyc-tlc.s3.amazonaws.com/trip+data/fhvhv_tripdata_2021-02.csv
```

Read it with Spark using the same schema as we did 
in the lessons. We will use this dataset for all
the remaining questions.

Repartition it to 24 partitions and save it to
parquet.

What's the size of the folder with results (in MB)?

``` python
# Ingest parquet data (csv version deprecated) and store in directory called 'data'
!sudo wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-02.parquet -P data

# Initialise params
filename = 'fhvhv_tripdata_2021-02.parquet'
print(f'Processing data for {filename}...')
input_path = 'data/'
output_path = 'data_partition/'

# Read parquet as pandas df
df_fhvhv = pd.read_parquet('/data/fhvhv_tripdata_2021-02.parquet')

# Overwrite pd df as spark df
df_fhvhv = spark.read \
    .option("header", "true") \
    .parquet(input_path)

#Repartition to 24 partitions
df_fhvhv \
    .repartition(24) \
    .write.parquet(output_path)

!du -sh ../data_partition/
```
Answer: 515MB


## Question 3. Count records 

How many taxi trips were there on February 15?

Consider only trips that started on February 15.

``` python
# Read partitioned parquet by parsing directory as spark df
df_fhvhv_pt = spark.read.parquet('data_partition')

# Register temp table to do SQL on it
df_fhvhv_pt.registerTempTable('fhvhv_pt')

# Count trips starting on 2021-01-15
spark.sql("""
SELECT
    COUNT(*)
FROM
    fhvhv_pt
WHERE
    TO_DATE(pickup_datetime) = "2021-02-15";
""").show()

# Sanity check count for each day of the month
spark.sql("""
SELECT
    EXTRACT(DAY FROM pickup_datetime) AS day,
    COUNT(EXTRACT(DAY FROM pickup_datetime))
FROM
    fhvhv_pt
GROUP BY day
ORDER BY day
-- WHERE
--    "
""").show()
```

Answer: 367170

## Question 4. Longest trip for each day

Now calculate the duration for each trip.

Trip starting on which day was the longest?

``` python
# Let's try this with built in spark functions/operators
from pyspark.sql.functions import *
select_cols = ['pickup_datetime', 'dropoff_datetime', 'TripDurationInSeconds', 'TripDurationInMinutes']
df_fhvhv_pt_duration=df_fhvhv_pt \
    .withColumn('TripDurationInSeconds', col("dropoff_datetime").cast("long") - col('pickup_datetime').cast("long")) \
    .withColumn('TripDurationInMinutes', round(col("TripDurationInSeconds")/60,1)) \
    .select(select_cols)
df_fhvhv_pt_duration.show(3)
```
```
+-------------------+-------------------+---------------------+---------------------+
|    pickup_datetime|   dropoff_datetime|TripDurationInSeconds|TripDurationInMinutes|
+-------------------+-------------------+---------------------+---------------------+
|2021-02-11 20:38:00|2021-02-11 20:42:37|                  277|                  4.6|
|2021-02-13 23:07:38|2021-02-13 23:12:57|                  319|                  5.3|
|2021-02-28 20:52:18|2021-02-28 21:01:00|                  522|                  8.7|
+-------------------+-------------------+---------------------+---------------------+
```

``` python
df_fhvhv_pt_duration \
    .groupby(to_date('pickup_datetime')) \
    .avg() \
    .sort(avg('TripDurationInMinutes').desc()) \
    .show(3)
```
```
+--------------------------+--------------------------+--------------------------+
|to_date(`pickup_datetime`)|avg(TripDurationInSeconds)|avg(TripDurationInMinutes)|
+--------------------------+--------------------------+--------------------------+
|                2021-02-04|        1147.4014095252628|        19.131712649487973|
|                2021-02-05|         1099.123832937176|         18.32713455007255|
|                2021-02-26|        1095.1929630896173|        18.261583764791027|
+--------------------------+--------------------------+--------------------------+
```

Answer: 2021-02-04

## Question 5. Most frequent `dispatching_base_num`

Now find the most frequently occurring `dispatching_base_num` 
in this dataset.

How many stages this spark job has?

> Note: the answer may depend on how you write the query,
> so there are multiple correct answers. 
> Select the one you have.

``` python
df_fhvhv_pt \
    .groupby("dispatching_base_num") \
    .count() \
    .sort((col("count")).desc()) \
    .show(3)
```
```
+--------------------+-------+
|dispatching_base_num|  count|
+--------------------+-------+
|              B02510|3233664|
|              B02764| 965568|
|              B02872| 882689|
+--------------------+-------+
```
Answer: B02510 (2 stages, 200+5 tasks)

## Question 6. Most common locations pair

Find the most common pickup-dropoff pair. 

For example:

"Jamaica Bay / Clinton East"

Enter two zone names separated by a slash

If any of the zone names are unknown (missing), use "Unknown". For example, "Unknown / Clinton East". 

``` python
# Download taxi lookup table csv file
!sudo wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

# Read taxi zone csv in to pandas df
df_zone = pd.read_csv('taxi+_zone_lookup.csv')

# Create speak df
df_zone = spark.read \
    .option("header", "true") \
    .csv('data/taxi+_zone_lookup.csv')

# Join tables
df_join = df_fhvhv_pt \
    .join(df_zone.alias("pickup_zone"), col("PULocationID") == col("pickup_zone.LocationID")) \
    .join(df_zone.alias('dropoff_zone'), col("DOLocationID") == col("dropoff_zone.LocationID"))
df_result = df_join \
    .select(col('pickup_zone.Zone').alias('pickup_zone'),
        col('dropoff_zone.Zone').alias('dropoff_zone'),
        concat_ws('/', 'pickup_zone.Zone','dropoff_zone.Zone').alias("Pair"))
# Find highest pickup/dropoff pair
df_result.groupby('Pair').count().sort(col('count').desc()).show(3, truncate=False)
```
```
+---------------------------+-----+
|Pair                       |count|
+---------------------------+-----+
|East New York/East New York|45041|
|Borough Park/Borough Park  |37329|
|Canarsie/Canarsie          |28026|
+---------------------------+-----+
```

Answer: East New York/East New York

## Bonus question. Join type

(not graded) 

For finding the answer to Q6, you'll need to perform a join.

What type of join is it?

And how many stages your spark job has?

Answer: Inner join. Stages: 1 (joining small table with big table so shuffling is not required, broadcasting is used instead)


## Submitting the solutions

* Form for submitting: https://forms.gle/dBkVK9yT8cSMDwuw7
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 07 March (Monday), 22:00 CET
