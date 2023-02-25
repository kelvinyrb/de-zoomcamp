## Week 5 Homework 

In this homework we'll put what we learned about Spark in practice.

For this homework we will be using the FHVHV 2021-06 data found here. [FHVHV Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhvhv/fhvhv_tripdata_2021-06.csv.gz )


### Question 1: 

**Install Spark and PySpark** 

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?
- 3.3.2
- 2.1.4
- 1.2.3
- 5.4
</br></br>

``` python
import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

spark.version
```

Ans: 3.3.2 is the closest (3.0.3)

### Question 2: 

**HVFHW June 2021**

Read it with Spark using the same schema as we did in the lessons.</br> 
We will use this dataset for all the remaining questions.</br>
Repartition it to 12 partitions and save it to parquet.</br>
What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.</br>


- 2MB
- 24MB
- 100MB
- 250MB
</br></br>

``` python
# Ingest parquet data (csv version deprecated) and store in directory called 'data'
!sudo wget https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_2021-06.parquet -P data

# Initialise params
filename = 'fhvhv_tripdata_2021-06.parquet'
print(f'Processing data for {filename}...')
input_path = 'data/'
output_path = 'data_partition/'

# Read parquet as pandas df
df_fhvhv = pd.read_parquet('/data/fhvhv_tripdata_2021-06.parquet')

# Overwrite pd df as spark df
df_fhvhv = spark.read \
    .option("header", "true") \
    .parquet(input_path)

#Repartition to 12 partitions
df_fhvhv \
    .repartition(12) \
    .write.parquet(output_path)
```

57.2MB (Closest is 24MB)

### Question 3: 

**Count records**  

How many taxi trips were there on June 15?</br></br>
Consider only trips that started on June 15.</br>

- 308,164
- 12,856
- 452,470
- 50,982
</br></br>

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
```

Ans: 452,470


### Question 4: 

**Longest trip for each day**  

Now calculate the duration for each trip.</br>
How long was the longest trip in Hours?</br>

- 66.87 Hours
- 243.44 Hours
- 7.68 Hours
- 3.32 Hours
</br></br>

``` python
# Let's try this with built in spark functions/operators
from pyspark.sql.functions import *
select_cols = ['pickup_datetime', 'dropoff_datetime', 'TripDurationInSeconds', 'TripDurationInMinutes','TripDurationInHours']
df_fhvhv_pt_duration=df_fhvhv_pt \
    .withColumn('TripDurationInSeconds', col("dropoff_datetime").cast("long") - col('pickup_datetime').cast("long")) \
    .withColumn('TripDurationInMinutes', round(col("TripDurationInSeconds")/60,1)) \
    .withColumn('TripDurationInHours', round(col("TripDurationInMinutes")/60,1)) \
    .select(select_cols)
df_fhvhv_pt_duration.show(10)

df_fhvhv_pt_duration \
    .sort(col('TripDurationInHours').desc()) \
    .show(3)
```

Ans: 66.87 hours

### Question 5: 

**User Interface**

 Spark’s User Interface which shows application's dashboard runs on which local port?</br>

- 80
- 443
- 4040
- 8080
</br></br>

Ans: 4040

### Question 6: 

**Most frequent pickup location zone**

Load the zone lookup data into a temp view in Spark</br>
[Zone Data](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv)</br>

Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?</br>

- East Chelsea
- Astoria
- Union Sq
- Crown Heights North
</br></br>

``` python
# Download taxi lookup table csv file
!sudo wget https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv

# Read taxi zone csv in to pandas df
df_zone = pd.read_csv('taxi+_zone_lookup.csv')

# Create speak df
df_zone = spark.read \
    .option("header", "true") \
    .csv('taxi+_zone_lookup.csv')

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
+-------------------+------+
|pickup_zone        |count |
+-------------------+------+
|Crown Heights North|231279|
|East Village       |221244|
|JFK Airport        |188867|
+-------------------+------+
```

Ans: Crown Heights North

## Submitting the solutions

* Form for submitting: https://forms.gle/EcSvDs6vp64gcGuD8
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 06 March (Monday), 22:00 CET


## Solution

We will publish the solution here
