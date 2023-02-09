## Week 3 Homework
<b><u>Important Note:</b></u> <p>You can load the data however you would like, but keep the files in .GZ Format. 
If you are using orchestration such as Airflow or Prefect do not load the data into Big Query using the orchestrator.</br> 
Stop with loading the files into a bucket. </br></br>
<u>NOTE:</u> You can use the CSV option for the GZ files when creating an External Table</br>

<b>SETUP:</b></br>
Create an external table using the fhv 2019 data. </br>
Create a table in BQ using the fhv 2019 data (do not partition or cluster this table). </br>
Data can be found here: https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/fhv </p>

## Question 1:
What is the count for fhv vehicle records for year 2019?
- 65,623,481
- 43,244,696
- 22,978,333
- 13,942,414

``` sql
-- Create external table from gcs data
CREATE OR REPLACE EXTERNAL TABLE ny-rides-kelvin.trips_data_all.fhv_trip_data_2019_ext_table
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_ny-rides-kelvin/fhv_csv/fhv_tripdata_2019*']
);
```

``` sql
-- Create table from external table (non-partitioned, not clustered)
CREATE OR REPLACE TABLE ny-rides-kelvin.trips_data_all.fhv_trip_data_2019_table AS
SELECT * FROM ny-rides-kelvin.trips_data_all.fhv_trip_data_2019_ext_table;
```

Answer: 43244696

## Question 2:
Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.</br> 
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

- 25.2 MB for the External Table and 100.87MB for the BQ Table
- 225.82 MB for the External Table and 47.60MB for the BQ Table
- 0 MB for the External Table and 0MB for the BQ Table
- 0 MB for the External Table and 317.94MB for the BQ Table 

Query for external table:
```sql
SELECT 
COUNT(DISTINCT(affiliated_base_number))
FROM `ny-rides-kelvin.trips_data_all.fhv_trip_data_2019`
```
Query for table:
```sql
SELECT 
COUNT(DISTINCT(affiliated_base_number))
FROM `ny-rides-kelvin.trips_data_all.fhv_trip_data_2019_table`
```

Answer: 0 MB and 317.94 MB for external table and table respectively. (D)

## Question 3:
How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?
- 717,748
- 1,215,687
- 5
- 20,332

``` sql
SELECT 
COUNT(*)
FROM `ny-rides-kelvin.trips_data_all.fhv_trip_data_2019_table`
WHERE
PUlocationID IS NULL
AND
DOlocationID IS NULL;
```
Answer: 717748

## Question 4:
What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?
- Cluster on pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Cluster on affiliated_base_number
- Partition by pickup_datetime Partition by affiliated_base_number
- Partition by affiliated_base_number Cluster on pickup_datetime

Create partitioned table:
``` sql
CREATE OR REPLACE TABLE trips_data_all.fhv_trip_data_2019_partitioned
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS (
  SELECT * FROM trips_data_all.fhv_trip_data_2019_table
);
```

Answer: Partition by pickup_datetime and cluster by affiliated_base_number (B)

## Question 5:
Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).</br> 
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.
- 12.82 MB for non-partitioned table and 647.87 MB for the partitioned table
- 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table
- 582.63 MB for non-partitioned table and 0 MB for the partitioned table
- 646.25 MB for non-partitioned table and 646.25 MB for the partitioned table

Query for non-partitioned table:
``` sql 
SELECT
COUNT(DISTINCT(affiliated_base_number))
FROM ny-rides-kelvin.trips_data_all.fhv_trip_data_2019_table
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
```

Query for partitioned table:
``` sql 
SELECT
COUNT(DISTINCT(affiliated_base_number))
FROM ny-rides-kelvin.trips_data_all.fhv_trip_data_2019_partitioned
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';
```

Answer: 647.87 MB and 23.05 MB for non-partitioned and partitioned table respectively. (B is closest)


## Question 6: 
Where is the data stored in the External Table you created?

- Big Query
- GCP Bucket
- Container Registry
- Big Table

Answer: GCP Bucket


## Question 7:
It is best practice in Big Query to always cluster your data:
- True
- False

Answer: False. Clustering in BigQuery is optional and depends on the specific use case. It can improve query performance by physically rearranging the data based on the values of a specific column or columns, but it also requires additional storage and processing resources. Whether or not to cluster the data should be determined based on the performance requirements and trade-offs for each specific use case.


## (Not required) Question 8:
A better format to store these files may be parquet. Create a data pipeline to download the gzip files and convert them into parquet. Upload the files to your GCP Bucket and create an External and BQ Table. 


Note: Column types for all files used in an External Table must have the same datatype. While an External Table may be created and shown in the side panel in Big Query, this will need to be validated by running a count query on the External Table to check if any errors occur. 
 
## Submitting the solutions

* Form for submitting: https://forms.gle/rLdvQW2igsAT73HTA
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 13 February (Monday), 22:00 CET


## Solution

We will publish the solution here
