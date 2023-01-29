## Week 3 Homework

### Question 1: 
**What is count for fhv vehicles data for year 2019**  
Can load the data for cloud storage and run a count(*)
```sql
SELECT 
COUNT(*)
FROM `ny-rides-kelvin.trips_data_all.fhv_external_table`
LIMIT 10
```

Output: 43261276 (Total 12 months)
Sanity check other months:
23159064 (Jan)
1707650 (Feb)
2044196 (Dec)

### Question 2: 
**How many distinct dispatching_base_num we have in fhv for 2019**  
Can run a distinct query on the table from question 1
```sql
SELECT 
COUNT(DISTINCT(dispatching_base_num))
FROM `ny-rides-kelvin.trips_data_all.fhv_external_table`
LIMIT 10
```
Output: 799

### Question 3: 
**Best strategy to optimise if query always filter by dropoff_datetime and order by dispatching_base_num**  
Review partitioning and clustering video.   
We need to think what will be the most optimal strategy to improve query 
performance and reduce cost.

``` sql
CREATE OR REPLACE TABLE `trips_data_all.fhv_tripdata_partitioned`
PARTITION BY DATE(dropOff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `trips_data_all.fhv_tripdata_partitioned`
);
```

### Question 4: 
**What is the count, estimated and actual data processed for query which counts trip between 2019/01/01 and 2019/03/31 for dispatching_base_num B00987, B02060, B02279**  
Create a table with optimized clustering and partitioning, and run a 
count(*). Estimated data processed can be found in top right corner and
actual data processed can be found after the query is executed.

``` sql 
SELECT COUNT(*)
FROM `ny-rides-kelvin.trips_data_all.fhv_tripdata_partitioned`
WHERE dropOff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
```
Estimated: 403.76MB
Actual 63.31 MB

``` sql 
SELECT COUNT(*)
FROM `ny-rides-kelvin.trips_data_all.fhv_tripdata_non_partitioned`
WHERE dropOff_datetime BETWEEN '2019-01-01' AND '2019-03-31'
AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');
```
Actual: 403.77 MB

### Question 5: 
**What will be the best partitioning or clustering strategy when filtering on dispatching_base_num and SR_Flag**  
Review partitioning and clustering video. 
Partitioning cannot be created on all data types.

### Question 6: 
**What improvements can be seen by partitioning and clustering for data size less than 1 GB**  
Partitioning and clustering also creates extra metadata.  
Before query execution this metadata needs to be processed.

### (Not required) Question 7: 
**In which format does BigQuery save data**  
Review big query internals video.
BigQuery stores data in a columnar format called Capacitor.