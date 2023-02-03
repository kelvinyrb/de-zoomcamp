## Week 4 Homework 
In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:
* Building a source table: stg_fhv_tripdata
* Building a fact table: fact_fhv_trips
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

### Question 1: 
**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)**  
You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

1. Go to `cloud.getdbt.com`
2. Set project directory to week_4
3. dbt build --select +fact_trips --var "is_test_run: false"
``` sql
SELECT
EXTRACT(YEAR FROM pickup_datetime),
COUNT(pickup_datetime)
FROM `ny-rides-kelvin.dbt_nytaxi.fact_trips`
WHERE
EXTRACT(YEAR FROM pickup_datetime) = 2019 OR
EXTRACT(YEAR FROM pickup_datetime) = 2020
GROUP BY EXTRACT(YEAR FROM pickup_datetime)
LIMIT 10
```
Answer:
2019 - 45991486
2020 - 16464843

### Question 2: 
**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 
1. Go to Look Studio
2. Look at pie chart that was created earlier
Answer: 89.2% (Yellow), 10.8% (Green)


### Question 3: 
**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

1. Ingest fhv trip data (casted parquet files) using `fhv_to_bqtable_dag.py` > In BigQuery, trips_data_all/fhv_casted_tripdata created
2. `dbt run --select stg_fhv_tripdata --var "is_test_run: false"`
3. 
``` sql
SELECT COUNT(pickup_datetime)
FROM `ny-rides-kelvin.dbt_nytaxi.stg_fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
```
Answer: 43261276

### Question 4: 
**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

1. `dbt build --select +fact_fhv_trips --var "is_test_run: false"`
2. In BQ, dbt_nytaxi/fact_fhv_trips table created
3. 
```sql
SELECT COUNT(pickup_datetime)
FROM `ny-rides-kelvin.dbt_nytaxi.fact_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
```
Answer: 23014060

### Question 5: 
**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.
1. Create bar plot on Looker Studio and filter by 2019 data only
2. Use BigQuery to double check
``` sql
SELECT
EXTRACT(MONTH FROM pickup_datetime) as month,
COUNT(EXTRACT(MONTH FROM pickup_datetime))
FROM `ny-rides-kelvin.dbt_nytaxi.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
GROUP BY month
ORDER BY month ASC;
```
https://lookerstudio.google.com/reporting/78d09ea3-7a5f-4ff8-b791-b88c615acf78/page/coMED/edit
Answer: January. 

