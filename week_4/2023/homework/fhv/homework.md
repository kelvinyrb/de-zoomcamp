## Week 4 Homework 

In this homework, we'll use the models developed during the week 4 videos and enhance the already presented dbt project using the already loaded Taxi data for fhv vehicles for year 2019 in our DWH.

We will use the data loaded for:

* Building a source table: `stg_fhv_tripdata`
* Building a fact table: `fact_fhv_trips`
* Create a dashboard 

If you don't have access to GCP, you can do this locally using the ingested data from your Postgres database
instead. If you have access to GCP, you don't need to do it for local Postgres -
only if you want to.

> **Note**: if your answer doesn't match exactly, select the closest option 

### Question 1: 

**What is the count of records in the model fact_trips after running all models with the test run variable disabled and filtering for 2019 and 2020 data only (pickup datetime)** 

You'll need to have completed the "Build the first dbt models" video and have been able to run the models via the CLI. 
You should find the views and models for querying in your DWH.

``` sql
-- Create external table from gcs data (yellow)
CREATE OR REPLACE EXTERNAL TABLE ny-rides-kelvin.trips_data_all.yellow_tripdata_2019_2020
OPTIONS(
  format = 'parquet',
  uris = ['gs://dtc_data_lake_ny-rides-kelvin/yellow_parquet/*']
);
``` 
``` sql
-- Create table from external table (yellow)
CREATE OR REPLACE TABLE ny-rides-kelvin.trips_data_all.yellow_tripdata_2019_2020_table AS
SELECT * FROM ny-rides-kelvin.trips_data_all.yellow_tripdata_2019_2020;
```
``` sql
-- Create external table from gcs data (green)
CREATE OR REPLACE EXTERNAL TABLE ny-rides-kelvin.trips_data_all.green_tripdata_2019_2020
OPTIONS(
  format = 'parquet',
  uris = ['gs://dtc_data_lake_ny-rides-kelvin/green_parquet/*']
);
``` 
``` sql
-- Create table from external table (green)
CREATE OR REPLACE TABLE ny-rides-kelvin.trips_data_all.green_tripdata_2019_2020_table AS
SELECT * FROM ny-rides-kelvin.trips_data_all.green_tripdata_2019_2020;
```

1. Go to `cloud.getdbt.com`
2. Set project directory to week_4/2023/homework/greenyellow
3. `dbt deps`
4. `dbt build --select +fact_trips --var "is_test_run: false"`
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

- 41648442
- 51648442
- 61648442
- 71648442

Ans: 61,602,960

2019: 45691959

2020: 15911001


### Question 2: 

**What is the distribution between service type filtering by years 2019 and 2020 data as done in the videos**

You will need to complete "Visualising the data" videos, either using data studio or metabase. 

- 89.9/10.1
- 94/6
- 76.3/23.7
- 99.1/0.9

Ans: 89.8% / 10.2%


### Question 3: 

**What is the count of records in the model stg_fhv_tripdata after running all models with the test run variable disabled (:false)**  

Create a staging model for the fhv data for 2019 and do not add a deduplication step. Run it via the CLI without limits (is_test_run: false).
Filter records with pickup time in year 2019.

- Run web_to_gcs.py to get fhv trip data files as parquet and upload to gcs folder
``` sql
-- Create external table from gcs data
CREATE OR REPLACE EXTERNAL TABLE ny-rides-kelvin.trips_data_all.fhv_tripdata_2019_2020
  format = 'parquet',
  uris = ['gs://dtc_data_lake_ny-rides-kelvin/fhv_parquet/*']
);
``` 

``` sql
-- Create table from external table
CREATE OR REPLACE TABLE ny-rides-kelvin.trips_data_all.fhv_tripdata_2019_2020_table AS
SELECT * FROM ny-rides-kelvin.trips_data_all.fhv_tripdata_2019_2020;
```

`dbt build --select stg_fhv_tripdata --var "is_test_run: false"`
``` sql
SELECT COUNT(pickup_datetime)
FROM `ny-rides-kelvin.dbt_nytaxi.stg_fhv_tripdata`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
```

- 33244696
- 43244696
- 53244696
- 63244696

Ans: 43244696

### Question 4: 

**What is the count of records in the model fact_fhv_trips after running all dependencies with the test run variable disabled (:false)**  

Create a core model for the stg_fhv_tripdata joining with dim_zones.
Similar to what we've done in fact_trips, keep only records with known pickup and dropoff locations entries for pickup and dropoff locations. 
Run it via the CLI without limits (is_test_run: false) and filter records with pickup time in year 2019.

`dbt build --select +fact_fhv_trips --var "is_test_run: false"`

- 12998722
- 22998722
- 32998722
- 42998722

Ans: 22998722

### Question 5: 

**What is the month with the biggest amount of rides after building a tile for the fact_fhv_trips table**
Create a dashboard with some tiles that you find interesting to explore the data. One tile should show the amount of trips per month, as done in the videos for fact_trips, based on the fact_fhv_trips table.

- March
- April
- January
- December

Check with SQL: 
```sql 
SELECT
EXTRACT(MONTH FROM pickup_datetime) as month,
COUNT(EXTRACT(MONTH FROM pickup_datetime))
FROM `ny-rides-kelvin.dbt_nytaxi.fact_fhv_trips`
WHERE EXTRACT(YEAR FROM pickup_datetime) = 2019
GROUP BY month
ORDER BY month ASC
```

Ans: January

## Submitting the solutions

* Form for submitting: https://forms.gle/6A94GPutZJTuT5Y16
* You can submit your homework multiple times. In this case, only the last submission will be used. 

Deadline: 25 February (Saturday), 22:00 CET


## Solution

We will publish the solution here