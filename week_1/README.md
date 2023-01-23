# Week 1 Homework
## Q1
```bash
--iidfile string Write the image ID to the file
```
## Q2
```bash
pip
setuptools
wheel
```
## Q3
``` sql
WITH cte AS (
	SELECT
	lpep_pickup_datetime::timestamp::date pu_time,
	lpep_dropoff_datetime::timestamp::date do_time
	FROM yellow_taxi_data t 
	WHERE
	lpep_pickup_datetime::timestamp::date = '2019-01-15' AND
	lpep_dropoff_datetime::timestamp::date = '2019-01-15'
-- 	LIMIT 100
	)
SELECT
	COUNT(*)
FROM
	cte;
```

## Q4
``` sql
SELECT
lpep_pickup_datetime::timestamp::date pu_time,
trip_distance
FROM yellow_taxi_data
ORDER BY trip_distance DESC
LIMIT 1
```
## Q5
``` sql
WITH cte AS (
	SELECT
	lpep_pickup_datetime::timestamp::date pu_time,
	lpep_dropoff_datetime::timestamp::date do_time,
	passenger_count
	FROM yellow_taxi_data t 
	WHERE
	lpep_pickup_datetime::timestamp::date = '2019-01-01' AND
	-- lpep_dropoff_datetime::timestamp::date = '2019-01-01' AND
	( passenger_count = 2 OR passenger_count = 3)
	)
SELECT
passenger_count,
COUNT(*)
FROM
	cte
GROUP BY passenger_count
```
## Q6
``` sql
WITH cte AS (
	SELECT
	lpep_pickup_datetime::timestamp::date pu_time,
	lpep_dropoff_datetime::timestamp::date do_time,
	passenger_count
	FROM yellow_taxi_data t 
	WHERE
	lpep_pickup_datetime::timestamp::date = '2019-01-01' AND
	-- lpep_dropoff_datetime::timestamp::date = '2019-01-01' AND
	( passenger_count = 2 OR passenger_count = 3)
	)
SELECT
passenger_count,
COUNT(*)
FROM
	cte
GROUP BY passenger_count
```
