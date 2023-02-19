{{ config(materialized='table') }}

with fhv_data as (
    select *, 
        'fhv' as service_type 
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)
select 
    dispatching_base_num, 
    pickup_datetime, 
    dropoff_datetime, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    Affiliated_base_number
from fhv_data
inner join dim_zones as pickup_zone
on fhv_data.PUlocationID = pickup_zone.locationid
inner join dim_zones as dropoff_zone
on fhv_data.DOlocationID = dropoff_zone.locationid