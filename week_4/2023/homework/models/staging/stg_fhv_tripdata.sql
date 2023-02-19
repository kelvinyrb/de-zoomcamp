{{ config(materialized='view') }}

select
    -- fhv trip data schema
    cast(dispatching_base_num as string) as dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(PUlocationID as integer) as PUlocationID,
    cast(DOlocationID as integer) as DOlocationID,
    -- cast(SR_Flag as integer) as SR_Flag,
    cast(Affiliated_base_number as string) as Affiliated_base_number,
from {{ source('staging','fhv_casted_tripdata') }}

-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}