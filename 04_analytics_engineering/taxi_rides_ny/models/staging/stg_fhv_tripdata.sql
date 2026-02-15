{{ config(materialized='view') }}

with tripdata as 
(
  select *
  from {{ source('raw', 'fhv_tripdata') }}
  where dispatching_base_num is not null
)

select
    -- identifiers
    dispatching_base_num,
    
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- location IDs
    cast(pulocationid as integer) as pickup_location_id,
    cast(dolocationid as integer) as dropoff_location_id,
   

from tripdata
