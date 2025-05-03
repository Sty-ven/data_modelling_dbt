{{ config(materialized='table') }}

with source_data as (
    select *
    from RAW_DB.PUBLIC.YELLOW_TAXI_TRIPS
    limit 10
)
select *
from source_data