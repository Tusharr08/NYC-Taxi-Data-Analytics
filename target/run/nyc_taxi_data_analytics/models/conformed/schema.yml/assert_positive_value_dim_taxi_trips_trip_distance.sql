select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
Select *
from nyc_taxi_db.conformed.dim_taxi_trips
where trip_distance<0

      
    ) dbt_internal_test