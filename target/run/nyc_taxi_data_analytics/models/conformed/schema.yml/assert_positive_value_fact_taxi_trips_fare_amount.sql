select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
Select *
from nyc_taxi_db.conformed.fact_taxi_trips
where fare_amount<0

      
    ) dbt_internal_test