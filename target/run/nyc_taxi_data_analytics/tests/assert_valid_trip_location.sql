select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      Select *
from nyc_taxi_db.processed.prc_taxi_trips
where PULOCATIONID=DOLOCATIONID
      
    ) dbt_internal_test