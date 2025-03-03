select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    

with child as (
    select dropoff_location_id as from_field
    from nyc_taxi_db.conformed.fact_taxi_trips
    where dropoff_location_id is not null
),

parent as (
    select location_id as to_field
    from nyc_taxi_db.conformed.dim_location
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



      
    ) dbt_internal_test