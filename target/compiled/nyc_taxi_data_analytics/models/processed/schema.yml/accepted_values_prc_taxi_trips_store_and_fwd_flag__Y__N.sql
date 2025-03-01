
    
    

with all_values as (

    select
        store_and_fwd_flag as value_field,
        count(*) as n_records

    from nyc_taxi_db.processed.prc_taxi_trips
    group by store_and_fwd_flag

)

select *
from all_values
where value_field not in (
    'Y','N'
)


