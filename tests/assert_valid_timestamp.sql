Select * 
from {{ ref('prc_taxi_trips')}}
where pickup_datetime > dropoff_datetime