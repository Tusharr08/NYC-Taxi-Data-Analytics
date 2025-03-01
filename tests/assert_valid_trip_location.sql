Select *
from {{ ref('prc_taxi_trips')}}
where PULOCATIONID=DOLOCATIONID