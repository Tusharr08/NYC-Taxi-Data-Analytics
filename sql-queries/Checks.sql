
Select count(*) from raw.nyc_taxi_trips; --5980721 -> 32376464 -> 9384487 -> 18455731

LS @RAW.SNFK_S3_RAW_NYC_STAGE/year_wise/;
SELECT COUNT(*) 
FROM TABLE(
    RESULT_SCAN(LAST_QUERY_ID())
);

select *
from table(information_schema.stage_storage_usage_history(date_range_start => dateadd('days',-10,current_date()),current_date()));


ALTER PIPE NYC_TAXI_DB.RAW.nyc_taxi_pipe REFRESH;

SELECT * from information_schema.load_history;

LS @RAW.SNFK_S3_RAW_NYC_STAGE/year_wise/;

select *
from table(information_schema.stage_storage_usage_history(dateadd(hour,-1,current_date()),current_date()));

SELECT *
  FROM TABLE(information_schema.stage_directory_file_registration_history(
    START_TIME=>DATEADD('minute',-1,current_timestamp()),
    STAGE_NAME=>'RAW.SNFK_S3_RAW_NYC_STAGE')) ;


SELECT * FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
    table_name => 'raw.nyc_taxi_trips',
    start_time => DATEADD(minute, -30, CURRENT_TIMESTAMP())
));




Select count(*) from processed.prc_taxi_trips; --74297

drop schema analytics;

Select count(*) from analytics.dim_location; --530
Select location_id, count(*) from dim_location group by location_id having count(*)>1;

Select count(*) from analytics.dim_time; --156507 -> 5045737
Select time_id, count(*) from dim_time group by time_id having count(*)>1;

Select count(*) from analytics.dim_taxi_trips; --74297 -> 82605
SELECT trip_id, COUNT(*)
FROM dim_taxi_trips
GROUP BY trip_id
HAVING COUNT(*) > 1;

SELECT count(*) from analytics.fact_taxi_trips; --74297 
SELECT trip_id, count(*) from analytics.fact_taxi_trips group by trip_id having count(*)>1;

-- DROP TABLE ANALYTICS.FACT_TAXI_TRIPS;
-- DROP TABLE analytics.dim_taxi_trips;
-- DROP TABLE analytics.dim_time;
-- DROP TABLE analytics.dim_location;
-- DROP TABLE processed.prc_taxi_trips;

TRUNCATE TABLE raw.nyc_taxi_trips;

Select distinct ratecodeid from raw.nyc_taxi_trips;

Select count(*) from raw.nyc_taxi_trips where ratecodeid=99 or ratecodeid is null;
select count(*) from raw.nyc_taxi_trips where PULOCATIONID=DOLOCATIONID;
select count(*) from raw.nyc_taxi_trips where TPEP_PICKUP_DATETIME<TPEP_DROPOFF_DATETIME;


Select distinct payment_type from raw.nyc_taxi_trips;
Select distinct store_and_fwd_flag from raw.nyc_taxi_trips;
Select distinct ratecodeid from raw.nyc_taxi_trips;
SHOW PIPES;