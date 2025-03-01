NYC_TAXI_DB.RAW.NYC_TAXI_PIPEUSE ROLE SYSADMIN;

CREATE OR REPLACE DATABASE NYC_TAXI_DB;
CREATE OR REPLACE SCHEMA RAW; -- INGESTION LAYER
CREATE OR REPLACE SCHEMA PROCESSED; --FOR PROCESSING TAXI TRIPS
CREATE OR REPLACE SCHEMA STAGING; --FOR STORING STATIC FILES
CREATE OR REPLACE SCHEMA CONFORMED; --DIM AND FACT TABLES
CREATE OR REPLACE SCHEMA ANALYTICS; --STREAMLIT VIEWS
CREATE OR REPLACE SCHEMA METADATA;

DROP SCHEMA STAGING_RAW;
DROP SCHEMA RAW_RAW;
SELECT COUNT(*) FROM STAGING_RAW.NYC_TAXI_TRIPS;
SELECT COUNT(*) FROM NYC_TAXI_DB.STAGING_RAW.RAW_LOAD_NYC_TAXI;

SHOW DATABASES;
--DROP DATABASE NYC_TAXI_DB;

CREATE OR REPLACE WAREHOUSE NYC_TAXI_WH 
    WAREHOUSE_SIZE = 'XSMALL' 
    WAREHOUSE_TYPE = 'STANDARD' 
    AUTO_SUSPEND = 600 
    AUTO_RESUME = TRUE 
    MIN_CLUSTER_COUNT = 1 
    MAX_CLUSTER_COUNT = 2 
    SCALING_POLICY = 'STANDARD'
COMMENT = 'Data Science Warehouse for analyzing NYC Taxi Data';

SHOW WAREHOUSES;
--DROP WAREHOUSE TAXI_WH;

CREATE STORAGE INTEGRATION SNFK_S3_NYC_INT //USE ROLE ACCOUNTADMIN ONLY
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::863518416041:role/nyc-taxi-snowflake-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://nyc-taxi-data-analytics/');

GRANT USAGE ON INTEGRATION SNFK_S3_NYC_INT TO ROLE SYSADMIN;

SHOW INTEGRATIONS;
DESC INTEGRATION SNFK_S3_NYC_INT;

CREATE OR REPLACE FILE FORMAT nyc_parquet_format 
TYPE = PARQUET;

--External stage to ingest .parquet files from s3 to raw schema
CREATE OR REPLACE STAGE SNFK_S3_RAW_NYC_STAGE
  STORAGE_INTEGRATION = SNFK_S3_NYC_INT
  URL = 's3://nyc-taxi-data-analytics/raw/'
  FILE_FORMAT = raw.nyc_parquet_format;

LIST @SNFK_S3_RAW_NYC_STAGE/year_wise/;


--External stage to export files from snowflake schema to s3
CREATE OR REPLACE STAGE SNFK_S3_EXPORT_NYC_STAGE
STORAGE_INTEGRATION = SNFK_S3_NYC_INT
URL = 's3://nyc-taxi-data-analytics/'
FILE_FORMAT = raw.nyc_csv_format;

LS @SNFK_S3_EXPORT_NYC_STAGE;

SHOW STAGES;


USE SCHEMA RAW;
ALTER WAREHOUSE NYC_TAXI_WH SET WAREHOUSE_SIZE = 'MEDIUM';

-- CHECK WHAT KIND OF DATA IS THERE IN PARQUET FILE:
SELECT 
    ARRAY_TO_STRING(
    ARRAY_AGG(DISTINCT column_name || ' ' || type), ', ')
FROM TABLE(
    INFER_SCHEMA(
        LOCATION=>'@SNFK_S3_RAW_NYC_STAGE/2022/', 
        FILE_FORMAT=>'nyc_parquet_format'
    )
);

-- SELECT * 
-- FROM TABLE(
--     INFER_SCHEMA(
--         LOCATION=>'@SNFK_S3_RAW_NYC_STAGE/raw/2022/', 
--         FILE_FORMAT=>'nyc_parquet_format'
--     )
-- );

CREATE OR REPLACE TABLE NYC_TAXI_TRIPS (
    trip_distance	REAL,
    tpep_pickup_datetime	NUMBER(38,0),
    tpep_dropoff_datetime	NUMBER(38,0),
    total_amount	REAL,
    tolls_amount	REAL,
    tip_amount	REAL,
    store_and_fwd_flag	VARCHAR,
    payment_type	NUMBER(38, 0),
    passenger_count	REAL,
    mta_tax	REAL,
    improvement_surcharge	REAL,
    fare_amount	REAL,
    extra	REAL,
    congestion_surcharge	REAL,
    airport_fee	REAL,
    VendorID	NUMBER(38, 0),
    RatecodeID	REAL,
    PULocationID	NUMBER(38, 0),
    DOLocationID	NUMBER(38, 0)
);




COPY INTO RAW.NYC_TAXI_TRIPS (trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime, total_amount, tolls_amount, tip_amount, store_and_fwd_flag, payment_type, passenger_count, mta_tax, improvement_surcharge, fare_amount, extra, congestion_surcharge, airport_fee, VendorID, RatecodeID, PULocationID, DOLocationID)
FROM (
    SELECT 
        $1:trip_distance::REAL,
        $1:tpep_pickup_datetime::NUMBER(38, 0),
        $1:tpep_dropoff_datetime::NUMBER(38, 0),
        $1:total_amount::REAL,
        $1:tolls_amount::REAL,
        $1:tip_amount::REAL,
        $1:store_and_fwd_flag::STRING,
        $1:payment_type::NUMBER(38,0),
        $1:passenger_count::REAL,
        $1:mta_tax::REAL,
        $1:improvement_surcharge::REAL,
        $1:fare_amount::REAL,
        $1:extra::REAL,
        $1:congestion_surcharge::REAL,
        $1:airport_fee::REAL,
        $1:VendorID::NUMBER(38,0),
        $1:RatecodeID::REAL,
        $1:PULocationID::NUMBER(38,0),
        $1:DOLocationID::NUMBER(38,0)
    FROM @SNFK_S3_RAW_NYC_STAGE/year_wise
) 
FILE_FORMAT = (FORMAT_NAME = nyc_parquet_format)
on_error = continue;


LS @RAW.SNFK_S3_RAW_NYC_STAGE/year_wise;


SELECT COUNT(*) FROM RAW.NYC_TAXI_TRIPS;
SELECT * FROM RAW.NYC_TAXI_TRIPS limit 100;
TRUNCATE TABLE NYC_TAXI_TRIPS;

-----creating snow pipe for auto-ingestion of data

CREATE or replace PIPE RAW.nyc_taxi_pipe
  AUTO_INGEST = TRUE 
  AS
  COPY INTO RAW.NYC_TAXI_TRIPS (trip_distance, tpep_pickup_datetime, tpep_dropoff_datetime, total_amount, tolls_amount, tip_amount, store_and_fwd_flag, payment_type, passenger_count, mta_tax, improvement_surcharge, fare_amount, extra, congestion_surcharge, airport_fee, VendorID, RatecodeID, PULocationID, DOLocationID)
FROM (
    SELECT 
        $1:trip_distance::REAL,
        $1:tpep_pickup_datetime::NUMBER(38, 0),
        $1:tpep_dropoff_datetime::NUMBER(38, 0),
        $1:total_amount::REAL,
        $1:tolls_amount::REAL,
        $1:tip_amount::REAL,
        $1:store_and_fwd_flag::STRING,
        $1:payment_type::NUMBER(38,0),
        $1:passenger_count::REAL,
        $1:mta_tax::REAL,
        $1:improvement_surcharge::REAL,
        $1:fare_amount::REAL,
        $1:extra::REAL,
        $1:congestion_surcharge::REAL,
        $1:airport_fee::REAL,
        $1:VendorID::NUMBER(38,0),
        $1:RatecodeID::REAL,
        $1:PULocationID::NUMBER(38,0),
        $1:DOLocationID::NUMBER(38,0)
    FROM @RAW.SNFK_S3_RAW_NYC_STAGE/year_wise/
) 
FILE_FORMAT = (FORMAT_NAME = nyc_parquet_format)
on_error = CONTINUE;

SHOW PIPES;
SHOW PIPES IN SCHEMA RAW;
DESC PIPE nyc_taxi_pipe;

SELECT COUNT(*) FROM RAW.NYC_TAXI_TRIPS; -- 32376464
--TRUNCATE TABLE RAW.NYC_TAXI_TRIPS;



SHOW FILE FORMATS;

LS @SNFK_S3_RAW_NYC_STAGE;

DESC STAGE SNFK_S3_RAW_NYC_STAGE;

ALTER PIPE nyc_taxi_pipe REFRESH;

-----------taxi Zone lookup table-------------------------

CREATE OR REPLACE FILE FORMAT NYC_CSV_FORMAT
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
FIELD_DELIMITER = ','
EMPTY_FIELD_AS_NULL = TRUE;


-- CHECK WHAT KIND OF DATA IS THERE IN PARQUET FILE:
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION=>'@SNFK_S3_RAW_NYC_STAGE/location/taxi_zone_lookup.csv', 
        FILE_FORMAT=>'NYC_CSV_FORMAT'
    )
);


CREATE OR REPLACE TABLE RAW.TAXI_ZONE_LOOKUP (
    LOCATIONID INT PRIMARY KEY,
    BOROUGH  VARCHAR,
    ZONE VARCHAR,
    SERVICE_ZONE VARCHAR
);

COPY INTO TAXI_ZONE_LOOKUP(LocationID, Borough, Zone, service_zone)
FROM @SNFK_S3_RAW_NYC_STAGE/location/taxi_zone_lookup.csv
FILE_FORMAT = (FORMAT_NAME='NYC_CSV_FORMAT');

SELECT * FROM TAXI_ZONE_LOOKUP;

