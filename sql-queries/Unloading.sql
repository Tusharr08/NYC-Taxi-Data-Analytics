CREATE OR REPLACE FILE FORMAT NYC_UNLOAD_CSV_FORMAT
TYPE = CSV
COMPRESSION = 'NONE'
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
FILE_EXTENSION = 'csv';


copy into @snfk_s3_export_nyc_stage/processed/csv/prc_taxi_trips/prc
from processed.prc_taxi_trips
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
header = true
overwrite = true;

copy into @snfk_s3_export_nyc_stage/staging/stg_payment_types.csv
from staging.stg_payment_types
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
single = true
header = true;

copy into @snfk_s3_export_nyc_stage/staging/stg_rate_codes.csv
from staging.stg_rate_codes
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
single = true
header = true;

copy into @snfk_s3_export_nyc_stage/staging/stg_vendors.csv
from staging.stg_vendors
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
single = true
header = true;

copy into @snfk_s3_export_nyc_stage/analytics/dim_location/dim_location.csv
from analytics.dim_location
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
single = true
header = true;

copy into @snfk_s3_export_nyc_stage/analytics/dim_time/dim_time
from analytics.dim_time
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
header = true
overwrite = true;

copy into @snfk_s3_export_nyc_stage/analytics/dim_taxi_trips/dim_taxi_trips
from analytics.dim_taxi_trips
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
header = true
overwrite = true;

copy into @snfk_s3_export_nyc_stage/analytics/fact_taxi_trips/fact_taxi_trips
from analytics.fact_taxi_trips
file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
header = true
overwrite = true;