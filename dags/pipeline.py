
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from airflow.utils.decorators import apply_defaults
from datetime import datetime, timedelta
from utils.data_ingestion_to_S3 import upload_files_to_s3

# class SnowPipeIngestionSensor(BaseSensorOperator):
#     """
#     Sensor to check if Snowpipe has ingested all files from the external stage.
#     """
#     @apply_defaults
#     def __init__(self, snowflake_conn_id, stage_name, table_name, *args, **kwargs):
#         super().__init__(*args, **kwargs)
#         self.snowflake_conn_id = snowflake_conn_id
#         self.stage_name  =stage_name
#         self.table_name = table_name

#     def poke(self, context):
#         hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id)

#         # Get timestamps for the last 5 and 10 minutes
#         now = datetime.now()
#         stage_start_time = now - timedelta(minutes=5)
#         copy_history_start_time = now - timedelta(minutes=10)

#         # ✅ Correct Query 1: Count new files added to the external stage in the last 5 minutes
#         stage_query = f"""
#         SELECT *
#         FROM TABLE(information_schema.stage_directory_file_registration_history(
#             START_TIME=>DATEADD('minute',-15,current_timestamp()),
#             STAGE_NAME=> {self.stage_name}));
#         """
#         stage_files = hook.get_first(stage_query)[0]

#         # ✅ Correct Query 2: Count files loaded by Snowpipe into the table in the last 10 minutes
#         copy_history_query = f"""
#             SELECT COUNT(*)
#             FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
#                 TABLE_NAME => '{self.table_name}',
#                 START_TIME => DATEADD(minute, -10 , CURRENT_TIMESTAMP())
#             ))
#         """
#         loaded_files = hook.get_first(copy_history_query)[0]

#         self.log.info(f"Files added to stage in last 5 minutes: {count_files}")
#         self.log.info(f"Files loaded in Snowpipe copy history in last 10 minutes: {loaded_files}")

#         # ✅ Check if all staged files are loaded
#         return count_files == 0 or loaded_files >= count_files
    


default_args = {
    'owner': 'tushar',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 28),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONN_ID= 'snowflake_default'
STAGE_NAME = 'RAW.SNFK_S3_RAW_NYC_STAGE'
TABLE_NAME = 'raw.nyc_taxi_trips'

#dag = DAG('nyc_taxi_etl', default_args=default_args, schedule_interval="*/30 * * * *" , catchup=False)
dag = DAG('nyc_taxi_etl', default_args=default_args, schedule_interval="@daily" , catchup=False)
start = BashOperator(
    task_id="start",
    bash_command="echo 'Starting export process...'",
    dag=dag,
)

# Task 1: Upload raw files to S3
upload_to_s3 = PythonOperator(
    task_id="upload_raw_files_to_s3",
    python_callable= upload_files_to_s3,
    dag=dag
)

#Task 2: Trigger Snowpipe to load data into Snowflake
trigger_snowpipe = SQLExecuteQueryOperator(
    task_id="trigger_snowpipe",
    sql="ALTER PIPE NYC_TAXI_DB.RAW.nyc_taxi_pipe REFRESH;",
    conn_id= SNOWFLAKE_CONN_ID,
    dag=dag
)

#Task 3: List files in external stage
list_stage = SQLExecuteQueryOperator(
    task_id="list_raw_stage",
    sql="LS @RAW.SNFK_S3_RAW_NYC_STAGE/year_wise/;",
    conn_id= SNOWFLAKE_CONN_ID,
    dag=dag
)

# #Task 4: Custom Sensor to wait for Snowpipe ingestion to complete
# wait_for_snowpipe = SnowPipeIngestionSensor(
#     task_id = 'wait_for_snowpipe',
#     snowflake_conn_id=SNOWFLAKE_CONN_ID,
#     stage_name=STAGE_NAME,
#     table_name=TABLE_NAME,
#     poke_interval = 30,
#     timeout = 240,
#     mode = 'poke',
#     dag=dag
# )

#Task 4: Run DBT Models
dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command='cd /Users/tushargupta/Documents/Projects/NYC-Taxi-Data-Analytics && dbt run',
    dag=dag
)

# Task 5: Test DBT Models
dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command='cd /Users/tushargupta/Documents/Projects/NYC-Taxi-Data-Analytics && dbt test',
    dag=dag
)

# Task 6: Export processed data to S3
export_processed = SQLExecuteQueryOperator(
    task_id='export_processed_layer',
    sql="""
        copy into @snfk_s3_export_nyc_stage/processed/prc_taxi_trips/prc
        from processed.prc_taxi_trips
        file_format = ( format_name  = raw.NYC_UNLOAD_CSV_FORMAT)
        header = true
        overwrite = true;
    """,
    conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

#Task 7: Export conformed data to S3
export_conformed_layer = SQLExecuteQueryOperator(
    task_id='export_conformed_layer',
    sql=[
        """
        COPY INTO @snfk_s3_export_nyc_stage/conformed/dim_time/dim_time
        FROM conformed.dim_time
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/conformed/dim_taxi_trips/dim_taxi_trips
        FROM conformed.dim_taxi_trips
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/conformed/fact_taxi_trips/fact_taxi_trips
        FROM conformed.fact_taxi_trips
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """
    ],
    conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

export_analytics = SQLExecuteQueryOperator(
    task_id='export_analytics_layer',
    sql = [
        """
        COPY INTO @raw.snfk_s3_export_nyc_stage/analytics/average_fare_amount/average_fare_amount
        FROM analytics.average_fare_amount
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @raw.snfk_s3_export_nyc_stage/analytics/average_trip_duration/average_trip_duration
        FROM analytics.average_trip_duration
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @raw.snfk_s3_export_nyc_stage/analytics/average_waiting_time_bw_trips/average_waiting_time_bw_trips
        FROM analytics.average_waiting_time_bw_trips
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @raw.snfk_s3_export_nyc_stage/analytics/cash_vs_card_trans/cash_vs_card_trans
        FROM analytics.cash_vs_card_trans
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/common_pickup_locations/common_pickup_locations
        FROM analytics.common_pickup_locations
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/fare_amount_by_borough/fare_amount_by_borough
        FROM analytics.fare_amount_by_borough
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/most_common_payment/most_common_payment
        FROM analytics.most_common_payment
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/most_efficient_route/most_efficient_route
        FROM analytics.most_efficient_route
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/peak_vs_nonpeak_hour/peak_vs_nonpeak_hour
        FROM analytics.peak_vs_nonpeak_hour
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/short_vs_long_trips/short_vs_long_trips
        FROM analytics.short_vs_long_trips
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/total_earnings_per_month/total_earnings_per_month
        FROM analytics.total_earnings_per_month
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/trips_by_time_and_day/trips_by_time_and_day
        FROM analytics.trips_by_time_and_day
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/vendor_market_share/vendor_market_share
        FROM analytics.vendor_market_share
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/peak_customer_demand/peak_customer_demand
        FROM analytics.peak_customer_demand
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """,
        """
        COPY INTO @snfk_s3_export_nyc_stage/analytics/tipping_behaviour/tipping_behaviour
        FROM analytics.TIPPING_BEHAVIOR
        FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
        HEADER = TRUE
        OVERWRITE = TRUE;
        """
    ],
    conn_id=SNOWFLAKE_CONN_ID,
    dag=dag
)

# Task Dependencies
start >> upload_to_s3 >> trigger_snowpipe >> list_stage  >> dbt_run >> dbt_test
dbt_test >> [export_processed, export_conformed_layer, export_analytics]
