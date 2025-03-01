from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'tushar',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

SNOWFLAKE_CONN_ID= 'snowflake_default'

dag = DAG('nyc_snfk_one_time_export_s3', default_args=default_args, schedule_interval=None, catchup=False)

start = BashOperator(
    task_id="start",
    bash_command="echo 'Starting export process...'",
    dag=dag,
)


EXPORT_SQL = [
    """
    COPY INTO @raw.snfk_s3_export_nyc_stage/seeds/taxi_zone_lookup.csv
    FROM seeds.taxi_zone_lookup
    FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
    SINGLE = TRUE HEADER = TRUE;
    """,
    """
    COPY INTO @raw.snfk_s3_export_nyc_stage/seeds/payment_types.csv
    FROM seeds.payments
    FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
    SINGLE = TRUE HEADER = TRUE;
    """,
    """
    COPY INTO @raw.snfk_s3_export_nyc_stage/seeds/rate_codes.csv
    FROM seeds.rate_codes
    FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
    SINGLE = TRUE HEADER = TRUE;
    """,
    """
    COPY INTO @raw.snfk_s3_export_nyc_stage/seeds/vendors.csv
    FROM seeds.vendors
    FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
    SINGLE = TRUE HEADER = TRUE;
    """,
    """
    COPY INTO @raw.snfk_s3_export_nyc_stage/analytics/dim_location/dim_location.csv
    FROM analytics.dim_location
    FILE_FORMAT = ( FORMAT_NAME = raw.NYC_UNLOAD_CSV_FORMAT )
    SINGLE = TRUE HEADER = TRUE;
    """
]

# Task 4: Export staging data one time to s3 folders.
export_tasks = [
    SQLExecuteQueryOperator(
        task_id=f"export_{i}",
        sql=query,
        conn_id=SNOWFLAKE_CONN_ID,
        dag=dag,
    )
    for i, query in enumerate(EXPORT_SQL)
]


start >> export_tasks  
