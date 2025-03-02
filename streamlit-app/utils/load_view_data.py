import os
import io
import boto3
import pandas as pd
import streamlit as st
from datetime import datetime
from dotenv import load_dotenv

load_dotenv('.env')

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
AWS_ACCESS_KEY = os.getenv('AWS_PYTHON_USER_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_PYTHON_USER_SECRET_ACCESS_KEY')

s3_client = boto3.client('s3', aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)


VIEWS = {
    "Average Trip Duration": "analytics/average_trip_duration/",
    "Common Pickup Locations": "analytics/common_pickup_locations/",
    "Trips by Time and Day": "analytics/trips_by_time_and_day/",

    "Average Fare Amount" : "analytics/average_fare_amount/",
    "Fare Amount by Borough": "analytics/fare_amount_by_borough/",
    "Peak vs. Non-Peak Hour Revenue Trends": "analytics/peak_vs_nonpeak_hour/",
    "Total Monthly Earnings Analysis": "analytics/total_earnings_per_month/",

    "Cash vs Card Transactions": "analytics/cash_vs_card_trans/",
    "Most Common Payment Types": "analytics/most_common_payment/",
    "Short vs Long Trips": "analytics/short_vs_long_trips/",
    "Peak Customer Demand by Time & Location": "analytics/peak_customer_demand/",
    "Tipping Behavior" : "analytics/tipping_behaviour/",

    "Average Waiting Time Between Trips": "analytics/average_waiting_time_bw_trips/",
    "Most Efficient Routes": "analytics/most_efficient_route/"
}

@st.cache_data
def fetch_and_merge_csvs(view_name, last_modified=None) -> pd.DataFrame:
    """Fetches the latest CSV file for a given view."""
    print(f"Fetching {view_name}...")
    prefix = VIEWS[view_name]
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    
    if "Contents" not in response or not response["Contents"]:
        return None, last_modified  # No files found
    
    df_list =[]
    latest_modified = last_modified or 0

    for file in response['Contents']:
        file_key = file['Key']
        modified_time = file['LastModified'].timestamp()
        print('file: ', file_key, 'last_modified: ', modified_time)

        if file_key.endswith('.csv'):
            if last_modified is None or modified_time > last_modified:
    
                obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
                df = pd.read_csv(io.BytesIO(obj['Body'].read()))
                df_list.append(df)
                latest_modified = max(latest_modified or 0, modified_time)

    if not df_list:
        print('Empty view!')
        return None, last_modified
    
    merged_df = pd.concat(df_list, ignore_index=True)
    return merged_df, latest_modified

@st.cache_data
def load_data(view_name: str) -> pd.DataFrame:
    df, last_modified = fetch_and_merge_csvs(view_name, st.session_state.last_modified_times.get(view_name))
    if last_modified:
        st.session_state.last_modified_times[view_name] = last_modified
    return df

# Track file modifications
if 'last_modified_times' not in st.session_state:
    st.session_state.last_modified_times = {}

#st.sidebar.dataframe('S3 Update Timestamps:\n', pd.DataFrame( st.session_state.last_modified_times))
