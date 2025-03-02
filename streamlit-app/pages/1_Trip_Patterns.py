import os
import io
import boto3
import pandas as pd
import streamlit as st
from utils.load_view_data import load_data

page_bg_img = """
<style>

[data-testid="stSidebar"] {
background-image : url("https://images.unsplash.com/photo-1541336032412-2048a678540d?q=80&w=3087&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D");
background-size : cover;
}
</style>
"""

st.markdown(page_bg_img, unsafe_allow_html=True)

st.title(" :taxi: Trip Patterns")


### 1: Trips by Time and Day###
st.subheader(" How does the number of trips vary by time of day and day of the week?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_trips_vary = load_data("Trips by Time and Day")
if df_trips_vary is not None:
    
    sql3= """
    SELECT 
    dt.hour as hour,
    dt.day_of_week as day_of_week,
    COUNT(*) AS trip_count
    FROM {{ ref('fact_taxi_trips')}}  t
    JOIN {{ ref('dim_time')}}  dt ON t.pickup_time_id = dt.time_id
    GROUP BY dt.hour, dt.day_of_week
    ORDER BY dt.hour, dt.day_of_week
    """
    
    with value_chart_tab:
        st.line_chart(df_trips_vary, x='HOUR', y='TRIP_COUNT', color="DAY_OF_WEEK")
    with value_dataframe_tab:
        st.dataframe(df_trips_vary)
    with value_query_tab:
        st.code(sql3)
else:
    st.warning("No data found for Common Pickups and DropOffs.")



#--------------------------------------------------------------

### 2: Common Pickup Locations###
st.subheader(" What are the most common pickup & drop-off locations?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_common_pickups = load_data("Common Pickup Locations")
print(df_common_pickups)
if df_common_pickups is not None:
    
    sql2= """
    SELECT 
    dl_pickup.zone AS pickup_location,
    dl_dropoff.zone AS dropoff_location,
    COUNT(ft.trip_id) AS trip_count
    FROM {{ ref('fact_taxi_trips')}} ft
    JOIN {{ ref('dim_location')}} dl_pickup 
        ON ft.pickup_location_id = dl_pickup.location_id
    JOIN {{ ref('dim_location')}} dl_dropoff 
        ON ft.dropoff_location_id = dl_dropoff.location_id
    GROUP BY dl_pickup.zone, dl_dropoff.zone
    ORDER BY trip_count DESC
    limit 20
    """
    print(df_common_pickups.dtypes)
    df_common_pickups["trip_count"] = pd.to_numeric(df_common_pickups["TRIP_COUNT"], errors="coerce")
    
    with value_chart_tab:
        heat_map ={
            "mark": "rect",
            "encoding": {
                "x": {"field": "PICKUP_LOCATION", "type": "nominal", "title": "Pickup Location"},
                "y": {"field": "DROPOFF_LOCATION", "type": "nominal", "title": "Drop-off Location"},
                "color": {
                    "field": "TRIP_COUNT",
                    "type": "quantitative",
                    "title": "Trip Count",
                    "scale" : {
                        "scheme" : "plasma"
                    }
                }
            },
            "config": {
                "view": {
                "stroke": "transparent"
                }
            }
        }
        st.vega_lite_chart(df_common_pickups, heat_map, use_container_width=True)
    with value_dataframe_tab:
        st.dataframe(df_common_pickups)
    with value_query_tab:
        st.code(sql2)
else:
    st.warning("No data found for Common Pickups and DropOffs.")

#--------------------------------------------------------------

### 1️: Average Trip Duration ###
st.subheader(" What is the average trip duration per borough?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_avg_trip_duration = load_data("Average Trip Duration")
if df_avg_trip_duration is not None:
    
    sql1= """
    SELECT 
    dl.borough AS pickup_borough,
    ROUND(AVG(ft.trip_duration_min), 2) AS avg_trip_duration,
    COUNT(ft.trip_id) AS total_trips
    FROM {{ref('fact_taxi_trips')}} ft
    JOIN {{ref('dim_location')}} dl ON ft.pickup_location_id = dl.location_id
    GROUP BY dl.borough
    ORDER BY avg_trip_duration DESC
    """
    
    with value_chart_tab:
        st.bar_chart(df_avg_trip_duration, x='PICKUP_BOROUGH', y='AVG_TRIP_DURATION', color='AVG_TRIP_DURATION')
    with value_dataframe_tab:
        st.dataframe(df_avg_trip_duration)
    with value_query_tab:
        st.code(sql1)
else:
    st.warning("No data found for Average Trip Duration.")