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

st.title(" :money_with_wings: Revenue & Fare Insights")


### 1: Fare Amount by Borough ###

col1, col2 = st.columns([1,1])
st.subheader(" How does fare amount vary by borough or distance traveled?")

df_average_fare_amount = load_data("Average Fare Amount")
col1.metric(label="Average Fare Amount", value=f"$ {df_average_fare_amount['AVG_FARE_PER_TRIP'][0]}", delta="$ 2.5")
col2.metric(label="Total Trips" , value=df_average_fare_amount['TOTAL_TRIPS'], delta='1M +')

value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_fare_amount = load_data("Fare Amount by Borough")


if df_fare_amount is not None:
    
    sql1= """
    SELECT 
    dl.borough AS pickup_borough,
    dl.zone as pickup_zone,
    ROUND(AVG(ft.fare_amount), 2) AS avg_fare,
    ROUND(AVG(ft.trip_distance), 2) AS avg_distance
    FROM {{ ref('fact_taxi_trips')}}  ft
    JOIN {{ ref('dim_location')}}  dl ON ft.pickup_location_id = dl.location_id
    GROUP BY dl.borough, dl.zone
    ORDER BY avg_fare DESC
    """
    
    with value_chart_tab:
        st.bar_chart(df_fare_amount, x='PICKUP_ZONE', y='AVG_FARE', color="PICKUP_BOROUGH")
    with value_dataframe_tab:
        st.dataframe(df_fare_amount)
    with value_query_tab:
        st.code(sql1)
else:
    st.warning("No data found for Common Pickups and DropOffs.")



#--------------------------------------------------------------

### 2: Peak vs. Non-Peak Hour Revenue Trends ###
st.subheader(" What is the impact of peak vs. non-peak hours on fare revenue?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_peak_vs_nonpeak = load_data("Peak vs. Non-Peak Hour Revenue Trends")
print(df_peak_vs_nonpeak)
if df_peak_vs_nonpeak is not None:
    
    sql2= """
    SELECT 
    CASE 
        WHEN dt.hour BETWEEN 7 AND 9 OR dt.hour BETWEEN 17 AND 19 THEN 'Peak'
        ELSE 'Non-Peak'
    END AS time_period,
    ROUND(AVG(t.fare_amount), 2) AS avg_fare,
    SUM(t.fare_amount) AS total_revenue
    FROM {{ ref('fact_taxi_trips')}} t
    JOIN {{ ref('dim_time')}}  dt ON t.pickup_time_id = dt.time_id
    GROUP BY time_period
    order by total_revenue
    """
    #print(df_peak_vs_nonpeak.dtypes)
    #df_peak_vs_nonpeak["trip_count"] = pd.to_numeric(df_peak_vs_nonpeak["TRIP_COUNT"], errors="coerce")
    
    with value_chart_tab:
        pie_chart ={
            "mark": {"type": "arc", "innerRadius": 50},
            "encoding": {
                "theta": {"field": "AVG_FARE", "type": "quantitative"},
                "color": {"field": "TOTAL_REVENUE", "type": "nominal", "scale": {"scheme": "greens"}}
            }
        }
        st.vega_lite_chart(df_peak_vs_nonpeak, pie_chart, use_container_width=True)
        pass
    with value_dataframe_tab:
        st.dataframe(df_peak_vs_nonpeak)
    with value_query_tab:
        st.code(sql2)
else:
    st.warning("No data found for Common Pickups and DropOffs.")

#--------------------------------------------------------------

### 3: Total Monthly Earnings Analysis ###
st.subheader(" What are the total earnings of taxi drivers per month every year?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_total_earnings = load_data("Total Monthly Earnings Analysis")
if df_total_earnings is not None:
    
    sql3= """
    SELECT 
    dt.year,
    dt.month,
    ROUND(SUM(t.fare_amount), 2) AS total_earnings,
    COUNT(t.trip_id) AS total_trips
    FROM {{ ref('fact_taxi_trips')}} t
    JOIN {{ ref('dim_time')}} dt ON t.pickup_time_id = dt.time_id
    GROUP BY dt.year, dt.month
    ORDER BY dt.year, dt.month
    """
    
    with value_chart_tab:
        tree_map_chart = {
            "mark": "rect",
            "encoding": {
                "x": {"field": "MONTH", "type": "ordinal", "title": "Month"},
                "y": {"field": "YEAR", "type": "ordinal", "title": "Year"},
                "color": {"field": "TOTAL_EARNINGS", "type": "quantitative", "title": "Total Earnings", "scale": {"scheme": "plasma"}},
                "size": {"field": "TOTAL_TRIPS", "type": "quantitative", "title": "Total Trips"}
            },
            "config": {
                "view": {"stroke": "transparent"}
            }
        }
        st.vega_lite_chart(df_total_earnings, tree_map_chart, use_container_width=True)
    with value_dataframe_tab:
        st.dataframe(df_total_earnings)
    with value_query_tab:
        st.code(sql3)
else:
    st.warning("No data found for Average Trip Duration.")