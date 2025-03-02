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

st.title(" :person_in_tuxedo: Customer Insights")


### 1: Peak Customer Demand by Time & Location ###
st.subheader(" What is the peak Customer Demand by Time & Location?")
st.markdown(' Below is a Heat Map, that shows the hot regions where peak demand is visible.')
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])

df_peak_cust_demand = load_data("Peak Customer Demand by Time & Location")
if df_peak_cust_demand is not None:
    
    sql1= """
    SELECT 
        dt.hour as hour_of_day, 
        p.zone as pickup_zone, 
        COUNT(t.trip_id) AS total_trips
    FROM {{ ref('fact_taxi_trips') }} t
    JOIN {{ ref('dim_time') }} dt ON t.pickup_time_id = dt.time_id
    JOIN {{ ref('dim_location') }} p ON t.pickup_location_id = p.location_id
    GROUP BY dt.hour, p.zone
    ORDER BY total_trips DESC
    """
    
    with value_chart_tab:
        heat_map = {
            "mark": "rect",
            "encoding": {
                "x": {"field": "HOUR_OF_DAY", "type": "ordinal", "title": "Hour of Day"},
                "y": {"field": "PICKUP_ZONE", "type": "nominal", "title": "Pickup Location"},
                "color": {"field": "TOTAL_TRIPS", "type": "quantitative", "title": "Total Trips", "scale" : {"scheme": "reds"}}
            }
        }
        st.vega_lite_chart(df_peak_cust_demand, heat_map, use_container_width=True)

    with value_dataframe_tab:
        st.dataframe(df_peak_cust_demand)
    with value_query_tab:
        st.code(sql1)
else:
    st.warning("No data found for Common Pickups and DropOffs.")

#--------------------------------------------------------------

### 2: Tipping Behavior by Payment Method & Time ###
st.subheader(" What is the Tipping Behavior by Payment Method & Time?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])

df_tipping_behavior = load_data("Tipping Behavior")
if df_tipping_behavior is not None:
    
    sql1= """
    SELECT 
        dt.hour_of_day, 
        t.payment_type, 
        ROUND(AVG(t.tip_amount), 2) AS avg_tip
    FROM {{ ref('fact_taxi_trips') }} t
    JOIN {{ ref('dim_time') }} dt ON t.pickup_time_id = dt.time_id
    WHERE t.tip_amount > 0
    GROUP BY dt.hour_of_day, t.payment_type
    ORDER BY avg_tip DESC
    """
    
    with value_chart_tab:
        bubble_chart = {
            "mark": "circle",
            "encoding": {
                "x": {"field": "HOUR_OF_DAY", "type": "ordinal", "title": "Hour of Day"},
                "y": {"field": "AVG_TIP", "type": "quantitative", "title": "Average Tip"},
                "size": {"field": "AVG_TIP", "type": "quantitative", "title": "Average Tip"},
                "color": {"field": "PAYMENT_TYPE", "type": "nominal", "title": "Payment Method", "scale": {"scheme": "magma"}}
            }
        }
        st.vega_lite_chart(df_tipping_behavior, bubble_chart, use_container_width=True)

    with value_dataframe_tab:
        st.dataframe(df_tipping_behavior)
    with value_query_tab:
        st.code(sql1)
else:
    st.warning("No data found for Common Pickups and DropOffs.")



#--------------------------------------------------------------


### 3: Cash vs Card Transactions ###
st.subheader(" How many customers prefer cash vs. card transactions?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])

df_cash_vs_card = load_data("Cash vs Card Transactions")
print(df_cash_vs_card)
if df_cash_vs_card is not None:
    
    sql1= """
    SELECT 
        payment_type ,
        COUNT(ft.trip_id) AS total_trips,
        ROUND(100.0 * COUNT(ft.trip_id) / SUM(COUNT(ft.trip_id)) OVER(), 2) AS percentage
    FROM {{ ref('fact_taxi_trips')}} ft
    GROUP BY payment_type
    ORDER BY total_trips DESC
    """
    
    with value_chart_tab:
        st.bar_chart(df_cash_vs_card, x='PERCENTAGE', y='TOTAL_TRIPS', color="PAYMENT_TYPE")
    with value_dataframe_tab:
        st.dataframe(df_cash_vs_card)
    with value_query_tab:
        st.code(sql1)
else:
    st.warning("No data found for Common Pickups and DropOffs.")



#--------------------------------------------------------------

### 4: Most Common Payment Types ###
st.subheader(" What is the most common payment method?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_payment_methods = load_data("Most Common Payment Types")
print(df_payment_methods)
if df_payment_methods is not None:
    
    sql2= """
    SELECT 
        spt.payment_type as payment_category,
        COUNT(ft.trip_id) AS total_trips,
        ROUND(100.0 * COUNT(ft.trip_id) / SUM(COUNT(ft.trip_id)) OVER(), 2) AS percentage
    FROM {{ ref('fact_taxi_trips')}} ft
    JOIN {{ ref('payments')}} spt 
        ON ft.payment_type = spt.payment_type
    GROUP BY spt.payment_type
    ORDER BY total_trips DESC
    """
    #print(df_peak_vs_nonpeak.dtypes)
    #df_peak_vs_nonpeak["trip_count"] = pd.to_numeric(df_peak_vs_nonpeak["TRIP_COUNT"], errors="coerce")
    
    with value_chart_tab:
        treemap_chart = {
            "mark": "rect",
            "encoding": {
                "x": {"field": "PAYMENT_CATEGORY", "type": "nominal", "title": "Payment Method"},
                "y": {"field": "TOTAL_TRIPS", "type": "quantitative", "title": "Total Transactions"},
                "color": {"field": "PERCENTAGE", "type": "quantitative", "title": "Percentage", "scale": {"scheme": "greens"}},
                "tooltip": [
                    {"field": "PAYMENT_CATEGORY", "type": "nominal", "title": "Payment Method"},
                    {"field": "TOTAL_TRIPS", "type": "quantitative", "title": "Total Trips"},
                    {"field": "PERCENTAGE", "type": "quantitative", "title": "Percentage"}
                ]
            }
        }
        st.vega_lite_chart(df_payment_methods, treemap_chart, use_container_width=True)


        pass
    with value_dataframe_tab:
        st.dataframe(df_payment_methods)
    with value_query_tab:
        st.code(sql2)
else:
    st.warning("No data found for Most Common Payment Types.")

#--------------------------------------------------------------

### 5: Short vs Long Trips ###
st.subheader("  What are the total earnings of taxi drivers per month every year?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_short_vs_long = load_data("Short vs Long Trips")
if df_short_vs_long is not None:
    
    sql3= """
    SELECT 
    CASE 
        WHEN trip_distance < 5 THEN 'Short Trip'
        ELSE 'Long Trip' 
    END AS trip_category,
    COUNT(trip_id) AS total_trips,
    ROUND(100.0 * COUNT(trip_id) / SUM(COUNT(trip_id)) OVER(), 2) AS percentage
    FROM {{ ref('fact_taxi_trips')}}
    GROUP BY trip_category
    ORDER BY total_trips DESC
    """
    
    with value_chart_tab:
        donut_chart = {
            "layer": [{
                "mark": {"type": "arc", "innerRadius": 20, "stroke": "#fff"}
            },{
                "mark": {"type": "text", "radiusOffset": 10},
                "encoding": {
                "text": {"field": "TOTAL_TRIPS", "type": "quantitative"}
                }
            }],
            "encoding": {
                "theta": {"field": "TOTAL_TRIPS", "type": "quantitative", "stack": True},
                "radius": {"field": "TOTAL_TRIPS", "scale": {"type": "sqrt", "zero": True, "rangeMin": 20}},
                "color": {"field": "TRIP_CATEGORY", "type": "nominal", "scale": {"schema": "inferno"}}
            }
        }
        st.vega_lite_chart(df_short_vs_long, donut_chart, use_container_width=True)
    with value_dataframe_tab:
        st.dataframe(df_short_vs_long)
    with value_query_tab:
        st.code(sql3)
else:
    st.warning("No data found for Short vs Long Trips.")