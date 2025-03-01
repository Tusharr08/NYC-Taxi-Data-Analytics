import os
import io
import boto3
import pandas as pd
import streamlit as st
import networkx as nx
from pyvis.network import Network

page_bg_img = """
<style>

[data-testid="stSidebar"] {
background-image : url("https://images.unsplash.com/photo-1541336032412-2048a678540d?q=80&w=3087&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D");
background-size : cover;
}
</style>
"""

st.markdown(page_bg_img, unsafe_allow_html=True)

st.title(" :gear: Efficiency & Performance")

BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

s3_client = boto3.client('s3')


VIEWS = {
    "Average Waiting Time Between Trips": "analytics/average_waiting_time_bw_trips/",
    "Most Efficient Routes": "analytics/most_efficient_route/"
}

def fetch_and_merge_csvs(view_name):
    """Fetches the latest CSV file for a given view."""
    print(f"Fetching {view_name}...")
    prefix = VIEWS[view_name]
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    
    if "Contents" not in response:
        return None  # No files found
    
    df_list =[]

    for file in response['Contents']:
        file_key = file['Key']
        if file_key.endswith('.csv'):
            print('file: ', file_key)
            obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
            df = pd.read_csv(io.BytesIO(obj['Body'].read()))
            df_list.append(df)

    if not df_list:
        print('Empty view!')
        return None
    
    merged_df = pd.concat(df_list, ignore_index=True)
    return merged_df


### 1: Average Waiting Time Between Trips ###
st.subheader(" What is the average waiting time between trips for drivers?")
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_waiting_time = fetch_and_merge_csvs("Average Waiting Time Between Trips")
if df_waiting_time is not None:
    
    sql1= """
    WITH trip_gaps AS (
    SELECT 
        TRIP_ID,
        PICKUP_DATETIME, 
        DROPOFF_DATETIME,
        hour_of_day,
        LAG(DROPOFF_DATETIME) OVER (ORDER BY PICKUP_DATETIME) AS prev_trip_end
        FROM {{ ref('fact_taxi_trips')}}
    )
    SELECT 
        HOUR_OF_DAY,
        abs(ROUND(AVG(DATEDIFF(MINUTE, prev_trip_end, PICKUP_DATETIME)), 2)) AS avg_waiting_time_mins
    FROM trip_gaps
    WHERE prev_trip_end IS NOT NULL
    GROUP BY HOUR_OF_DAY
    ORDER BY HOUR_OF_DAY
    """
    
    with value_chart_tab:
        scatter_plot = {
            "mark": "point",
            "encoding": {
                "x": {"field": "HOUR_OF_DAY", "type": "quantitative"},
                "y": {"field": "AVG_WAITING_TIME_MINS", "type": "quantitative"},
                "size": {"field": "AVG_WAITING_TIME_MINS", "type": "quantitative"},
                "color": {"field": "AVG_WAITING_TIME_MINS", "type": "quantitative", "scale": {"scheme": "purples"}}
            }
        }
        with st.spinner("Loading chart..."):
            st.vega_lite_chart(df_waiting_time, scatter_plot, use_container_width=True)
    with value_dataframe_tab:
        st.dataframe(df_waiting_time)
    with value_query_tab:
        st.code(sql1)
else:
    st.warning("No data found for Common Pickups and DropOffs.")



#--------------------------------------------------------------

### 2: Most Efficient Routes ###
st.subheader(" What is the most efficient route between common pickup and drop-off points?")
st.markdown("Below is a network of locations, hover over each path and node to see it's details.")
st.write('Thicker the Path, More is the efficient and common route.')
value_chart_tab, value_dataframe_tab, value_query_tab = st.tabs([
        "Chart",
        "Raw Data",
        "SQL Query"
    ])
df_efficient_route = fetch_and_merge_csvs("Most Efficient Routes")
print(df_efficient_route)
if df_efficient_route is not None:
    
    sql2= """
    Select * from (
    SELECT 
        l1.zone AS pickup_zone,
        l2.zone AS dropoff_zone,
        ROUND(AVG(trip_duration_min), 2) AS avg_trip_duration_mins, 
        COUNT(*) AS trip_count
    FROM {{ ref('fact_taxi_trips')}} t
    JOIN {{ ref('dim_location')}} l1 ON t.pickup_location_id = l1.location_id
    JOIN {{ ref('dim_location')}} l2 ON t.dropoff_location_id = l2.location_id
    GROUP BY l1.zone, l2.zone
    ORDER BY trip_count DESC
    ) s where dropoff_zone is not null and pickup_zone is not null and dropoff_zone!='N/A' and pickup_zone!='N/A'
    """
    #print(df_peak_vs_nonpeak.dtypes)
    #df_peak_vs_nonpeak["trip_count"] = pd.to_numeric(df_peak_vs_nonpeak["TRIP_COUNT"], errors="coerce")
    
    with value_chart_tab:
        G = nx.DiGraph()
        df_top20 = df_efficient_route.nlargest(20, "TRIP_COUNT")  

        for _, row in df_top20.iterrows():
            G.add_edge(row["PICKUP_ZONE"], row["DROPOFF_ZONE"], weight=row["TRIP_COUNT"], duration=row["AVG_TRIP_DURATION_MINS"])

        # Visualize with Pyvis
        net = Network(height="500px", width="100%", bgcolor="#222222", font_color="white", directed=True)

        for node in G.nodes():
            net.add_node(node, label=node, title=node)

        for edge in G.edges(data=True):
            net.add_edge(edge[0], edge[1], title=f"Trips: {edge[2]['weight']} | Duration: {edge[2]['duration']} mins", width=edge[2]['weight']/5000)

        net.save_graph("network.html")

        # Display in Streamlit
        with st.spinner("Loading chart..."):
            st.components.v1.html(open("network.html", "r").read(), height=500)
    with value_dataframe_tab:
        st.dataframe(df_efficient_route)
    with value_query_tab:
        st.code(sql2)
else:
    st.warning("No data found for Common Pickups and DropOffs.")

#--------------------------------------------------------------
