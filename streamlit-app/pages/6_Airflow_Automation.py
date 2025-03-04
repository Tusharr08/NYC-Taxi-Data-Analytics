import os
import streamlit as st
import networkx as nx
import matplotlib.pyplot as  plt
import streamlit.components.v1 as components

page_bg_img = """
<style>

[data-testid="stSidebar"] {
background-image : url("https://images.unsplash.com/photo-1541336032412-2048a678540d?q=80&w=3087&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D");
background-size : cover;
}
</style>
"""

st.markdown(page_bg_img, unsafe_allow_html=True)

st.title(" :airplane: Airflow Automation")
    
# DBT DAG Visualization
st.subheader("Airflow DAG")
st.write("""
The Apache Airflow automates data pipelines using DAGs (Directed Acyclic Graphs).
Instead of manually running scripts, Airflow schedules and executes tasks.
""")

with st.expander('Click to know more about Airflow DAGs used:'):
    st.write('This project uses 2 Airflow DAGs:')
    st.code('1: nyc_snfk_one_time_export_s3')
    st.write('Above DAG exports the seeds to s3://nyc-taxi-data-analytics/seeds. It is a one time export so this dag is developed separately.')
    st.code('2: nyc_taxi_etl')
    st.write('This DAG manages complete ETL pipeline from uploading to s3 to exporting views to s3 analytics layer.')

st.caption('Go through the DAGs present in below Airflow Webserver screenshot.')
st.caption('For now, airflow is not hosted anywhere but you can find the working code in git repo.')

st.image(os.path.join(os.getcwd(), 'streamlit-app/assets/airflow-dag.png') , use_container_width=True)

if st.link_button(label='Click to get viewer access on Airflow!', url='http://streamer:streamer@ec2-35-90-57-50.us-west-2.compute.amazonaws.com:8080/home'):
    st.toast('username: streamer', icon='🤵')
    st.toast('password: streamer', icon='💼')