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

st.title(" :pushpin: Interactive Data Exploration")

def data_exploration_page():
    
    # DBT DAG Visualization
    st.subheader("DBT DAG")
    st.write("""
    The DBT DAG shows the dependencies between models in the data transformation process. Explore the models, seeds and tests developed for rendering real-time data analytics.
    """)

    st.caption('You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.')
    
    st.components.v1.iframe("http://nyc-taxi-data-analytics.s3-website-us-west-2.amazonaws.com/#!/overview", height=800, width = 900)


data_exploration_page()