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
    
# DBT DAG Visualization
st.subheader("DBT DAG")
st.write("""
The DBT DAG shows the dependencies between models in the data transformation process. Explore the models, seeds and tests developed for rendering real-time data analytics.
""")

with st.expander('Click to know more about the dbt structure:'):
    st.markdown('The current project has 20 models, 34 data tests, 4 seeds and 1 source(raw table).')
    st.code('dbt seed')
    st.text('Seeds are static table, whose data doesn\'t change often. Above command creates static table in seeds schema.')
    st.code('dbt run')
    st.write('Runs all the exisiting 20 models and creates respective tables/views.')
    st.code('dbt test')
    st.write('Runs all the 34 data tests included in "tests/" folder and respective schema.yml files for each layer.')
    st.code ('dbt docs generate \ndbt docs serve')
    st.write('Below dbt flow DAG is generate through above commands.')
    

st.caption('Go through the models, seeds and tests listed in left pane.')
st.caption('You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models. Click on any node to highlight the lineage.')

st.components.v1.iframe("http://nyc-taxi-data-analytics.s3-website-us-west-2.amazonaws.com/#!/overview", height=800, width = 900)
