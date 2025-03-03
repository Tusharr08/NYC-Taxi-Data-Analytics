import os
import streamlit as st
from streamlit_extras.let_it_rain import rain 

# Set page config
st.set_page_config(page_title="NYC Taxi Data Dashboard",page_icon=":streamlit:",    layout="wide")

# Custom background for the sidebar
page_bg_img = """
<style>
[data-testid="stSidebar"] {
    background-image: url("https://images.unsplash.com/photo-1541336032412-2048a678540d?q=80&w=3087&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D");
    background-size: cover;
}
</style>
"""
st.markdown(page_bg_img, unsafe_allow_html=True)


# Main content for the Overview page
st.title(":rocket: NYC Yellow Taxi Data Analytics")

col1, col2 = st.columns([3,1])
# Initialize heart counter in session state
if "heart_count" not in st.session_state:
    st.session_state.heart_count = 0

# Display heart button
col1.markdown(f"#### :snowflake: **Total Snowflakes Dropped:** {st.session_state.heart_count}")
if col2.button(":snowflake: Drop a Snowflake"):
    st.session_state.heart_count += 1

    # Trigger heart rain animation
    rain(
        emoji="❄️",
        font_size=54,
        falling_speed=5,
        animation_length=20,
    )

# Display total hearts




# Project Banner
st.image(os.path.join(os.getcwd(), "streamlit-app/assets/vidar-nordli-mathisen-ZYDhBqxJnJ8-unsplash.jpg"), use_container_width=True)

# Title & Subtitle
st.markdown("# 🚖 NYC Taxi Data Analytics Dashboard")
st.markdown("### A comprehensive analysis of NYC taxi trips using DBT, Airflow, and Snowflake")

# Project Objectives
st.header("🎯 Project Objectives")
st.write("""
- **Automate ETL pipelines** for NYC taxi trip data.
- **Ensure data quality** and compliance with governance rules.
- **Provide real-time analytics** on trip patterns, revenue, and efficiency.
- **Optimize infrastructure** using Snowflake and AWS services.
""")

# Technology Stack
st.header("🛠️ Technology Stack")
technologies = [
    "Python ", "Streamlit :streamlit:", "Data Build Tool(dbt) 📊", "Apache Airflow 🌬️", "Snowflake ❄️", "AWS S3 ☁️", "Boto3 📦"
]
cols = st.columns(len(technologies))
for col, tech in zip(cols, technologies):
    col.button(tech)

# Architecture Overview
st.header("🏗️ Architecture Overview")
st.image(os.path.join(os.getcwd(), 'streamlit-app/assets/nyc-data-pipeline.drawio.png') , use_container_width=True)
st.write("""
- **Data Ingestion**: Extract trip data and store it in **AWS S3**.
- **Processing & Transformation**: DBT models clean and transform raw data.
- **Orchestration**: Apache Airflow automates workflows.
- **Storage & Querying**: Snowflake serves as the data warehouse.
- **Dashboarding**: Streamlit provides an interactive UI for insights.
""")

# Call to Action
st.markdown("## 🚀 Explore the Dashboard")
st.markdown("### Check out trip patterns, customer insights, and revenue analysis in detailed reports!")
if st.button("Get Started"):
    st.switch_page("pages/1_Trip_Patterns.py")