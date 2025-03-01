import streamlit as st

page_bg_img = """
<style>

[data-testid="stSidebar"] {
background-image : url("https://images.unsplash.com/photo-1541336032412-2048a678540d?q=80&w=3087&auto=format&fit=crop&ixlib=rb-4.0.3&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D");
background-size : cover;
}
</style>
"""

st.markdown(page_bg_img, unsafe_allow_html=True)

def project_details_page():
    st.title("📌 Project Details & Deployment")
    st.subheader("🚀 Overview of Data Pipeline")
    
    st.write("### 🔹 Project Objectives")
    st.write("- Automate data ingestion from NYC Taxi data sources into S3")
    st.write("- Transform data using DBT and load into Snowflake")
    st.write("- Orchestrate workflow using Apache Airflow")
    st.write("- Provide an interactive dashboard using Streamlit")
    
    st.write("### 🏗 Tech Stack")
    tech_stack = ["AWS S3", "Snowflake", "DBT", "Apache Airflow", "Python", "Streamlit"]
    st.write(", ".join(tech_stack))
    
    st.write("### 🛠 GitHub Repository & Code Structure")
    st.link_button("Visit GitHub Repo", "https://github.com/Tusharr08/NYC-Taxi-Data-Analytics")
    
    st.write("### 📂 Data Storage & Processing")

    st.write("💾 **Storage Layers:**")
    st.write("- **Raw Data:** Stored in S3 (Parquet format)")
    st.write("- **Processed Data:** Transformed using DBT")
    st.write("- **Analytics Layer:** Aggregated tables for insights")
    
    st.write("### 🔧 Deployment Strategy")
    st.write("1️⃣ Deploy on **Streamlit Cloud** for UI")
    st.write("2️⃣ Use **AWS Lambda + S3 + Snowflake** for data processing")
    st.write("3️⃣ Schedule DAGs in **Apache Airflow**")
    
    if st.button("🔗 View DAGs & Lineage Page"):
        st.switch_page("pages/5_Data_Lineage.py")

project_details_page()