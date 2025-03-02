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

    st.write(' Below is the current repo structure:')
    st.code("""
├── Architecture.drawio
├── README.md
├── dags
│   ├── __pycache__
│   │   ├── one_time_export.cpython-312.pyc
│   │   └── pipeline.cpython-312.pyc
│   ├── ec2-airflow.pem
│   ├── one_time_export.py
│   ├── pipeline.py
│   └── utils
│       ├── __init__.py
│       ├── __pycache__
│       └── data_ingestion_to_S3.py
├── data
│   └── raw
│       ├── yellow_tripdata_2022-01.parquet
│       ├── yellow_tripdata_2022-02.parquet
│       ├── yellow_tripdata_2022-03.parquet
│       ├── yellow_tripdata_2022-04.parquet
│       ├── yellow_tripdata_2022-05.parquet
│       ├── yellow_tripdata_2022-06.parquet
│       ├── yellow_tripdata_2022-07.parquet
│       ├── yellow_tripdata_2023-01.parquet
│       ├── yellow_tripdata_2023-02.parquet
│       └── yellow_tripdata_2023-03.parquet
├── dbt_project.yml
├── lib
│   ├── bindings
│   │   └── utils.js
│   ├── tom-select
│   │   ├── tom-select.complete.min.js
│   │   └── tom-select.css
│   └── vis-9.1.2
│       ├── vis-network.css
│       └── vis-network.min.js
├── logs
│   └── dbt.log
├── macros
│   ├── assert_positive_value.sql
│   └── generate_schema_name.sql
├── models
│   ├── analytics
│   │   ├── average_fare_amount.sql
│   │   ├── average_trip_duration.sql
│   │   ├── average_waiting_time_bw_trips.sql
│   │   ├── cash_vs_card_trans.sql
│   │   ├── common_pickup_locations.sql
│   │   ├── fare_amount_by_borough.sql
│   │   ├── most_common_payment.sql
│   │   ├── most_efficient_route.sql
│   │   ├── peak_customer_demand.sql
│   │   ├── peak_vs_nonpeak_hour.sql
│   │   ├── short_vs_long_trips.sql
│   │   ├── tipping_behavior.sql
│   │   ├── total_earnings_per_month.sql
│   │   ├── trips_by_time_and_day.sql
│   │   └── vendor_market_share.sql
│   ├── conformed
│   │   ├── dim_location.sql
│   │   ├── dim_taxi_trips.sql
│   │   ├── dim_time.sql
│   │   ├── fact_taxi_trips.sql
│   │   └── schema.yml
│   ├── processed
│   │   ├── prc_taxi_trips.sql
│   │   └── schema.yml
│   └── source.yml
├── network.html
├── notebooks
│   └── data_dictionary_trip_records_yellow.pdf
├── notes.txt
├── nyc-taxi-venv
│   ├── bin
│   │   ├── Activate.ps1
│   │   ├── __pycache__
│   │   ├── activate
│   │   ├── activate-global-python-argcomplete
│   │   ├── activate.csh
│   │   ├── activate.fish
│   │   ├── airflow
│   │   ├── alembic
│   │   ├── aws
│   │   ├── aws.cmd
│   │   ├── aws_bash_completer
│   │   ├── aws_completer
│   │   ├── aws_zsh_completer.sh
│   │   ├── connexion
│   │   ├── daff
│   │   ├── daff.py
│   │   ├── dask
│   │   ├── dbt
│   │   ├── deep
│   │   ├── dotenv
│   │   ├── email_validator
│   │   ├── f2py
│   │   ├── fabmanager
│   │   ├── faker
│   │   ├── flask
│   │   ├── fonttools
│   │   ├── get_gprof
│   │   ├── get_objgraph
│   │   ├── gunicorn
│   │   ├── httpx
│   │   ├── ipython
│   │   ├── ipython3
│   │   ├── isort
│   │   ├── isort-identify-imports
│   │   ├── jp.py
│   │   ├── jsonpath_ng
│   │   ├── jsonschema
│   │   ├── keyring
│   │   ├── mako-render
│   │   ├── markdown-it
│   │   ├── markdown_py
│   │   ├── normalizer
│   │   ├── nvd3
│   │   ├── pip
│   │   ├── pip3
│   │   ├── pip3.12
│   │   ├── pybabel
│   │   ├── pyftmerge
│   │   ├── pyftsubset
│   │   ├── pygmentize
│   │   ├── pylint
│   │   ├── pylint-config
│   │   ├── pyreverse
│   │   ├── pyrsa-decrypt
│   │   ├── pyrsa-encrypt
│   │   ├── pyrsa-keygen
│   │   ├── pyrsa-priv2pub
│   │   ├── pyrsa-sign
│   │   ├── pyrsa-verify
│   │   ├── python -> python3.12
│   │   ├── python-argcomplete-check-easy-install-script
│   │   ├── python3 -> python3.12
│   │   ├── python3.12 -> /Library/Frameworks/Python.framework/Versions/3.12/bin/python3.12
│   │   ├── register-python-argcomplete
│   │   ├── rst2html.py
│   │   ├── rst2html4.py
│   │   ├── rst2html5.py
│   │   ├── rst2latex.py
│   │   ├── rst2man.py
│   │   ├── rst2odt.py
│   │   ├── rst2odt_prepstyles.py
│   │   ├── rst2pseudoxml.py
│   │   ├── rst2s5.py
│   │   ├── rst2xetex.py
│   │   ├── rst2xml.py
│   │   ├── rstpep2html.py
│   │   ├── slugify
│   │   ├── snowflake-dump-certs
│   │   ├── snowflake-dump-ocsp-response
│   │   ├── snowflake-dump-ocsp-response-cache
│   │   ├── sqlformat
│   │   ├── st-theme
│   │   ├── streamlit
│   │   ├── streamlit.cmd
│   │   ├── symilar
│   │   ├── tabulate
│   │   ├── ttx
│   │   └── undill
│   ├── etc
│   │   └── jupyter
│   ├── include
│   │   └── python3.12
│   ├── lib
│   │   └── python3.12
│   ├── pyvenv.cfg
│   └── share
│       ├── jupyter
│       └── man
├── reports
│   └── nyc-taxi-dbt-dag.png
├── requirements.txt
├── seeds
│   ├── payments.csv
│   ├── rate_codes.csv
│   ├── taxi_zone_lookup.csv
│   └── vendors.csv
├── sql-queries
│   ├── Checks.sql
│   ├── SNFK_INFRA_SETUP.sql
│   └── Unloading.sql
├── streamlit-app
│   ├── Dashboard.py
│   ├── assets
│   │   ├── nyc-data-pipeline.drawio.png
│   │   ├── timo-wagner-fT6-YkB0nfg-unsplash.jpg
│   │   └── vidar-nordli-mathisen-ZYDhBqxJnJ8-unsplash.jpg
│   ├── components
│   ├── pages
│   │   ├── 1_Trip_Patterns.py
│   │   ├── 2_Revenue_Analysis.py
│   │   ├── 3_Customer_Insights.py
│   │   ├── 4_Efficiency_and_Performance.py
│   │   ├── 5_Data_Lineage.py
│   │   ├── 6_Airflow_Automation.py
│   │   └── 7_Project_Details.py
│   └── utils
│       ├── __init__.py
│       ├── __pycache__
│       └── load_view_data.py
├── target
│   ├── catalog.json
│   ├── compiled
│   │   └── nyc_taxi_data_analytics
│   ├── graph.gpickle
│   ├── graph_summary.json
│   ├── index.html
│   ├── manifest.json
│   ├── partial_parse.msgpack
│   ├── run_results.json
│   └── semantic_manifest.json
└── tests
    ├── assert_valid_timestamp.sql
    ├── assert_valid_trip_location.sql
    ├── duplicate_check.sql
    └── fact_vs_dim_count.sql

43 directories, 176 files
            """)

project_details_page()