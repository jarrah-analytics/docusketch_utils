import streamlit as st
from google.cloud import bigquery
import pandas as pd
import os

# --- CONFIGURATION ---
APP_PASSWORD = os.environ.get("APP_PASSWORD", "default_dev_password")
PROJECT_ID = os.environ.get("PROJECT_ID", "your-gcp-project-id") 

# --- LOGIN ---
def check_password():
    if st.session_state.get("password_correct", False):
        return True
    st.sidebar.header("Login")
    password = st.sidebar.text_input("Password", type="password")
    if password == APP_PASSWORD:
        st.session_state["password_correct"] = True
        st.rerun()
    elif password:
        st.sidebar.error("Incorrect password.")
    return False

if not check_password():
    st.stop()

# --- BIGQUERY CONNECTION ---
@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)

@st.cache_data(ttl=3600)
def fetch_data():
    client = get_bq_client()
    query = """
        SELECT *
        FROM `jarrah-freshbooks.marketing_performance.campaign_performance__in_period`
        ORDER BY conversion_date DESC
    """
    return client.query(query).to_dataframe()

# --- MAIN LAYOUT & REPORTING ---
st.title("Campaign Performance Dashboard")

with st.spinner("Fetching data..."):
    try:
        df = fetch_data()
        
        df['conversion_date'] = pd.to_datetime(df['conversion_date'])
        
        # --- FILTERS ---
        st.sidebar.header("Filters")
        campaigns = st.sidebar.multiselect("Filter by Campaign", options=df['campaign'].dropna().unique())
        
        filtered_df = df.copy()
        if campaigns:
            filtered_df = filtered_df[filtered_df['campaign'].isin(campaigns)]

        # --- KPIs ---
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Trials", f"{filtered_df['trials'].sum():,.0f}")
        col2.metric("Total Upgrades", f"{filtered_df['total_upgrades'].sum():,.0f}")
        col3.metric("Total MRR", f"${filtered_df['upgrade_GNMRR'].sum():,.2f}")

        st.divider()

        # --- TABS ---
        tab1, tab2, tab3 = st.tabs(["Trends", "Campaign Breakdown", "Raw Data"])
        
        with tab1:
            st.subheader("Trials & Upgrades Over Time")
            daily_data = filtered_df.groupby('conversion_date')[['trials', 'total_upgrades']].sum().reset_index()
            st.line_chart(daily_data, x='conversion_date', y=['trials', 'total_upgrades'])
            
        with tab2:
            st.subheader("Upgrades by Campaign")
            campaign_data = filtered_df.groupby('campaign')['total_upgrades'].sum().reset_index()
            st.bar_chart(campaign_data, x='campaign', y='total_upgrades')

        with tab3:
            st.subheader("Raw Data")
            st.dataframe(filtered_df, use_container_width=True)
            
    except Exception as e:
        st.error(f"Failed to load dashboard: {e}")