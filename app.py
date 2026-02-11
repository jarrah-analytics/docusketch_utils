import streamlit as st
import streamlit.components.v1 as components
import requests
from google.cloud import storage
import google.auth.transport.requests
import google.oauth2.id_token
import os

# --- CONFIGURATION ---
FUNCTION_URL = os.environ.get("FUNCTION_URL")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
APP_PASSWORD = os.environ.get("APP_PASSWORD")

if not FUNCTION_URL or not BUCKET_NAME or not APP_PASSWORD:
    st.error("Config Error: Missing Environment Variables.")
    st.stop()

# --- LOGIN ---
with st.sidebar:
    st.header("Login")
    if st.text_input("Password", type="password") != APP_PASSWORD:
        st.info("Enter password to access.")
        st.stop()

# --- MAIN LAYOUT ---
st.title("Professional Extractor Tool")

tab1, tab2 = st.tabs(["Extraction Tool", "Live Weather Map"])

# === TAB 1: EXTRACTION & HISTORY ===
with tab1:
    st.subheader("Run New Extraction")
    zip_code = st.text_input("Enter Zip Code", placeholder="H1M 3K9")

    if st.button("Run Extraction", type="primary"):
        if not zip_code:
            st.warning("Zip code required.")
        else:
            with st.spinner(f"Processing {zip_code}..."):
                try:
                    # Auth & Request
                    auth_req = google.auth.transport.requests.Request()
                    id_token = google.oauth2.id_token.fetch_id_token(auth_req, FUNCTION_URL)
                    
                    response = requests.post(
                        FUNCTION_URL, 
                        json={"zip_code": zip_code}, 
                        headers={"Authorization": f"Bearer {id_token}"}
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        filename = data.get("filename")
                        
                        if filename:
                            st.success(f"Success! Created: {filename}")
                            
                            # Download
                            client = storage.Client()
                            bucket = client.bucket(BUCKET_NAME)
                            blob = bucket.blob(filename)
                            
                            if blob.exists():
                                st.download_button(
                                    label="Download Results Now",
                                    data=blob.download_as_bytes(),
                                    file_name=filename,
                                    mime="text/csv",
                                    key="download_new"
                                )
                        else:
                            st.warning("No filename returned.")
                    else:
                        st.error(f"Error {response.status_code}: {response.text}")
                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()
    st.subheader("Previous Extractions")

    try:
        client = storage.Client()
        blobs = list(client.list_blobs(BUCKET_NAME))
        
        if not blobs:
            st.info("No history found.")
        else:
            # Sort newest first
            blobs.sort(key=lambda x: x.updated, reverse=True)
            
            blob_options = {f"{b.name} ({b.updated.strftime('%Y-%m-%d %H:%M')})": b for b in blobs}
            
            selected = st.selectbox("Select file to download:", options=blob_options.keys())
            
            if selected:
                blob = blob_options[selected]
                st.download_button(
                    label=f"Download {blob.name}",
                    data=blob.download_as_bytes(),
                    file_name=blob.name,
                    mime="text/csv",
                    key="download_history"
                )
    except Exception as e:
        st.error(f"Could not load history: {e}")

# === TAB 2: WEATHER MAP ===
with tab2:
    st.header("Extreme Weather Tracker")
    st.write("View active weather patterns below to help target zip codes.")
    
    # Windy.com Embed (Rain overlay)
    components.iframe(
        src="https://embed.windy.com/embed2.html?lat=40.0&lon=-95.0&detailLat=40.0&detailLon=-95.0&width=650&height=450&zoom=3&level=surface&overlay=rain&product=ecmwf&menu=&message=true&marker=&calendar=now&pressure=&type=map&location=coordinates&detail=&metricWind=default&metricTemp=default&radarRange=-1",
        height=600,
        scrolling=False
    )
