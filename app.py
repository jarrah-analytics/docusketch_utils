import streamlit as st
import requests
from google.cloud import storage
import google.auth.transport.requests
import google.oauth2.id_token
import os

# --- CONFIG ---
FUNCTION_URL = os.environ.get("FUNCTION_URL")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
APP_PASSWORD = os.environ.get("APP_PASSWORD")

if not FUNCTION_URL or not BUCKET_NAME or not APP_PASSWORD:
    st.error("Configuration Error: Environment variables are missing.")
    st.stop()

# --- AUTHENTICATION ---
with st.sidebar:
    st.header("Login")
    password = st.text_input("Password", type="password")

if password != APP_PASSWORD:
    st.info("Please enter the password to access the tool.")
    st.stop()

# --- MAIN APP ---
st.title("Professional Extractor Tool")

# ----------------- PART 1: RUN NEW EXTRACTION -----------------
st.subheader("Run New Extraction")
zip_code = st.text_input("Enter Zip Code", placeholder="H1M 3K9")

if st.button("Run Extraction", type="primary"):
    if not zip_code:
        st.warning("Please enter a zip code.")
    else:
        with st.spinner(f"Processing {zip_code}... (this might take a minute)"):
            try:
                # 1. Get Auth Token
                auth_req = google.auth.transport.requests.Request()
                id_token = google.oauth2.id_token.fetch_id_token(auth_req, FUNCTION_URL)
                
                # 2. Call Function
                headers = {"Authorization": f"Bearer {id_token}"}
                response = requests.post(
                    FUNCTION_URL, 
                    json={"zip_code": zip_code}, 
                    headers=headers
                )
                
                if response.status_code == 200:
                    data = response.json()
                    filename = data.get("filename")

                    if filename:
                        st.success(f"Extraction complete: {filename}")
                        
                        # Direct download for the just-finished run
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
                elif response.status_code == 403:
                    st.error("Error 403: Permission Denied.")
                else:
                    st.error(f"Error {response.status_code}: {response.text}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")

# ----------------- PART 2: DOWNLOAD HISTORY -----------------
st.divider()
st.subheader("Previous Extractions")

try:
    # 1. List files in bucket
    client = storage.Client()
    blobs = list(client.list_blobs(BUCKET_NAME))
    
    if not blobs:
        st.info("No previous extractions found.")
    else:
        # Sort by time (newest first)
        blobs.sort(key=lambda x: x.updated, reverse=True)
        
        # Create a nice list of names for the dropdown
        # key is the friendly name, value is the blob object
        blob_options = {f"{b.name} ({b.updated.strftime('%Y-%m-%d %H:%M')})": b for b in blobs}
        
        selected_option = st.selectbox(
            "Select a file to download:",
            options=blob_options.keys()
        )
        
        # 2. Download Selected File Logic
        if selected_option:
            selected_blob = blob_options[selected_option]
            
            # We download bytes into memory only when selected to save bandwidth
            file_bytes = selected_blob.download_as_bytes()
            
            st.download_button(
                label=f"Download {selected_blob.name}",
                data=file_bytes,
                file_name=selected_blob.name,
                mime="text/csv",
                key="download_history" # Unique key is required
            )

except Exception as e:
    st.error(f"Could not load history: {e}")
