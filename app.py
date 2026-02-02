import streamlit as st
import requests
from google.cloud import storage
import google.auth.transport.requests
import google.oauth2.id_token
import datetime
import os

# --- CONFIG ---
FUNCTION_URL = os.environ.get("FUNCTION_URL")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
APP_PASSWORD = os.environ.get("APP_PASSWORD")

# Fail fast if config is missing
if not FUNCTION_URL or not BUCKET_NAME or not APP_PASSWORD:
    st.error("Configuration Error: Environment variables are missing.")
    st.stop()

# --- AUTHENTICATION (WEBSITE LOGIN) ---
with st.sidebar:
    st.header("Login")
    password = st.text_input("Password", type="password")

if password != APP_PASSWORD:
    st.info("Please enter the password to access the tool.")
    st.stop()

# --- MAIN APP ---
st.title("Professional Extractor Tool")
st.write("Enter a zip code to extract professionals and download the results.")

zip_code = st.text_input("Zip Code", placeholder="H1M 3K9")

if st.button("Run Extraction", type="primary"):
    if not zip_code:
        st.warning("Please enter a zip code.")
    else:
        with st.spinner(f"Processing {zip_code}... (this might take a minute)"):
            try:
                # 1. GET THE AUTH TOKEN (The "ID Card")
                # We fetch an ID token specifically for the target FUNCTION_URL
                auth_req = google.auth.transport.requests.Request()
                id_token = google.oauth2.id_token.fetch_id_token(auth_req, FUNCTION_URL)
                
                # 2. SEND REQUEST WITH TOKEN
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
                        st.success("Extraction complete!")
                        
                        # 3. GENERATE DOWNLOAD LINK
                        client = storage.Client()
                        bucket = client.bucket(BUCKET_NAME)
                        blob = bucket.blob(filename)
                        
                        if blob.exists():
                            url = blob.generate_signed_url(
                                version="v4",
                                expiration=datetime.timedelta(minutes=15),
                                method="GET",
                            )
                            st.link_button("Download Results (CSV)", url)
                        else:
                            st.error(f"Function finished, but file '{filename}' was not found in bucket.")
                    else:
                        st.warning("No filename returned. Check Function logs.")
                        st.write(data) # Debug info
                elif response.status_code == 403:
                    st.error("Error 403: Permission Denied. The website does not have permission to invoke the function.")
                else:
                    st.error(f"Error {response.status_code}: {response.text}")
                    
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")