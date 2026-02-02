import streamlit as st
import requests
from google.cloud import storage
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

# --- AUTHENTICATION ---
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
                response = requests.post(FUNCTION_URL, json={"zip_code": zip_code})
                
                if response.status_code == 200:
                    data = response.json()
                    # Expecting: {"message": "...", "filename": "..."}
                    filename = data.get("filename")

                    if filename:
                        st.success("Extraction complete!")
                        
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
                            st.error(f"File '{filename}' not found in bucket.")
                    else:
                        st.warning("No filename returned. Check Function logs.")
                        st.write(data)
                else:
                    st.error(f"Error {response.status_code}: {response.text}")
            except Exception as e:
                st.error(f"An error occurred: {str(e)}")