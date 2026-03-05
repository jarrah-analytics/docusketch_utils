import os

import requests
import streamlit as st
import streamlit.components.v1 as components
from google.cloud import storage
import google.auth.transport.requests
import google.oauth2.id_token

# --- CONFIGURATION ---
FUNCTION_URL = os.environ.get("FUNCTION_URL")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
APP_PASSWORD = os.environ.get("APP_PASSWORD")

if not FUNCTION_URL or not BUCKET_NAME or not APP_PASSWORD:
    st.error("Config Error: Missing environment variables.")
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
    places_results = st.number_input(
        "Number of new companies to target",
        min_value=1,
        max_value=200,
        value=30,
        step=1,
        help="The backend will try to return this many new companies, skipping duplicates already in storage.",
    )
    radius = st.number_input(
        "Search Radius (meters)",
        min_value=1000,
        max_value=50000,
        value=8000,
        step=500,
    )

    if st.button("Run Extraction", type="primary"):
        if not zip_code:
            st.warning("Zip code required.")
        else:
            with st.spinner(f"Processing {zip_code}..."):
                try:
                    # Fetch ID token for authenticated Cloud Function call
                    auth_req = google.auth.transport.requests.Request()
                    id_token = google.oauth2.id_token.fetch_id_token(auth_req, FUNCTION_URL)

                    response = requests.post(
                        FUNCTION_URL,
                        json={
                            "zip_code": zip_code.strip(),
                            "places_results": int(places_results),
                            "radius": int(radius),
                        },
                        headers={"Authorization": f"Bearer {id_token}"},
                        timeout=300,
                    )

                    try:
                        data = response.json()
                    except Exception:
                        data = {}

                    if response.status_code == 200:
                        filename = data.get("filename")

                        # Helpful stats from backend
                        rows_saved = data.get("rows_saved", data.get("rows"))
                        rows_skipped_existing = data.get("rows_skipped_existing")
                        places_candidates = data.get("places_candidates")
                        serper_queries_used = data.get("serper_queries_used")

                        stats = {
                            "rows_saved": rows_saved,
                            "rows_skipped_existing": rows_skipped_existing,
                            "places_candidates": places_candidates,
                            "serper_queries_used": serper_queries_used,
                        }

                        # Remove None values before displaying
                        stats = {k: v for k, v in stats.items() if v is not None}

                        if filename:
                            st.success(f"Success! Created: {filename}")

                            if stats:
                                st.write("Run summary:")
                                st.json(stats)

                            # Download new file immediately
                            client = storage.Client()
                            bucket = client.bucket(BUCKET_NAME)
                            blob = bucket.blob(filename)

                            if blob.exists():
                                st.download_button(
                                    label="Download Results Now",
                                    data=blob.download_as_bytes(),
                                    file_name=filename,
                                    mime="text/csv",
                                    key="download_new",
                                )
                            else:
                                st.warning("File was reported as created, but could not be found in the bucket yet.")
                        else:
                            # Success with no file usually means no new rows were found
                            st.info(data.get("message", "No new rows to save."))

                            if stats:
                                st.write("Run summary:")
                                st.json(stats)
                    else:
                        error_message = data.get("message") if isinstance(data, dict) else None
                        if error_message:
                            st.error(f"Error {response.status_code}: {error_message}")
                        else:
                            st.error(f"Error {response.status_code}: {response.text}")

                except Exception as e:
                    st.error(f"Error: {e}")

    st.divider()
    st.subheader("Previous Extractions")

    try:
        client = storage.Client()
        blobs = list(client.list_blobs(BUCKET_NAME))

        # Optional: only show CSV extraction files, hide index files / misc files
        blobs = [b for b in blobs if b.name.lower().endswith(".csv")]

        if not blobs:
            st.info("No history found.")
        else:
            # Sort newest first
            blobs.sort(key=lambda x: x.updated, reverse=True)

            blob_options = {
                f"{b.name} ({b.updated.strftime('%Y-%m-%d %H:%M')})": b
                for b in blobs
            }

            selected = st.selectbox("Select file to download:", options=blob_options.keys())

            if selected:
                blob = blob_options[selected]
                st.download_button(
                    label=f"Download {blob.name}",
                    data=blob.download_as_bytes(),
                    file_name=blob.name,
                    mime="text/csv",
                    key="download_history",
                )
    except Exception as e:
        st.error(f"Could not load history: {e}")

# === TAB 2: WEATHER MAP ===
with tab2:
    st.header("Extreme Weather Tracker")
    st.write("View active weather patterns below to help target zip codes.")

    components.iframe(
        src="https://embed.windy.com/embed2.html?lat=40.0&lon=-95.0&detailLat=40.0&detailLon=-95.0&width=650&height=450&zoom=3&level=surface&overlay=rain&product=ecmwf&menu=&message=true&marker=&calendar=now&pressure=&type=map&location=coordinates&detail=&metricWind=default&metricTemp=default&radarRange=-1",
        height=600,
        scrolling=False,
    )