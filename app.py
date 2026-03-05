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

def fetch_id_token() -> str:
    auth_req = google.auth.transport.requests.Request()
    return google.oauth2.id_token.fetch_id_token(auth_req, FUNCTION_URL)

def call_backend(id_token: str, payload: dict, timeout_s: int = 300):
    return requests.post(
        FUNCTION_URL,
        json=payload,
        headers={"Authorization": f"Bearer {id_token}"},
        timeout=timeout_s,
    )

def download_blob_bytes(bucket_name: str, blob_name: str):
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if blob.exists():
            return blob.download_as_bytes()
    except Exception:
        return None
    return None

def render_run_result(data: dict):
    filename = data.get("filename")
    message = data.get("message")
    mode = data.get("mode")

    stats = {
        "mode": mode,
        "rows_total_returned": data.get("rows_total_returned"),
        "rows_new_added_to_index": data.get("rows_new_added_to_index"),
        "rows_existing_returned": data.get("rows_existing_returned"),
        "results_total_requested": data.get("results_total_requested"),
        "use_linkedin": data.get("use_linkedin"),
        "index_key": data.get("index_key"),
        "index_blob": data.get("index_blob"),
        "index_saved": data.get("index_saved"),
        "cells_scanned": data.get("cells_scanned"),
        "pages_scanned": data.get("pages_scanned"),
        "places_candidates_scanned": data.get("places_candidates_scanned"),
        "duplicates_blocked_by_index": data.get("duplicates_blocked_by_index"),
        "filtered_out": data.get("filtered_out"),
        "serper_queries_used": data.get("serper_queries_used"),
        "grid_rows": data.get("grid_rows"),
        "grid_cols": data.get("grid_cols"),
    }
    stats = {k: v for k, v in stats.items() if v is not None}

    if filename:
        st.success(f"Success! Created: {filename}")
        st.json(stats)

        file_bytes = download_blob_bytes(BUCKET_NAME, filename)
        if file_bytes:
            st.download_button(
                label="Download Results Now",
                data=file_bytes,
                file_name=filename,
                mime="text/csv",
                key=f"download_{filename}",
            )
        else:
            st.warning("File was reported as created, but could not be found in the bucket yet.")
    else:
        st.info(message or "No results.")
        if stats:
            st.json(stats)

st.title("Professional Extractor Tool")
tab1, tab2 = st.tabs(["Extraction Tool", "Live Weather Map"])

with tab1:
    st.subheader("Run New Extraction")

    mode = st.radio(
        "Search Mode",
        options=["zip", "city"],
        horizontal=True,
        format_func=lambda x: "Zip Mode" if x == "zip" else "City Mode",
    )

    # NOTE: this is now "total results to return", not "new"
    places_results = st.number_input(
        "Number of results to return (total)",
        min_value=1,
        max_value=200,
        value=30,
        step=1,
        help="Returns up to this many total results (mix of existing + new). Only new ones are added to the index.",
    )

    text_query = st.text_input(
        "Search Query",
        value="Water Fire Mold Restoration",
        help="The Places Text Search query sent to Google.",
    )

    max_pages_per_cell = st.number_input(
        "Max pages per search cell",
        min_value=1,
        max_value=5,
        value=3,
        step=1,
        help="Each page can return up to 20 Places results.",
    )

    use_linkedin = st.checkbox(
        "Find LinkedIn company via Serper (new results only)",
        value=True,
        help="If off, LinkedInCompany will be N/A. If on, Serper runs only for new (not-in-index) rows.",
    )

    if mode == "zip":
        zip_code = st.text_input("Enter Zip / Postal Code", placeholder="78701 or H1M 3K9")
        radius = st.number_input(
            "Search Radius (meters)",
            min_value=1000,
            max_value=50000,
            value=8000,
            step=500,
        )

        if st.button("Run Zip Extraction", type="primary"):
            if not zip_code:
                st.warning("Zip/postal code required.")
            else:
                with st.spinner(f"Processing {zip_code}..."):
                    token = fetch_id_token()
                    payload = {
                        "mode": "zip",
                        "zip_code": zip_code.strip(),
                        "radius": int(radius),
                        "places_results": int(places_results),
                        "text_query": text_query.strip(),
                        "max_pages_per_cell": int(max_pages_per_cell),
                        "use_linkedin": bool(use_linkedin),
                    }
                    resp = call_backend(token, payload)
                    try:
                        data = resp.json()
                    except Exception:
                        data = {}

                    if resp.status_code == 200:
                        render_run_result(data)
                    else:
                        st.error(f"Error {resp.status_code}: {data.get('message') if isinstance(data, dict) else resp.text}")

    else:
        city = st.text_input("City", placeholder="Austin")
        col1, col2 = st.columns(2)
        with col1:
            state = st.text_input("State / Province", value="TX")
        with col2:
            country = st.text_input("Country", value="USA")

        col3, col4 = st.columns(2)
        with col3:
            grid_rows = st.number_input("Grid Rows", min_value=1, max_value=6, value=3, step=1)
        with col4:
            grid_cols = st.number_input("Grid Columns", min_value=1, max_value=6, value=3, step=1)

        index_key = st.text_input(
            "Index Key (optional)",
            placeholder="austin_tx_usa",
            help="Optional shared dedupe key. Leave blank to auto-generate from city/state/country.",
        )

        if st.button("Run City Extraction", type="primary"):
            if not city:
                st.warning("City required.")
            else:
                with st.spinner(f"Processing {city}..."):
                    token = fetch_id_token()
                    payload = {
                        "mode": "city",
                        "city": city.strip(),
                        "state": state.strip(),
                        "country": country.strip(),
                        "grid_rows": int(grid_rows),
                        "grid_cols": int(grid_cols),
                        "places_results": int(places_results),
                        "text_query": text_query.strip(),
                        "max_pages_per_cell": int(max_pages_per_cell),
                        "use_linkedin": bool(use_linkedin),
                    }
                    if index_key.strip():
                        payload["index_key"] = index_key.strip()

                    resp = call_backend(token, payload)
                    try:
                        data = resp.json()
                    except Exception:
                        data = {}

                    if resp.status_code == 200:
                        render_run_result(data)
                    else:
                        st.error(f"Error {resp.status_code}: {data.get('message') if isinstance(data, dict) else resp.text}")

    st.divider()
    st.subheader("Previous Extractions")

    try:
        client = storage.Client()
        blobs = list(client.list_blobs(BUCKET_NAME))
        blobs = [b for b in blobs if b.name.lower().endswith(".csv")]
        if not blobs:
            st.info("No history found.")
        else:
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
                    key="download_history",
                )
    except Exception as e:
        st.error(f"Could not load history: {e}")

with tab2:
    st.header("Extreme Weather Tracker")
    st.write("View active weather patterns below to help target areas.")
    components.iframe(
        src="https://embed.windy.com/embed2.html?lat=40.0&lon=-95.0&detailLat=40.0&detailLon=-95.0&width=650&height=450&zoom=3&level=surface&overlay=rain&product=ecmwf&menu=&message=true&marker=&calendar=now&pressure=&type=map&location=coordinates&detail=&metricWind=default&metricTemp=default&radarRange=-1",
        height=600,
        scrolling=False,
    )