import os
import requests
import pandas as pd
import pydeck as pdk
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

# --- AUTH / BACKEND HELPERS ---
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

# --- MAP RENDERING ---
def render_city_grid_map(map_center, cells):
    polygons = []
    labels = []

    for i, cell in enumerate(cells, start=1):
        low_lat = cell["low"]["latitude"]
        high_lat = cell["high"]["latitude"]
        low_lng = cell["low"]["longitude"]
        high_lng = cell["high"]["longitude"]

        polygons.append({
            "cell_number": i,
            "polygon": [
                [low_lng, low_lat],
                [high_lng, low_lat],
                [high_lng, high_lat],
                [low_lng, high_lat],
            ],
        })

        labels.append({
            "cell_number": str(i),
            "lat": (low_lat + high_lat) / 2,
            "lng": (low_lng + high_lng) / 2,
        })

    polygon_layer = pdk.Layer(
        "PolygonLayer",
        data=pd.DataFrame(polygons),
        get_polygon="polygon",
        stroked=True,
        filled=True,
        extruded=False,
        get_fill_color=[0, 0, 255, 30],
        get_line_color=[0, 0, 0, 180],
        line_width_min_pixels=2,
        pickable=True,
    )

    text_layer = pdk.Layer(
        "TextLayer",
        data=pd.DataFrame(labels),
        get_position="[lng, lat]",
        get_text="cell_number",
        get_size=16,
        get_color=[0, 0, 0, 255],
        pickable=False,
    )

    deck = pdk.Deck(
        layers=[polygon_layer, text_layer],
        initial_view_state=pdk.ViewState(
            latitude=map_center["lat"],
            longitude=map_center["lng"],
            zoom=10,
        ),
        tooltip={"text": "Cell {cell_number}"},
    )

    st.pydeck_chart(deck, use_container_width=True)

# --- RESULT RENDERING ---
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
        "coverage_scale": data.get("coverage_scale"),
    }
    stats = {k: v for k, v in stats.items() if v is not None}

    if filename:
        st.success(f"Success! Created: {filename}")
        st.json(stats)

        if data.get("mode") == "city" and data.get("grid_cells") and data.get("map_center"):
            st.write("City grid used for this run:")
            render_city_grid_map(data["map_center"], data["grid_cells"])

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

        if data.get("mode") == "city" and data.get("grid_cells") and data.get("map_center"):
            st.write("City grid:")
            render_city_grid_map(data["map_center"], data["grid_cells"])

# --- APP ---
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

    places_results = st.number_input(
        "Number of results to return (total)",
        min_value=1,
        max_value=200,
        value=30,
        step=1,
        help="Returns up to this many total results.",
    )

    text_query = st.text_input(
        "Search Query",
        value="Water Fire Mold Restoration",
        help="The Places Text Search query sent to Google.",
    )

    use_linkedin = st.checkbox(
        "Find LinkedIn company via Serper (new results only)",
        value=False,
        help="If off, LinkedInCompany will be N/A. If on, Serper runs only for new rows.",
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
                        st.error(
                            f"Error {resp.status_code}: "
                            f"{data.get('message') if isinstance(data, dict) else resp.text}"
                        )

    else:
        city = st.text_input("City", placeholder="Austin")
        col1, col2 = st.columns(2)
        with col1:
            state = st.text_input("State / Province", value="TX")
        with col2:
            country = st.text_input("Country", value="USA")

        coverage_scale = st.number_input(
            "Coverage Scale",
            min_value=0.25,
            max_value=3.0,
            value=1.0,
            step=0.25,
            help="1.0 = default city viewport. Lower searches a tighter area. Higher searches a wider area.",
        )

        index_key = st.text_input(
            "Index Key (optional)",
            placeholder="austin_tx_usa",
            help="Optional shared dedupe key. Leave blank to auto-generate from city/state/country.",
        )

        if st.button("Preview City Grid"):
            if not city:
                st.warning("City required.")
            else:
                with st.spinner(f"Building grid preview for {city}..."):
                    token = fetch_id_token()
                    payload = {
                        "mode": "city",
                        "city": city.strip(),
                        "state": state.strip(),
                        "country": country.strip(),
                        "coverage_scale": float(coverage_scale),
                        "preview_only": True,
                    }

                    if index_key.strip():
                        payload["index_key"] = index_key.strip()

                    resp = call_backend(token, payload)
                    try:
                        data = resp.json()
                    except Exception:
                        data = {}

                    if resp.status_code == 200:
                        if data.get("grid_cells") and data.get("map_center"):
                            st.write("Grid preview (numbered in search order):")
                            render_city_grid_map(data["map_center"], data["grid_cells"])
                        else:
                            st.error("Backend did not return grid data.")
                    else:
                        st.error(
                            f"Error {resp.status_code}: "
                            f"{data.get('message') if isinstance(data, dict) else resp.text}"
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
                        "coverage_scale": float(coverage_scale),
                        "places_results": int(places_results),
                        "text_query": text_query.strip(),
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
                        st.error(
                            f"Error {resp.status_code}: "
                            f"{data.get('message') if isinstance(data, dict) else resp.text}"
                        )

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