import os
import re
from pathlib import Path
from urllib.parse import urlparse

import google.auth.transport.requests
import google.oauth2.id_token
import pandas as pd
import pydeck as pdk
import requests
import streamlit as st
import streamlit.components.v1 as components
from google.cloud import bigquery
from google.cloud import storage

st.set_page_config(page_title="Metro Area Leads - Admin Console", layout="wide")

# --- CONFIGURATION ---
FUNCTION_URL = os.environ.get("FUNCTION_URL")
BUCKET_NAME = os.environ.get("BUCKET_NAME")
APP_PASSWORD = os.environ.get("APP_PASSWORD")
LOCAL_BACKEND_URL = os.environ.get("LOCAL_BACKEND_URL", "").strip()
BQ_PROJECT_ID = os.environ.get("BQ_PROJECT_ID", "ds-data-warehouse")
BQ_DATASET = os.environ.get("BQ_DATASET", "landing__metro_area_leads")
BQ_MASTER_VIEW = os.environ.get("BQ_MASTER_VIEW", "metro_master_current")

APP_ROOT = Path(__file__).resolve().parent.parent
METRO_CSV_PATH = APP_ROOT / "US_Metropolitan_Statistical_Areas - MSA Master List.csv"
LOCAL_OUTPUTS_DIR = APP_ROOT / "local_outputs"
DEFAULT_GCP_CREDENTIALS_PATH = APP_ROOT / "ds-data-warehouse-0b4e47d880af.json"
LINKEDIN_PEOPLE_TABLE = os.environ.get(
    "LINKEDIN_PEOPLE_TABLE", "ds-data-warehouse.landing__brightdata.linkedin_people_snapshot_20260223"
)

STATE_ABBREVIATIONS = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR", "california": "CA",
    "colorado": "CO", "connecticut": "CT", "delaware": "DE", "florida": "FL", "georgia": "GA",
    "hawaii": "HI", "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA", "kansas": "KS",
    "kentucky": "KY", "louisiana": "LA", "maine": "ME", "maryland": "MD", "massachusetts": "MA",
    "michigan": "MI", "minnesota": "MN", "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND", "ohio": "OH", "oklahoma": "OK",
    "oregon": "OR", "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}

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


def is_local_backend(url: str) -> bool:
    host = (urlparse(url).hostname or "").lower()
    return host in {"127.0.0.1", "localhost"}


def get_backend_url() -> str:
    if LOCAL_BACKEND_URL:
        return LOCAL_BACKEND_URL
    return FUNCTION_URL


def call_backend(id_token: str, payload: dict, timeout_s: int = 300):
    backend_url = get_backend_url()
    headers = {}
    if id_token:
        headers["Authorization"] = f"Bearer {id_token}"

    return requests.post(backend_url, json=payload, headers=headers, timeout=timeout_s)


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


def get_credentials_path() -> Path | None:
    configured = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if configured:
        path = Path(configured)
        if path.exists():
            return path
    if DEFAULT_GCP_CREDENTIALS_PATH.exists():
        return DEFAULT_GCP_CREDENTIALS_PATH
    return None


def get_bigquery_client():
    credentials_path = get_credentials_path()
    try:
        if credentials_path:
            return bigquery.Client.from_service_account_json(str(credentials_path), project=BQ_PROJECT_ID)
        return bigquery.Client(project=BQ_PROJECT_ID)
    except Exception:
        return None


@st.cache_data(show_spinner=False)
def load_metro_options():
    if not METRO_CSV_PATH.exists():
        return pd.DataFrame()

    metros = pd.read_csv(METRO_CSV_PATH, dtype=str).fillna("")
    metros["CBSA Code"] = metros["CBSA Code"].str.extract(r"(\d{5})", expand=False).fillna("")
    metros = metros[metros["CBSA Code"] != ""].copy()
    metros["label"] = metros.apply(
        lambda row: f"{row['Metro Area Name']} ({row['CBSA Code']})",
        axis=1,
    )
    metros = metros.sort_values(["Rank", "Metro Area Name"], na_position="last")
    return metros


def is_local_mode() -> bool:
    return is_local_backend(get_backend_url())


@st.cache_data(show_spinner=False)
def load_local_output_inventory():
    if not LOCAL_OUTPUTS_DIR.exists():
        return pd.DataFrame()

    files = []
    for path in LOCAL_OUTPUTS_DIR.glob("*.csv"):
        files.append(
            {
                "name": path.name,
                "path": str(path),
                "modified_at": pd.Timestamp(path.stat().st_mtime, unit="s"),
                "size_kb": round(path.stat().st_size / 1024, 1),
            }
        )
    if not files:
        return pd.DataFrame()
    return pd.DataFrame(files).sort_values("modified_at", ascending=False)


@st.cache_data(show_spinner=False)
def load_local_validation_inventory(prefix: str):
    files = []
    for path in APP_ROOT.glob(f"{prefix}*.json"):
        files.append(
            {
                "name": path.name,
                "path": str(path),
                "modified_at": pd.Timestamp(path.stat().st_mtime, unit="s"),
                "size_kb": round(path.stat().st_size / 1024, 1),
            }
        )
    if not files:
        return pd.DataFrame()
    return pd.DataFrame(files).sort_values("modified_at", ascending=False)


@st.cache_data(show_spinner=False)
def load_local_csv(path: str):
    return pd.read_csv(path)


@st.cache_data(show_spinner=False)
def load_local_json(path: str):
    return pd.read_json(path, typ="series")


@st.cache_data(show_spinner=False)
def load_master_leads(cbsa_code: str):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    sql = f"""
        SELECT
          cbsa_code,
          metro_area_name,
          state_names,
          company_name,
          company_website,
          website_phone,
          street_address,
          latest_search_query,
          latest_run_id,
          latest_source_timestamp_utc
        FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_MASTER_VIEW}`
        WHERE cbsa_code = @cbsa_code
        ORDER BY company_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("cbsa_code", "STRING", cbsa_code)]
    )
    return client.query(sql, job_config=job_config).to_dataframe()


def dataframe_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False).encode("utf-8")


def build_state_regex(state_names: str) -> str:
    tokens = []
    for item in str(state_names or "").split(","):
        name = item.strip()
        if not name:
            continue
        tokens.append(name.lower())
        abbr = STATE_ABBREVIATIONS.get(name.lower())
        if abbr:
            tokens.append(abbr.lower())
    tokens = sorted(set(tokens), key=len, reverse=True)
    if not tokens:
        return ""
    escaped = [re.escape(token).replace(r"\ ", r"\s+") for token in tokens]
    return r"(^|[^a-z])(" + "|".join(escaped) + r")([^a-z]|$)"


@st.cache_data(show_spinner=False)
def load_linkedin_people_matches(cbsa_code: str, state_names: str):
    client = get_bigquery_client()
    if client is None:
        return pd.DataFrame()

    state_regex = build_state_regex(state_names)
    sql = f"""
        WITH master AS (
          SELECT
            cbsa_code,
            metro_area_name,
            state_names,
            company_name,
            LOWER(
              REGEXP_REPLACE(
                REGEXP_REPLACE(company_name, r'[^a-zA-Z0-9]+', ''),
                r'(inc|llc|corp|corporation|ltd|co|company)$',
                ''
              )
            ) AS normalized_company_name
          FROM `{BQ_PROJECT_ID}.{BQ_DATASET}.{BQ_MASTER_VIEW}`
          WHERE cbsa_code = @cbsa_code
        ),
        linkedin_people AS (
          SELECT
            name AS full_name,
            first_name,
            last_name,
            position,
            city,
            location,
            email,
            cellphone_number,
            url AS linkedin_url,
            COALESCE(current_company_name, JSON_VALUE(current_company, '$.name'), current_company) AS linkedin_company_name,
            LOWER(
              REGEXP_REPLACE(
                REGEXP_REPLACE(COALESCE(current_company_name, JSON_VALUE(current_company, '$.name'), current_company, ''), r'[^a-zA-Z0-9]+', ''),
                r'(inc|llc|corp|corporation|ltd|co|company)$',
                ''
              )
            ) AS normalized_current_company_name
          FROM `{LINKEDIN_PEOPLE_TABLE}`
          WHERE @state_regex = ''
             OR REGEXP_CONTAINS(
                  LOWER(CONCAT(COALESCE(city, ''), ' ', COALESCE(location, ''))),
                  @state_regex
                )
        )
        SELECT
          m.company_name,
          p.linkedin_company_name,
          p.full_name,
          p.first_name,
          p.last_name,
          p.position,
          p.city,
          p.location,
          p.email,
          p.cellphone_number,
          p.linkedin_url
        FROM master m
        JOIN linkedin_people p
          ON p.normalized_current_company_name = m.normalized_company_name
        ORDER BY m.company_name, p.full_name
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cbsa_code", "STRING", cbsa_code),
            bigquery.ScalarQueryParameter("state_regex", "STRING", state_regex),
        ]
    )
    return client.query(sql, job_config=job_config).to_dataframe()


def flatten_pdl_people_results(payload: dict, cbsa_code: str):
    if not isinstance(payload, dict):
        return pd.DataFrame()
    if str(payload.get("cbsa_code", "")) != str(cbsa_code):
        return pd.DataFrame()

    rows = []
    for result in payload.get("results", []):
        company_name = result.get("company_name")
        website = result.get("website")
        company_preview = result.get("company_match_preview") or {}
        people_preview = result.get("people_search_preview") or {}
        for person in people_preview.get("data", []) or []:
            rows.append(
                {
                    "company_name": company_name,
                    "company_website": website,
                    "matched_company_name": company_preview.get("display_name") or company_preview.get("name"),
                    "matched_company_id": company_preview.get("id"),
                    "matched_company_size": company_preview.get("size"),
                    "matched_company_industry": company_preview.get("industry"),
                    "full_name": person.get("full_name"),
                    "job_title": person.get("job_title"),
                    "job_company_name": person.get("job_company_name"),
                    "job_company_website": person.get("job_company_website"),
                    "location_country": person.get("location_country"),
                    "linkedin_url": person.get("linkedin_url"),
                }
            )
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def load_pdl_people_matches(cbsa_code: str):
    inventory = load_local_validation_inventory("pdl_")
    if inventory.empty:
        return pd.DataFrame(), None

    for _, row in inventory.iterrows():
        try:
            payload = load_local_json(row["path"]).to_dict()
        except Exception:
            continue
        matches = flatten_pdl_people_results(payload, cbsa_code)
        if not matches.empty:
            return matches, row
    return pd.DataFrame(), None


def parse_city_from_address(address: str) -> str:
    parts = [part.strip() for part in str(address or "").split(",")]
    if len(parts) >= 3:
        return parts[-3]
    return "Unknown"


def render_review_panel():
    metros = load_metro_options()
    if metros.empty:
        st.info("Metro metadata is unavailable.")
        return

    selected_label = st.selectbox(
        "Metro Area",
        options=metros["label"].tolist(),
        key="review_metro",
    )
    selected_metro = metros.loc[metros["label"] == selected_label].iloc[0]

    master_df = load_master_leads(selected_metro["CBSA Code"])
    st.subheader("Master Metro Area Leads List")
    if master_df.empty:
        st.info("No master-list rows found yet for this metro in BigQuery.")
    else:
        top_col1, top_col2, top_col3 = st.columns(3)
        with top_col1:
            st.metric("Master Rows", len(master_df))
        with top_col2:
            sites = int(master_df["company_website"].fillna("").ne("").sum()) if "company_website" in master_df else 0
            st.metric("With Website", sites)
        with top_col3:
            phones = int(master_df["website_phone"].fillna("").ne("").sum()) if "website_phone" in master_df else 0
            st.metric("With Phone", phones)

        st.dataframe(master_df, use_container_width=True, hide_index=True)

        linkedin_df = load_linkedin_people_matches(
            selected_metro["CBSA Code"],
            selected_metro.get("State(s)", ""),
        )
        st.subheader("LinkedIn People Matches")
        st.caption("Best-effort matches from the Bright Data LinkedIn dataset based on company-name matching plus state filtering.")
        if linkedin_df.empty:
            st.info("No LinkedIn people matches found yet for this metro.")
        else:
            match_col1, match_col2, match_col3 = st.columns(3)
            with match_col1:
                st.metric("Matched People", len(linkedin_df))
            with match_col2:
                st.metric("Matched Companies", linkedin_df["company_name"].nunique())
            with match_col3:
                st.metric("With Email", int(linkedin_df["email"].fillna("").ne("").sum()))
            st.dataframe(linkedin_df, use_container_width=True, hide_index=True)

        pdl_df, pdl_file = load_pdl_people_matches(selected_metro["CBSA Code"])
        st.subheader("PDL People Matches")
        st.caption("Local validation results from saved PDL test files. This is not in BigQuery yet.")
        if pdl_df.empty:
            st.info("No PDL people matches found yet for this metro.")
        else:
            pdl_col1, pdl_col2, pdl_col3 = st.columns(3)
            with pdl_col1:
                st.metric("Matched People", len(pdl_df))
            with pdl_col2:
                st.metric("Matched Companies", pdl_df["company_name"].nunique())
            with pdl_col3:
                st.metric("With LinkedIn URL", int(pdl_df["linkedin_url"].fillna("").ne("").sum()))
            if pdl_file is not None:
                st.caption(
                    f"Source file: {pdl_file['name']} | Updated: {pdl_file['modified_at']} | Size: {pdl_file['size_kb']} KB"
                )
            st.dataframe(pdl_df, use_container_width=True, hide_index=True)

def render_debug_panel():
    inventory = load_local_output_inventory()
    metros = load_metro_options()
    if metros.empty:
        st.info("Metro metadata is unavailable.")
        return
    if inventory.empty:
        st.info("No local extraction files yet.")
        return

    selected_label = st.selectbox(
        "Metro Area",
        options=metros["label"].tolist(),
        key="debug_metro",
    )
    selected_metro = metros.loc[metros["label"] == selected_label].iloc[0]

    st.subheader("Selected Run")
    metro_file_options = inventory[inventory["name"].str.contains(selected_metro["CBSA Code"], case=False, regex=False)]
    file_options = metro_file_options if not metro_file_options.empty else inventory

    selected_name = st.selectbox(
        "Review file",
        options=file_options["name"].tolist(),
        key="review_file",
    )
    selected_row = file_options.loc[file_options["name"] == selected_name].iloc[0]
    df = load_local_csv(selected_row["path"])

    st.caption(
        f"Updated: {selected_row['modified_at']} | Size: {selected_row['size_kb']} KB | Rows: {len(df)}"
    )

    if "streetAddress" in df:
        chart_df = (
            df.assign(city=df["streetAddress"].map(parse_city_from_address))
            .groupby("city", as_index=False)
            .size()
            .rename(columns={"size": "rows"})
            .sort_values("rows", ascending=False)
            .head(12)
        )
        st.subheader("City Spread")
        st.bar_chart(chart_df.set_index("city"))
    st.subheader("Run Lead Table")
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_export_panel():
    metros = load_metro_options()
    if metros.empty:
        st.info("Metro metadata is unavailable.")
        return

    st.subheader("Export Master Metro Area Leads List")
    selected_label = st.selectbox(
        "Metro Area to export",
        options=metros["label"].tolist(),
        key="export_metro",
    )
    selected_metro = metros.loc[metros["label"] == selected_label].iloc[0]
    master_df = load_master_leads(selected_metro["CBSA Code"])
    linkedin_df = load_linkedin_people_matches(
        selected_metro["CBSA Code"],
        selected_metro.get("State(s)", ""),
    )

    if master_df.empty:
        st.info("No BigQuery master-list rows found yet for this metro.")
    else:
        export_name = (
            f"master_metro_area_leads_"
            f"{selected_metro['CBSA Code']}_"
            f"{selected_metro['Metro Area Name'].lower().replace(',', '').replace(' ', '_')}.csv"
        )
        st.caption(f"Rows ready: {len(master_df)}")
        st.download_button(
            label=f"Download {export_name}",
            data=dataframe_to_csv_bytes(master_df),
            file_name=export_name,
            mime="text/csv",
            key=f"download_master_{selected_metro['CBSA Code']}",
        )

    st.subheader("Export LinkedIn People Matches")
    if linkedin_df.empty:
        st.info("No LinkedIn people matches found yet for this metro.")
    else:
        linkedin_export_name = (
            f"linkedin_people_matches_"
            f"{selected_metro['CBSA Code']}_"
            f"{selected_metro['Metro Area Name'].lower().replace(',', '').replace(' ', '_')}.csv"
        )
        st.caption(f"Matched people ready: {len(linkedin_df)}")
        st.download_button(
            label=f"Download {linkedin_export_name}",
            data=dataframe_to_csv_bytes(linkedin_df),
            file_name=linkedin_export_name,
            mime="text/csv",
            key=f"download_linkedin_{selected_metro['CBSA Code']}",
        )

    inventory = load_local_output_inventory()
    if inventory.empty:
        return

    st.subheader("Export Selected Run File")
    selected_name = st.selectbox(
        "Export file",
        options=inventory["name"].tolist(),
        key="export_file",
    )
    selected_row = inventory.loc[inventory["name"] == selected_name].iloc[0]
    path = Path(selected_row["path"])

    st.caption(f"Path: {path}")
    st.caption(f"Updated: {selected_row['modified_at']} | Size: {selected_row['size_kb']} KB")

    st.download_button(
        label=f"Download {selected_name}",
        data=path.read_bytes(),
        file_name=selected_name,
        mime="text/csv",
        key=f"download_local_{selected_name}",
    )


# --- MAP RENDERING ---
def render_grid_map(map_center, cells):
    polygons = []
    labels = []

    for idx, cell in enumerate(cells, start=1):
        low_lat = cell["low"]["latitude"]
        high_lat = cell["high"]["latitude"]
        low_lng = cell["low"]["longitude"]
        high_lng = cell["high"]["longitude"]

        polygons.append(
            {
                "cell_number": idx,
                "polygon": [
                    [low_lng, low_lat],
                    [high_lng, low_lat],
                    [high_lng, high_lat],
                    [low_lng, high_lat],
                ],
            }
        )

        labels.append(
            {
                "cell_number": str(idx),
                "lat": (low_lat + high_lat) / 2,
                "lng": (low_lng + high_lng) / 2,
            }
        )

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
            zoom=8,
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
        "full_scan": data.get("full_scan"),
        "index_key": data.get("index_key"),
        "index_blob": data.get("index_blob"),
        "index_saved": data.get("index_saved"),
        "cells_scanned": data.get("cells_scanned"),
        "pages_scanned": data.get("pages_scanned"),
        "places_candidates_scanned": data.get("places_candidates_scanned"),
        "duplicates_blocked_by_index": data.get("duplicates_blocked_by_index"),
        "filtered_out": data.get("filtered_out"),
        "coverage_scale": data.get("coverage_scale"),
        "cbsa_code": data.get("cbsa_code"),
        "metro_area_name": data.get("metro_area_name"),
        "state_names": data.get("state_names"),
        "population_2025": data.get("population_2025"),
    }
    stats = {key: value for key, value in stats.items() if value is not None}

    if filename:
        st.success(f"Run complete: {filename}")
        st.json(stats)

        if data.get("mode") == "metro" and data.get("grid_cells") and data.get("map_center"):
            st.write("Search grid used for this run:")
            render_grid_map(data["map_center"], data["grid_cells"])

        if is_local_mode():
            st.info(f"Saved locally: {data.get('gcs_uri') or filename}")
        else:
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

        if data.get("mode") == "metro" and data.get("grid_cells") and data.get("map_center"):
            st.write("Search grid:")
            render_grid_map(data["map_center"], data["grid_cells"])


def handle_backend_response(payload: dict):
    backend_url = get_backend_url()
    token = "" if is_local_backend(backend_url) else fetch_id_token()
    response = call_backend(token, payload)
    try:
        data = response.json()
    except Exception:
        data = {}

    if response.status_code == 200:
        render_run_result(data)
    else:
        st.error(
            f"Error {response.status_code}: "
            f"{data.get('message') if isinstance(data, dict) else response.text}"
        )


# --- APP ---
st.title("Metro Area Leads - Admin Console")
tab1, tab2 = st.tabs(["Metro Extraction", "Live Weather Map"])

with tab1:
    st.subheader("Build or Refresh a Metro Area Leads List")
    st.caption("Internal admin tool for metro-wide lead harvesting, review, and master-list building.")
    workflow_tab1, workflow_tab2, workflow_tab3, workflow_tab4 = st.tabs(["Run", "Review", "Export", "Debug"])

    with workflow_tab1:
        places_results = st.number_input(
            "Number of results to return (total)",
            min_value=1,
            max_value=200,
            value=30,
            step=1,
            help="Used for capped test runs. Turn on full metro area scan below to ignore this cap.",
        )

        text_query = st.text_input(
            "Search Query",
            value="Water Fire Mold Restoration",
            help="The Places Text Search query sent to Google.",
        )
        st.markdown(
            """
**Estimated API Cost**

Google charges per search request/page, not per lead.

- Each request can return up to about 20 places
- A 30-result run often takes about 2-4 requests: `$0.07-$0.14`
- Broader metro scans can take 20-100+ requests: `$0.70-$3.50+`
"""
        )

        metros = load_metro_options()
        if metros.empty:
            st.error(f"Missing metro CSV at {METRO_CSV_PATH}")
            st.stop()

        selected_label = st.selectbox(
            "Metro Area",
            options=metros["label"].tolist(),
            index=None,
            placeholder="Type to search metro areas",
            key="run_metro",
        )
        if not selected_label:
            st.info("Select a metro area to preview or run extraction.")
        else:
            selected_row = metros.loc[metros["label"] == selected_label].iloc[0]

            st.caption(
                f"States: {selected_row['State(s)']} | Population (2025): {selected_row['Population (2025)']}"
            )

            full_scan = st.checkbox(
                "Full metro area scan",
                value=False,
                help="If checked, the scraper keeps paging across metro cells until the metro search space is exhausted.",
            )

            index_key = st.text_input(
                "Index Key (optional)",
                placeholder=f"{selected_row['CBSA Code']}_metro",
                help="Optional shared dedupe key. Leave blank to auto-generate from CBSA code and metro name.",
            )

            selected_cells = st.text_input(
                "Selected Grid Cells (optional)",
                placeholder="1,2,5",
                help="Optional admin control. Leave blank to scan all kept metro cells, or enter cell numbers like 1 or 1,2,5.",
            )

            preview_col, run_col = st.columns(2)
            with preview_col:
                preview_clicked = st.button("Preview Metro Area Grid")
            with run_col:
                run_clicked = st.button("Run Metro Area Extraction", type="primary")

            if preview_clicked:
                with st.spinner(f"Building metro preview for {selected_row['Metro Area Name']}..."):
                    payload = {
                        "mode": "metro",
                        "cbsa_code": selected_row["CBSA Code"],
                        "metro_area_name": selected_row["Metro Area Name"],
                        "preview_only": True,
                    }
                    if index_key.strip():
                        payload["index_key"] = index_key.strip()
                    if selected_cells.strip():
                        payload["selected_cells"] = selected_cells.strip()
                    handle_backend_response(payload)

            if run_clicked:
                with st.spinner(f"Processing {selected_row['Metro Area Name']}..."):
                    payload = {
                        "mode": "metro",
                        "cbsa_code": selected_row["CBSA Code"],
                        "metro_area_name": selected_row["Metro Area Name"],
                        "places_results": int(places_results),
                        "text_query": text_query.strip(),
                        "full_scan": bool(full_scan),
                    }
                    if index_key.strip():
                        payload["index_key"] = index_key.strip()
                    if selected_cells.strip():
                        payload["selected_cells"] = selected_cells.strip()
                    handle_backend_response(payload)

    with workflow_tab2:
        st.subheader("Review Recent Output")
        render_review_panel()

    with workflow_tab3:
        st.subheader("Export")
        render_export_panel()

    with workflow_tab4:
        st.subheader("Debug")
        if is_local_mode():
            render_debug_panel()
        else:
            st.info("Hosted debug UX can come next. Local debug is available in local mode.")

with tab2:
    st.header("Extreme Weather Tracker")
    st.write("View active weather patterns below to help target areas.")
    components.iframe(
        src="https://embed.windy.com/embed2.html?lat=40.0&lon=-95.0&detailLat=40.0&detailLon=-95.0&width=650&height=450&zoom=3&level=surface&overlay=rain&product=ecmwf&menu=&message=true&marker=&calendar=now&pressure=&type=map&location=coordinates&detail=&metricWind=default&metricTemp=default&radarRange=-1",
        height=600,
        scrolling=False,
    )
