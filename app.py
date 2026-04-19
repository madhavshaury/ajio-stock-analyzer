import streamlit as st
import pandas as pd
import json
import re
import requests
import time
import hashlib
import io
from dotenv import load_dotenv
import os

from erpnext_client import (
    diagnose_erpnext_api,
    get_bin_stock_for_item_warehouse,
    get_open_pos_for_item_warehouse,
    normalize_base_url,
)

# Load environment variables if present
load_dotenv()

st.set_page_config(page_title="Order Search & ERP Analyzer", layout="wide")

st.title("Order Search & ERP Analyzer")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    .rounded-box {
        background-color: #f1f3f6;
        padding: 20px;
        border-radius: 10px;
        border: 1px solid #dcdfe3;
        margin-bottom: 20px;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
</style>
""", unsafe_allow_html=True)

# --- SIDEBAR CONFIG ---
st.sidebar.header("Configuration")

# Redash Config
# Check environment variables for defaults
env_redash_url = os.getenv("REDASH_URL", "https://redash.agilitas.digital")
env_redash_api_key = os.getenv("REDASH_API_KEY", "")
env_data_source_id = os.getenv("REDASH_DATA_SOURCE_ID", "")

# Fix URL if protocol is missing
if env_redash_url and not env_redash_url.startswith("http"):
    env_redash_url = f"https://{env_redash_url}"

with st.sidebar.expander("Redash Credentials", expanded=not env_redash_api_key):
    redash_url = st.text_input("Redash URL", value=env_redash_url)
    redash_api_key = st.text_input("Redash API Key", value=env_redash_api_key, type="password")
    
    # Ensure typed URL also has protocol
    if redash_url and not redash_url.startswith("http"):
        redash_url = f"https://{redash_url}"
    
    # Helper to fetch Data Sources
    def get_data_sources(url, api_key):
        try:
            headers = {"Authorization": f"Key {api_key}"}
            # Increased timeout to 10s for slow VPN
            resp = requests.get(f"{url.rstrip('/')}/api/data_sources", headers=headers, timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            return None

    data_sources = []
    if redash_url and redash_api_key:
        data_sources = get_data_sources(redash_url, redash_api_key)
    
    if data_sources:
        ds_options = {f"{ds['name']} (ID: {ds['id']})": str(ds['id']) for ds in data_sources}
        # Find index of current env_data_source_id if it exists (by ID or by name)
        default_index = 0
        if env_data_source_id:
            for i, (label, val) in enumerate(ds_options.items()):
                if val == str(env_data_source_id) or str(env_data_source_id).lower() in label.lower():
                    default_index = i
                    break
        
        selected_ds_name = st.selectbox("Select Data Source", options=list(ds_options.keys()), index=default_index)
        data_source_id = ds_options[selected_ds_name]
    else:
        # Fallback if fetching fails or no API key yet
        data_source_id = st.text_input("Data Source ID (Manual)", value=env_data_source_id if env_data_source_id.isdigit() else "1")
        if redash_api_key:
            st.info(
                "Could not fetch Data Sources. Ensure you are connected to the **VPN (Pritunl)**."
            )

    if not redash_api_key:
        st.warning("API Key is required for searching.")

# ERPNext (Phase 2) — env defaults for sidebar (site root; strip /app — see normalize_base_url)
env_erpnext_url = normalize_base_url(os.getenv("ERPNEXT_URL", ""))
env_erpnext_api_key = os.getenv("ERPNEXT_API_KEY", "")
env_erpnext_api_secret = os.getenv("ERPNEXT_API_SECRET", "")
env_erpnext_test_po = os.getenv("ERPNEXT_TEST_PO", "")

with st.sidebar.expander("ERPNext Credentials", expanded=False):
    erpnext_url = st.text_input(
        "ERPNext URL (site root, no /app)",
        value=env_erpnext_url,
        placeholder="https://your-site.com",
        help="Use the site base URL. Desk /app URLs break the API; use /api/... on the site root.",
    )
    erpnext_api_key = st.text_input("ERPNext API Key", value=env_erpnext_api_key, type="password")
    erpnext_api_secret = st.text_input("ERPNext API Secret", value=env_erpnext_api_secret, type="password")
    erpnext_test_po = st.text_input(
        "Optional: PO name for API test (single-doc GET)",
        value=env_erpnext_test_po,
        placeholder="PO-M27-02-000114",
        help="Same as curl /api/resource/Purchase Order/<name>. Token auth only; cookies not required.",
    )
    enrich_erpnext_po = st.checkbox(
        "Enrich with ERPNext (live stock qty + open POs)", value=True
    )
    if erpnext_url and not erpnext_url.startswith("http"):
        erpnext_url = f"https://{erpnext_url}"
    erpnext_url = normalize_base_url(erpnext_url)

    if st.button("Test ERPNext API (why 404?)", use_container_width=True):
        with st.spinner("Calling /api/method/ping and resource endpoints..."):
            st.session_state["_erpnext_diag"] = diagnose_erpnext_api(
                erpnext_url,
                erpnext_api_key,
                erpnext_api_secret,
                test_po_name=(erpnext_test_po or "").strip() or None,
            )
    diag = st.session_state.get("_erpnext_diag")
    if diag:
        st.caption(diag.get("hint", ""))
        st.json(diag)

# --- HELPERS ---

# Failure taxonomy (human-readable in CSV / UI)
FAILURE_NEGATIVE_STOCK = "Negative Stock"
FAILURE_ZERO_VALUATION = "Zero Valuation Error"
FAILURE_INTERNAL_SERVER = "Internal Server Error"
FAILURE_OTHER = "Other"


def _is_negative_stock_text(t: str) -> bool:
    """t must already be lowercased."""
    if any(
        k in t
        for k in (
            "negative stock",
            "insufficient stock",
            "not enough stock",
            "insufficient qty",
            "qty not available",
            "quantity not available",
            "stock not available",
            "no stock",
            "out of stock",
        )
    ):
        return True
    if "stock" in t and any(
        x in t for x in ("negative", "not available", "insufficient", "shortage")
    ):
        return True
    return False


def classify_failure_category(text: str) -> str:
    """
    Classify failed ERP responses into one of three known types (order matters).
    1) Internal Server Error  2) Zero Valuation Error  3) Negative Stock  4) Other
    """
    if not text or not str(text).strip():
        return FAILURE_OTHER
    t = str(text).lower()

    if any(
        k in t
        for k in (
            "internal server error",
            "internal error",
            "http 500",
            "http 502",
            "http 503",
            "http 504",
            "status 500",
            "status 502",
            "status 503",
            "errcode 500",
            "bad gateway",
            "gateway timeout",
            "service unavailable",
        )
    ):
        return FAILURE_INTERNAL_SERVER

    if any(
        k in t
        for k in (
            "zero valuation",
            "valuation error",
            "valuation rate",
            "valuationrate",
            "valuation rate not found",
            "valuation rate is mandatory",
            "cannot make stock entry",
            "stock value is zero",
        )
    ):
        return FAILURE_ZERO_VALUATION

    if _is_negative_stock_text(t):
        return FAILURE_NEGATIVE_STOCK

    return FAILURE_OTHER


def _failure_text_for_row(row, df, erp_error, erp_response):
    """Full text blob for failure classification."""
    parts = []
    for x in (erp_error, erp_response):
        if pd.notna(x) and x is not None:
            parts.append(str(x))
    for col in df.columns:
        val = row.get(col)
        if pd.notna(val) and val is not None:
            parts.append(str(val))
    return " ".join(parts)

def run_redash_query(url, api_key, ds_id, query_text):
    """Executes a query on Redash and polls for results."""
    api_url = f"{url.rstrip('/')}/api/query_results"
    headers = {"Authorization": f"Key {api_key}"}
    payload = {
        "data_source_id": int(ds_id),
        "query": query_text,
        "max_age": 0
    }
    
    try:
        response = requests.post(api_url, json=payload, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        # If result is already there
        if "query_result" in data:
            return pd.DataFrame(data["query_result"]["data"]["rows"])
        
        # If it's a job, poll for it
        if "job" in data:
            job_id = data["job"]["id"]
            poll_url = f"{url.rstrip('/')}/api/jobs/{job_id}"
            
            job_progress = st.status("Querying Redash...")
            for _ in range(30): # 30 attempts, 1s each
                time.sleep(1)
                try:
                    job_resp = requests.get(poll_url, headers=headers)
                    job_data = job_resp.json()
                    
                    status_text = {1: "Pending", 2: "Started", 3: "Success", 4: "Failure"}.get(job_data["job"]["status"], "Processing")
                    job_progress.update(
                        label=f"Querying Redash... Status: {status_text}", state="running"
                    )

                    if job_data["job"]["status"] == 3: # SUCCESS
                        result_id = job_data["job"]["query_result_id"]
                        res_url = f"{url.rstrip('/')}/api/query_results/{result_id}"
                        res_resp = requests.get(res_url, headers=headers)
                        res_data = res_resp.json()
                        job_progress.update(label="Query complete.", state="complete")
                        return pd.DataFrame(res_data["query_result"]["data"]["rows"])
                    elif job_data["job"]["status"] == 4: # FAILED
                        job_progress.update(
                            label=f"Query failed: {job_data['job'].get('error', 'Internal Error')}",
                            state="error",
                        )
                        return None
                except requests.exceptions.RequestException as poll_error:
                    job_progress.update(
                        label=f"Polling interrupted: {str(poll_error)}", state="error"
                    )
                    return None
    except requests.exceptions.ConnectionError:
        st.error(
            "Redash connection failed. Ensure you are connected to the **Pritunl VPN** and try again."
        )
        return None
    except requests.exceptions.Timeout:
        st.error("Redash connection timeout. Check if your VPN connection is stable.")
        return None
    except Exception as e:
        st.error(f"Error connecting to Redash: {str(e)}")
        return None
    return None

def extract_article_warehouse(text):
    if not text or pd.isna(text):
        return None, None
    text_str = str(text)
    
    # Article: Look for "Article L..." or "channelSkuCode=L..." or "Article [Code]"
    # Added support for longer codes and varied prefixes
    article_match = re.search(r'Article\s+([A-Z0-9]+)', text_str, re.I) or \
                    re.search(r'channelSkuCode=([A-Z0-9]+)', text_str, re.I) or \
                    re.search(r'(L\d{5,})', text_str)
    
    # Warehouse: Look for "Warehouse [Name]" (stop at </a>, <br>, comma or quote)
    warehouse_match = re.search(r'Warehouse\s+([^<,\"\n\r]+)', text_str, re.I)
    
    article = article_match.group(1) if article_match else None
    warehouse = warehouse_match.group(1).strip() if warehouse_match else None
    
    # Clean up warehouse name if it ends with unwanted chars
    if warehouse:
        warehouse = re.split(r'[:\\]', warehouse)[0].strip()
        
    return article, warehouse

def perform_analysis(df, input_codes=None):
    """Common logic to analyze logs from either Excel or Redash."""
    results = []
    
    # Priority-based column mapping
    def get_col(candidates, default):
        for cand in candidates:
            # Check case-insensitive
            match = next((c for c in df.columns if c.lower() == cand), None)
            if match: return match
        return default

    col_map = {
        "payload": get_col(["payload", "request_payload"], "payload"),
        "status": get_col(["erp_status", "status", "api_status", "db_status"], "erp_status"),
        "error": get_col(["erp_error", "error", "error_message", "db_error", "db_response"], "erp_error"),
        "response": get_col(["erp_response", "response", "db_response"], "erp_response"),
    }

    for _, row in df.iterrows():
        payload = row.get(col_map["payload"])
        erp_error = row.get(col_map["error"])
        erp_response = row.get(col_map["response"])
        
        # Status logic: Handle Redash 'None' status
        raw_status = row.get(col_map["status"])
        erp_status = str(raw_status).upper() if pd.notna(raw_status) else "SUCCESS"
        
        # STRICT FAILURE CHECK as requested by user
        is_failure = erp_status in ["FAILED", "FAILURE"]

        if pd.isna(payload) and pd.isna(raw_status) and pd.isna(erp_response):
            continue

        try:
            data = json.loads(payload) if isinstance(payload, str) else (payload if isinstance(payload, dict) else {})
        except:
            data = {}

        # Look for parentOrderCode
        p_code = data.get("parentOrderCode") or row.get("parentOrderCode") or row.get("parent_order_code") or row.get("correlation_id")
        parent_code = str(p_code) if pd.notna(p_code) else ""
        
        # If missing, check in response string (Redash DTO format)
        if (not parent_code or len(parent_code) < 5) and erp_response:
            code_match = re.search(r'parentOrderCode=([^,\s\)]+)', str(erp_response), re.IGNORECASE)
            if code_match:
                parent_code = code_match.group(1)

        if not is_failure:
            results.append({
                "parentOrderCode": parent_code,
                "erp_status": "SUCCESS",
                "erp_response": erp_response,
                "article_code": None,
                "warehouse": None,
                "erp_error": None,
                "failure_category": "",
                "po_lookup_eligible": False,
            })
        else:
            # Extraction logic - ONLY FOR FAILURES
            article, warehouse = None, None
            # Scan error and response columns primarily
            for source in [erp_error, erp_response]:
                if not article or not warehouse:
                    a, w = extract_article_warehouse(source)
                    article = article or a
                    warehouse = warehouse or w
            
            # Final fallback scan of the whole row
            if not article or not warehouse:
                for col in df.columns:
                    val = row.get(col)
                    if not article or not warehouse:
                        a, w = extract_article_warehouse(val)
                        article = article or a
                        warehouse = warehouse or w

            fail_text = _failure_text_for_row(row, df, erp_error, erp_response)
            category = classify_failure_category(fail_text)
            eligible = category == FAILURE_NEGATIVE_STOCK and bool(article) and bool(warehouse)

            results.append({
                "parentOrderCode": parent_code,
                "erp_status": "FAILED",
                "article_code": article,
                "warehouse": warehouse,
                "erp_error": erp_error or erp_response,
                "erp_response": None,
                "failure_category": category,
                "po_lookup_eligible": eligible,
            })
    
    return pd.DataFrame(results)


_PRESET_CORE_KEYS = (
    "parentOrderCode",
    "erp_status",
    "article_code",
    "warehouse",
    "failure_category",
    "po_lookup_eligible",
)


def _visible_columns_from_preset(preset: str, all_cols: list[str]) -> list[str]:
    """Map preset label to an ordered subset of columns (all must exist in all_cols)."""
    ac = list(all_cols)
    erp_like = [
        c
        for c in ac
        if c.startswith(("erp_", "open_po")) or c == "erpnext_lookup_error"
    ]
    if preset == "All columns":
        return ac
    if preset == "Core":
        out = [c for c in _PRESET_CORE_KEYS if c in ac]
        return out or ac
    if preset == "Core + ERP":
        seen: list[str] = []
        for c in _PRESET_CORE_KEYS:
            if c in ac and c not in seen:
                seen.append(c)
        for c in erp_like:
            if c not in seen:
                seen.append(c)
        for c in ac:
            if c not in seen:
                seen.append(c)
        return seen or ac
    return ac


def _arrow_safe_enrichment_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Streamlit serializes dataframes with PyArrow; mixed str + int in the same column fails.
    Normalize ERPNext enrichment columns to nullable numeric dtypes.
    """
    for c in ("open_po_count", "matching_po_count"):
        if c in df.columns:
            s = df[c].replace("", pd.NA)
            df[c] = pd.to_numeric(s, errors="coerce").astype("Int64")
    for c in (
        "erp_actual_qty",
        "erp_reserved_qty",
        "erp_projected_qty",
        "erp_valuation_rate",
        "erp_ordered_qty",
    ):
        if c in df.columns:
            s = df[c].replace("", pd.NA)
            df[c] = pd.to_numeric(s, errors="coerce").astype("Float64")
    return df


def enrich_analysis_with_erpnext_po(
    result_df: pd.DataFrame,
    base_url: str,
    api_key: str,
    api_secret: str,
    enabled: bool,
    *,
    timeout: int = 30,
) -> pd.DataFrame:
    """Add Bin stock qty (failed rows with SKU+WH) and open PO columns (negative stock only)."""
    out = result_df.copy()
    po_cols = [
        "open_po_count",
        "open_po_names",
        "open_po_statuses",
        "open_po_detail",
        "matching_po_count",
        "open_po_diagnostic",
        "erpnext_lookup_error",
    ]
    stock_cols = [
        "erp_actual_qty",
        "erp_reserved_qty",
        "erp_projected_qty",
        "erp_valuation_rate",
        "erp_ordered_qty",
        "erp_bin_found",
        "erp_bin_never_inwarded",
        "erp_bin_health_code",
        "erp_zero_valuation_risk",
        "erp_strict_stock_val_ok",
        "erp_stock_error",
    ]
    for c in po_cols + stock_cols:
        out[c] = ""

    if out.empty or not enabled:
        return _arrow_safe_enrichment_dtypes(out)

    bu = normalize_base_url(base_url)
    if not bu or not (api_key or "").strip() or not (api_secret or "").strip():
        return _arrow_safe_enrichment_dtypes(out)

    # Bump key when ERPNext fetch logic changes so old error results are not reused.
    if "erpnext_pair_cache_v7" not in st.session_state:
        st.session_state["erpnext_pair_cache_v7"] = {}

    cache: dict = st.session_state["erpnext_pair_cache_v7"]

    def bucket(key: tuple[str, str]) -> dict:
        if key not in cache:
            cache[key] = {}
        return cache[key]

    def fmt_num(v) -> str:
        if v is None:
            return ""
        try:
            return str(float(v))
        except (TypeError, ValueError):
            return str(v)

    def fmt_yes_no(v) -> str:
        return "yes" if v else "no"

    # Live stock: any FAILED row with article + warehouse
    if "erp_status" in out.columns:
        fail_mask = out["erp_status"].astype(str).str.upper().isin(["FAILED", "FAILURE"])
        wh_mask = (
            out["article_code"].notna()
            & out["warehouse"].notna()
            & (out["article_code"].astype(str).str.strip() != "")
            & (out["warehouse"].astype(str).str.strip() != "")
        )
        stock_idxs = out.index[fail_mask & wh_mask].tolist()
    else:
        stock_idxs = []

    for i in stock_idxs:
        item = str(out.at[i, "article_code"]).strip()
        wh = str(out.at[i, "warehouse"]).strip()
        key = (item, wh)
        b = bucket(key)
        if "stock" not in b:
            b["stock"] = get_bin_stock_for_item_warehouse(
                bu, api_key, api_secret, item, wh, timeout=timeout
            )
        sres = b["stock"]
        err = sres.get("error")
        out.at[i, "erp_actual_qty"] = fmt_num(sres.get("actual_qty"))
        out.at[i, "erp_reserved_qty"] = fmt_num(sres.get("reserved_qty"))
        out.at[i, "erp_projected_qty"] = fmt_num(sres.get("projected_qty"))
        out.at[i, "erp_valuation_rate"] = fmt_num(sres.get("valuation_rate"))
        out.at[i, "erp_ordered_qty"] = fmt_num(sres.get("ordered_qty"))
        out.at[i, "erp_bin_found"] = "yes" if sres.get("bin_found") else "no"
        out.at[i, "erp_bin_never_inwarded"] = fmt_yes_no(bool(sres.get("bin_never_inwarded")))
        out.at[i, "erp_bin_health_code"] = str(sres.get("bin_health_code") or "")
        out.at[i, "erp_zero_valuation_risk"] = fmt_yes_no(bool(sres.get("zero_valuation_risk")))
        out.at[i, "erp_strict_stock_val_ok"] = fmt_yes_no(
            bool(sres.get("strict_stock_and_valuation_ok"))
        )
        if err:
            out.at[i, "erp_stock_error"] = err

    # Open POs: negative-stock failures only
    if "po_lookup_eligible" not in out.columns:
        return _arrow_safe_enrichment_dtypes(out)

    mask = out["po_lookup_eligible"].fillna(False).astype(bool)
    idxs = out.index[mask].tolist()
    for i in idxs:
        item = out.at[i, "article_code"]
        wh = out.at[i, "warehouse"]
        if pd.isna(item) or pd.isna(wh) or not str(item).strip() or not str(wh).strip():
            continue
        key = (str(item).strip(), str(wh).strip())
        b = bucket(key)
        if "po" not in b:
            b["po"] = get_open_pos_for_item_warehouse(
                bu, api_key, api_secret, key[0], key[1], timeout=timeout
            )
        res = b["po"]
        err = res.get("error")
        out.at[i, "open_po_count"] = res.get("open_po_count", 0)
        out.at[i, "open_po_names"] = res.get("open_po_names", "") or ""
        out.at[i, "open_po_statuses"] = res.get("open_po_statuses", "") or ""
        out.at[i, "open_po_detail"] = res.get("open_po_detail", "") or ""
        out.at[i, "matching_po_count"] = res.get("matching_po_count", 0)
        out.at[i, "open_po_diagnostic"] = str(res.get("open_po_diagnostic") or "")
        if err:
            out.at[i, "erpnext_lookup_error"] = err

    return _arrow_safe_enrichment_dtypes(out)

def display_analysis_results(
    result_df,
    label="Results",
    *,
    enrich_enabled: bool = False,
    erpnext_has_credentials: bool = False,
):
    """Displays the dataframe and insights for processed results."""
    if not result_df.empty:
        st.success(f"Analyzed {len(result_df)} records.")
        if not enrich_enabled:
            st.info(
                "ERPNext stock and PO columns are empty until you enable "
                "**Enrich with ERPNext (live stock qty + open POs)** in the sidebar."
            )
        elif enrich_enabled and not erpnext_has_credentials:
            st.warning(
                "ERPNext enrichment needs **URL**, **API Key**, and **API Secret** in the sidebar."
            )

        all_cols = list(result_df.columns)
        col_layout = st.selectbox(
            "Visible columns",
            [
                "All columns",
                "Core",
                "Core + ERP",
                "Custom (choose in expander below)",
            ],
            key=f"col_layout_choice_{label}",
            help="Does not re-fetch data; analysis stays in session until you run a new search or change filters.",
        )

        if col_layout == "Custom (choose in expander below)":
            with st.expander("Choose columns to show", expanded=True):
                vis_key = f"col_custom_multi_{label}"
                visible_cols = st.multiselect(
                    "Columns included in the table and CSV download",
                    options=all_cols,
                    default=all_cols,
                    key=vis_key,
                )
        else:
            visible_cols = _visible_columns_from_preset(col_layout, all_cols)

        if not visible_cols:
            visible_cols = all_cols
        display_df = result_df[visible_cols]
        st.dataframe(display_df, width="stretch")

        csv = display_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label=f"Download {label} CSV",
            data=csv,
            file_name=f"{label.lower().replace(' ', '_')}_{int(time.time())}.csv",
            mime="text/csv",
        )

        st.subheader("Analysis Insights")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Top Failing SKUs")
            failing_rows = result_df[result_df["erp_status"] == "FAILED"]
            if not failing_rows.empty:
                failing_skus = failing_rows["article_code"].value_counts().reset_index()
                failing_skus.columns = ["article_code", "count"]
                st.dataframe(failing_skus, width='stretch')
            else:
                st.info("No failures found.")
        with col2:
            st.markdown("#### Status Distribution")
            st.dataframe(result_df["erp_status"].value_counts().reset_index(), width='stretch')
        if "failure_category" in result_df.columns:
            st.markdown("#### Failure category (failed rows)")
            failed_only = result_df[result_df["erp_status"] == "FAILED"]
            if not failed_only.empty:
                st.dataframe(
                    failed_only["failure_category"].value_counts().reset_index(),
                    width="stretch",
                )
            else:
                st.info("No failed rows in this result set.")
    else:
        st.warning("No data matched the analysis criteria. Check if the 'status' or 'error' columns are available.")

# --- MAIN UI ---

SESSION_REDASH_ANALYSIS = "cached_redash_analyzed_df"
SESSION_REDASH_RAW = "cached_redash_raw_df"
SESSION_EXCEL_SIG = "cached_excel_analysis_sig"
SESSION_EXCEL_DF = "cached_excel_analysis_df"

tab_redash, tab_erp = st.tabs(["Redash Search (Manual)", "ERP Analyzer (Excel)"])

# -----------------
# TAB 1: REDASH SEARCH (PRIMARY)
# -----------------
with tab_redash:
    st.header("Search Orders via Redash")
    st.markdown("Enter `parentOrderCode`s manually to fetch details directly from the `api_event_log`.")

    with st.container():
        user_input = st.text_area("Paste parentOrderCode(s) here (comma or newline separated)", 
                                placeholder="Example: 165000, 1100, abcd",
                                height=150)
        
        col_btn, col_info = st.columns([1, 4])
        
        with col_btn:
            run_query = st.button("Search in Redash", use_container_width=True, type="primary")
        
        with col_info:
            if user_input:
                codes = [x.strip() for x in re.split(r'[,\n]', user_input) if x.strip()]
                st.write(f"Detected **{len(codes)}** codes.")

    if run_query:
        if not redash_api_key:
            st.error("Please enter your Redash API Key in the sidebar.")
        elif not user_input:
            st.warning("Please enter at least one parentOrderCode.")
        else:
            codes = [x.strip() for x in re.split(r'[,\n]', user_input) if x.strip()]
            
            # Wrap in double quotes as requested: "165000", "1100"
            formatted_codes = ", ".join([f'"{c}"' for c in codes])
            
            sql_query = f"""select * from api_event_log 
where correlation_id in (
    select order_id from outward_orders 
    where parent_order_code in ({formatted_codes})
) 
and event_type = "OUTWARD_ORDER" 
order by timestamp desc;"""

            with st.expander("View SQL Query"):
                st.code(sql_query, language="sql")

            results_df = run_redash_query(redash_url, redash_api_key, data_source_id, sql_query)

            if results_df is not None:
                if not results_df.empty:
                    analyzed_res = perform_analysis(results_df, input_codes=codes)
                    analyzed_res = enrich_analysis_with_erpnext_po(
                        analyzed_res,
                        erpnext_url,
                        erpnext_api_key,
                        erpnext_api_secret,
                        enrich_erpnext_po,
                    )
                    st.session_state[SESSION_REDASH_ANALYSIS] = analyzed_res
                    st.session_state[SESSION_REDASH_RAW] = results_df
                else:
                    st.warning("No records found for these order codes in Redash.")
                    st.session_state.pop(SESSION_REDASH_ANALYSIS, None)
                    st.session_state.pop(SESSION_REDASH_RAW, None)

    cached_redash = st.session_state.get(SESSION_REDASH_ANALYSIS)
    if cached_redash is not None and isinstance(cached_redash, pd.DataFrame) and not cached_redash.empty:
        st.divider()
        st.subheader("Automated Analysis for Redash results")
        erpnext_ok_cached = bool(
            normalize_base_url(erpnext_url)
            and (erpnext_api_key or "").strip()
            and (erpnext_api_secret or "").strip()
        )
        display_analysis_results(
            cached_redash,
            label="Redash Analysis",
            enrich_enabled=enrich_erpnext_po,
            erpnext_has_credentials=erpnext_ok_cached,
        )
        st.divider()
        st.subheader("Raw Data (Redash)")
        raw_df = st.session_state.get(SESSION_REDASH_RAW)
        if raw_df is not None:
            st.dataframe(raw_df, width="stretch")

# -----------------
# TAB 2: ERP ANALYZER (OPTIONAL EXCEL)
# -----------------
with tab_erp:
    st.header("ERP Negative Stock Analyzer")
    st.markdown("Upload an Excel file to extract SKUs and Warehouses from failure logs.")
    
    uploaded_file = st.file_uploader("Upload ERP Log File (.xlsx)", type=["xlsx"], key="erp_uploader")
    
    if uploaded_file:
        file_bytes = uploaded_file.getvalue()
        file_fingerprint = hashlib.sha256(file_bytes).hexdigest()

        col1, col2 = st.columns(2)
        with col1:
            filter_input = st.text_area("Filter Codes (comma-sep)", key="filter_erp")
        with col2:
            exclude_input = st.text_area("Exclude Codes (comma-sep)", key="exclude_erp")

        filter_codes = {x.strip() for x in filter_input.split(",") if x.strip()}
        exclude_codes = {x.strip() for x in exclude_input.split(",") if x.strip()}

        bu_excel = normalize_base_url(erpnext_url)
        erpnext_ok = bool(
            bu_excel and (erpnext_api_key or "").strip() and (erpnext_api_secret or "").strip()
        )
        excel_sig = (
            file_fingerprint,
            filter_input,
            exclude_input,
            enrich_erpnext_po,
            bu_excel,
            bool((erpnext_api_key or "").strip()),
            bool((erpnext_api_secret or "").strip()),
        )
        need_rebuild = (
            st.session_state.get(SESSION_EXCEL_SIG) != excel_sig
            or st.session_state.get(SESSION_EXCEL_DF) is None
        )
        if need_rebuild:
            df_raw = pd.read_excel(io.BytesIO(file_bytes))
            analyzed_excel = perform_analysis(df_raw)
            if filter_codes:
                analyzed_excel = analyzed_excel[
                    analyzed_excel["parentOrderCode"].isin(filter_codes)
                ]
            if exclude_codes:
                analyzed_excel = analyzed_excel[
                    ~analyzed_excel["parentOrderCode"].isin(exclude_codes)
                ]
            analyzed_excel = enrich_analysis_with_erpnext_po(
                analyzed_excel,
                erpnext_url,
                erpnext_api_key,
                erpnext_api_secret,
                enrich_erpnext_po,
            )
            st.session_state[SESSION_EXCEL_SIG] = excel_sig
            st.session_state[SESSION_EXCEL_DF] = analyzed_excel
        else:
            analyzed_excel = st.session_state[SESSION_EXCEL_DF]

        display_analysis_results(
            analyzed_excel,
            label="Excel Analysis",
            enrich_enabled=enrich_erpnext_po,
            erpnext_has_credentials=erpnext_ok,
        )
    else:
        st.info("Upload an Excel file to begin analyzer process.")
