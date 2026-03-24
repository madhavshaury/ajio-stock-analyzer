import streamlit as st
import pandas as pd
import json
import re
import requests
import time
from io import BytesIO
from dotenv import load_dotenv
import os

# Load environment variables if present
load_dotenv()

st.set_page_config(page_title="Order Search & ERP Analyzer", layout="wide")

st.title("📦 Order Search & ERP Analyzer")

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
st.sidebar.header("⚙️ Configuration")

# Redash Config
# Check environment variables for defaults
env_redash_url = os.getenv("REDASH_URL", "https://redash.agilitas.digital")
env_redash_api_key = os.getenv("REDASH_API_KEY", "")
env_data_source_id = os.getenv("REDASH_DATA_SOURCE_ID", "")

# Fix URL if protocol is missing
if env_redash_url and not env_redash_url.startswith("http"):
    env_redash_url = f"https://{env_redash_url}"

with st.sidebar.expander("🔴 Redash Credentials", expanded=not env_redash_api_key):
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
            st.info("⚠️ Could not fetch Data Sources. Ensure you are connected to the **VPN (Pritunl)**.")

    if not redash_api_key:
        st.warning("⚠️ API Key is required for searching.")

# --- HELPERS ---

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
            
            job_progress = st.status("🔍 Querying Redash...")
            for _ in range(30): # 30 attempts, 1s each
                time.sleep(1)
                try:
                    job_resp = requests.get(poll_url, headers=headers)
                    job_data = job_resp.json()
                    
                    status_text = {1: "Pending", 2: "Started", 3: "Success", 4: "Failure"}.get(job_data["job"]["status"], "Processing")
                    job_progress.update(label=f"🔍 Querying Redash... Status: {status_text}", state="running")

                    if job_data["job"]["status"] == 3: # SUCCESS
                        result_id = job_data["job"]["query_result_id"]
                        res_url = f"{url.rstrip('/')}/api/query_results/{result_id}"
                        res_resp = requests.get(res_url, headers=headers)
                        res_data = res_resp.json()
                        job_progress.update(label="✅ Query Complete!", state="complete")
                        return pd.DataFrame(res_data["query_result"]["data"]["rows"])
                    elif job_data["job"]["status"] == 4: # FAILED
                        job_progress.update(label=f"❌ Query Failed: {job_data['job'].get('error', 'Internal Error')}", state="error")
                        return None
                except requests.exceptions.RequestException as poll_error:
                    job_progress.update(label=f"⚠️ Polling interrupted: {str(poll_error)}", state="error")
                    return None
    except requests.exceptions.ConnectionError:
        st.error("❌ **Redash Connection Failed.** Please ensure you are connected to the **Pritunl VPN** and try again.")
        return None
    except requests.exceptions.Timeout:
        st.error("❌ **Redash Connection Timeout.** Check if your VPN connection is stable.")
        return None
    except Exception as e:
        st.error(f"❌ Error connecting to Redash: {str(e)}")
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
                "erp_error": None
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

            results.append({
                "parentOrderCode": parent_code,
                "erp_status": "FAILED",
                "article_code": article,
                "warehouse": warehouse,
                "erp_error": erp_error or erp_response,
                "erp_response": None
            })
    
    return pd.DataFrame(results)

def display_analysis_results(result_df, label="Results"):
    """Displays the dataframe and insights for processed results."""
    if not result_df.empty:
        st.success(f"✅ Analyzed {len(result_df)} records.")
        st.dataframe(result_df, width='stretch')

        # Download
        csv = result_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label=f"📥 Download {label} CSV",
            data=csv,
            file_name=f"{label.lower().replace(' ', '_')}_{int(time.time())}.csv",
            mime="text/csv"
        )

        # Insights
        st.subheader("📊 Analysis Insights")
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
    else:
        st.warning("No data matched the analysis criteria. Check if the 'status' or 'error' columns are available.")

# --- MAIN UI ---

tab_redash, tab_erp = st.tabs(["🚀 Redash Search (Manual)", "🔍 ERP Analyzer (Excel)"])

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
            run_query = st.button("🔎 Search in Redash", use_container_width=True, type="primary")
        
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

            with st.expander("📝 View SQL Query"):
                st.code(sql_query, language="sql")
            
            results_df = run_redash_query(redash_url, redash_api_key, data_source_id, sql_query)
            
            if results_df is not None:
                if not results_df.empty:
                    st.divider()
                    st.subheader("🎯 Automated Analysis for Redash results")
                    analyzed_res = perform_analysis(results_df, input_codes=codes)
                    display_analysis_results(analyzed_res, label="Redash Analysis")
                    
                    st.divider()
                    st.subheader("📋 Raw Data (Redash)")
                    st.dataframe(results_df, width='stretch')
                else:
                    st.warning("No records found for these order codes in Redash.")

# -----------------
# TAB 2: ERP ANALYZER (OPTIONAL EXCEL)
# -----------------
with tab_erp:
    st.header("ERP Negative Stock Analyzer")
    st.markdown("Upload an Excel file to extract SKUs and Warehouses from failure logs.")
    
    uploaded_file = st.file_uploader("Upload ERP Log File (.xlsx)", type=["xlsx"], key="erp_uploader")
    
    if uploaded_file:
        df_raw = pd.read_excel(uploaded_file)
        
        # Analyzer Filters
        col1, col2 = st.columns(2)
        with col1:
            filter_input = st.text_area("Filter Codes (comma-sep)", key="filter_erp")
        with col2:
            exclude_input = st.text_area("Exclude Codes (comma-sep)", key="exclude_erp")
            
        filter_codes = set([x.strip() for x in filter_input.split(",") if x.strip()])
        exclude_codes = set([x.strip() for x in exclude_input.split(",") if x.strip()])

        analyzed_excel = perform_analysis(df_raw)
        
        # Apply filters to analyzed results
        if filter_codes:
            analyzed_excel = analyzed_excel[analyzed_excel["parentOrderCode"].isin(filter_codes)]
        if exclude_codes:
            analyzed_excel = analyzed_excel[~analyzed_excel["parentOrderCode"].isin(exclude_codes)]

        display_analysis_results(analyzed_excel, label="Excel Analysis")
    else:
        st.info("Upload an Excel file to begin analyzer process.")
