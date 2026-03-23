import streamlit as st
import pandas as pd
import json
import re

st.set_page_config(page_title="ERP Negative Stock Analyzer", layout="wide")

st.title("📦 ERP Negative Stock Analyzer")
st.markdown("Upload your Excel file to extract parentOrderCode, Article Code, and Warehouse from Negative Stock errors.")

# Upload file
uploaded_file = st.file_uploader("Upload Excel File", type=["xlsx"])

# Filters
st.sidebar.header("Filters")
filter_codes_input = st.sidebar.text_area("Filter parentOrderCodes (comma-separated)")
exclude_codes_input = st.sidebar.text_area("Exclude parentOrderCodes (comma-separated)")


def parse_codes(text):
    return set([x.strip() for x in text.split(",") if x.strip()])


def extract_article_warehouse(error_text):
    article_match = re.search(r'(L\d+)', str(error_text))
    # Refined regex to capture warehouse name from inside HTML or plain text
    warehouse_match = re.search(r'Warehouse ([^<]+)', str(error_text))

    article = article_match.group(1) if article_match else None
    warehouse = warehouse_match.group(1).strip() if warehouse_match else None

    return article, warehouse


if uploaded_file:
    df = pd.read_excel(uploaded_file)

    filter_codes = parse_codes(filter_codes_input)
    exclude_codes = parse_codes(exclude_codes_input)

    results = []

    for _, row in df.iterrows():
        payload = row.get("payload")
        erp_status = row.get("erp_status")
        erp_error = row.get("erp_error")
        erp_response = row.get("erp_response")

        if pd.isna(payload):
            continue

        try:
            # Handle both string and dict payload
            if isinstance(payload, str):
                data = json.loads(payload)
            else:
                data = payload
        except:
            continue

        parent_code = str(data.get("parentOrderCode", ""))

        # Filtering
        if filter_codes and parent_code not in filter_codes:
            continue
        if parent_code in exclude_codes:
            continue

        if erp_status == "SUCCESS":
            results.append({
                "parentOrderCode": parent_code,
                "erp_status": erp_status,
                "erp_response": erp_response,
                "article_code": None,
                "warehouse": None,
                "erp_error": None
            })
        elif erp_status == "FAILED":
            article, warehouse = extract_article_warehouse(erp_error)
            results.append({
                "parentOrderCode": parent_code,
                "erp_status": erp_status,
                "article_code": article,
                "warehouse": warehouse,
                "erp_error": erp_error,
                "erp_response": None
            })

    result_df = pd.DataFrame(results)

    st.success(f"✅ Processed {len(result_df)} rows")

    st.dataframe(result_df, use_container_width=True)

    # Download
    if not result_df.empty:
        csv = result_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name="erp_analysis_output.csv",
            mime="text/csv"
        )

        # Insights
        st.subheader("📊 Insights")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### Top Failing SKUs")
            failing_skus = result_df[result_df["erp_status"] == "FAILED"]["article_code"].value_counts().reset_index()
            failing_skus.columns = ["article_code", "count"]
            st.dataframe(failing_skus)

        with col2:
            st.markdown("### Status Distribution")
            st.dataframe(result_df["erp_status"].value_counts().reset_index())

else:
    st.info("Upload an Excel file to begin.")
