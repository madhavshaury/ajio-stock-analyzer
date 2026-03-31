# AJIO ERP Stock Analyzer

A powerful and intuitive Streamlit application designed to help logistics and operations teams analyze ERP order processing logs. It specifically focuses on extracting critical data from "Negative Stock" errors and success responses.

## Features

- **Automated Extraction**: Pulls `parentOrderCode`, `article_code`, and `warehouse` details directly from raw ERP payloads and error messages.
- **Smart Logic**:
  - **SUCCESS Orders**: Captures the parent order code and ERP response.
  - **FAILED Orders**: Parses the `erp_error` text (including HTML and stack traces) using regex to find the specific failing SKU and Warehouse location.
- **Robust Fallback**: If an error message doesn't contain SKU info, the tool automatically falls back to the original order payload to retrieve the data.
- **Filtering & Exclusions**: Filter or exclude specific `parentOrderCode` lists via the sidebar.
- **Interactive Insights**: View distribution charts and a list of top failing SKUs at a glance.
- **Export**: Download the cleaned and processed data as a CSV for further reporting.

## Tech Stack

- **Python**: Core logic and data processing.
- **Streamlit**: Modern UI and web framework.
- **Pandas**: Efficient data manipulation.
- **OpenPyxl**: Excel file support for `.xlsx` processing.

## Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/madhavshaury/ajio-stock-analyzer.git
   cd ajio-stock-analyzer
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Run the application**:
   ```bash
   streamlit run app.py
   ```

## Usage

1. Prepare an Excel file (`.xlsx`) containing at least the following columns: `payload`, `erp_status`, `erp_error`, and `erp_response`.
2. Upload the file through the sidebar.
3. Use the filters to narrow down specific orders.
4. Analyze the results in the interactive table and charts.
5. Download the final report using the **"Download CSV"** button.

---
Built with ❤️ by Madhav Shaurya
