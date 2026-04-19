# Order Search & ERP Analyzer

Streamlit app for analyzing ERP order logs from **Redash** (`api_event_log`) or **Excel**, with optional **ERPNext** enrichment (Bin stock / valuation flags and open Purchase Orders). It focuses on failed outward orders and classifies failures (negative stock, zero valuation, internal server errors).

## Features

- **Redash Search**: Enter `parentOrderCode` values; the app runs a parameterized SQL query against `api_event_log` via the Redash API and analyzes rows.
- **Excel Analyzer**: Upload `.xlsx` logs with payload/status/error columns; optional filter/exclude by `parentOrderCode`.
- **Failure classification**: Each failed row is labeled (see [Failure categories](#failure-categories)).
- **ERPNext enrichment** (sidebar; **on by default**): For failed rows with extracted `article_code` and `warehouse`, calls ERPNext **read-only** REST APIs for Bin quantities/valuation and, for **negative-stock** failures only, open POs matching item + warehouse.
- **Results cache**: After a successful Redash search or Excel analysis, results stay in the browser session so changing **visible columns** does not trigger a new Redash or ERPNext fetch.
- **Visible columns**: Dropdown presets (All / Core / Core + ERP / Custom) plus CSV download matching the table.

## Requirements

- Python 3.10+
- Dependencies: `streamlit`, `pandas`, `requests`, `python-dotenv`, `openpyxl`

```bash
pip install -r requirements.txt
```

## Configuration

Copy [`.env.example`](.env.example) to `.env` and set values (optional defaults for local dev).

| Variable | Purpose |
|----------|---------|
| `REDASH_URL` | Redash base URL |
| `REDASH_API_KEY` | Redash API key (`Authorization: Key ...`) |
| `REDASH_DATA_SOURCE_ID` | Default data source for ad-hoc queries |
| `ERPNEXT_URL` | ERPNext site root (no `/app`; API is under `/api/`) |
| `ERPNEXT_API_KEY` / `ERPNEXT_API_SECRET` | Frappe token auth |
| `ERPNEXT_TEST_PO` | Optional PO name for the sidebar “Test ERPNext API” diagnostic |

Run:

```bash
streamlit run app.py
```

## Data sources and columns

The analyzer maps flexible column names (case-insensitive), e.g. `payload` / `request_payload`, `erp_status` / `status`, `erp_error` / `error`, `erp_response` / `response`. Failed rows parse **Article** and **Warehouse** from error/response text (regex) when possible.

## Failure categories

Classification uses a **fixed order** (first match wins) on a single text blob built from error, response, and row fields (`classify_failure_category` in `app.py`).

1. **Internal Server Error** — HTTP 5xx / gateway / “internal server error” style phrases in the text.
2. **Zero Valuation Error** — Phrases such as `zero valuation`, `valuation rate`, `valuation rate not found`, `valuation rate is mandatory`, `cannot make stock entry`, `stock value is zero`, etc.
3. **Negative Stock** — Phrases such as `negative stock`, `insufficient stock`, `not enough stock`, `out of stock`, `stock not available`, or `stock` combined with `negative` / `insufficient` / `shortage`, etc.
4. **Other** — Anything that does not match the above.

**Open PO lookup** runs only for rows classified as **Negative Stock** that also have both `article_code` and `warehouse`. **Bin stock** enrichment runs for **all failed rows** that have article + warehouse (when enrichment is enabled and credentials are set).

## ERPNext methodology (read-only GET APIs)

All calls use `Authorization: token <api_key>:<api_secret>` and only **GET** `/api/resource/...` (no document POST). Implementation: [`erpnext_client.py`](erpnext_client.py).

### Bin (stock, valuation, health)

- **Endpoint**: `GET /api/resource/Bin` with Frappe filters: `item_code`, `warehouse` (warehouse string is whitespace-normalized).
- **Fields requested**: `actual_qty`, `valuation_rate`, `reserved_qty`, `projected_qty`, `ordered_qty`, plus `item_code` / `warehouse`.
- **No Bin row**: Treated as **never inwarded** (`bin_never_inwarded`), health code `never_inwarded`.
- **Health codes** (from `actual_qty` + `valuation_rate` when a Bin exists):
  - `never_inwarded` — no Bin row (handled separately).
  - `out_of_stock` — `actual_qty <= 0`.
  - `zero_valuation_with_stock` — `actual_qty > 0` and `valuation_rate <= 0` (flags `zero_valuation_risk`).
  - `stock_and_valuation_ok` — `actual_qty > 0` and `valuation_rate > 0` (`strict_stock_and_valuation_ok`).

These are **live ERPNext Bin** signals, separate from the log-text **Zero Valuation Error** category above (that uses message keywords from ERP responses).

### Open Purchase Orders

Open POs are **not** inferred from Purchase Receipt lines. They use the **Purchase Order** list API with **child-table filters** on **Purchase Order Item** (same pattern as Frappe/Desk list views).

**Strict “open PO” query** (one list GET):

- Child filters: `Purchase Order Item.item_code = <sku>`, `Purchase Order Item.warehouse = <warehouse>`.
- Parent filters: `Purchase Order.per_received = 0`, `Purchase Order.docstatus = 1`, `Purchase Order.status` in:

  `To Receive and Bill`, `To Bill`, `To Receive`

  (`To Receive` is included because ERPNext often uses it for receivable work; omitting it previously missed valid POs.)

- Returned POs are deduped by `name`; counts and comma-separated names/statuses are exposed in the UI/CSV.

**Diagnostic (when strict query returns zero rows)**:

- A **looser** query runs: same item + warehouse + `docstatus = 1` only (no `per_received` / status filter).
- If any submitted PO lines exist, `open_po_diagnostic` explains per-PO **status** (whether in the allowed list) and **per_received** (strict filter requires parent `per_received = 0`), so you can see mismatches vs Desk.

### Sidebar diagnostics

**Test ERPNext API** runs `diagnose_erpnext_api`: ping, logged-in user, sample list GETs for Purchase Order and Bin to localize 401/403/404 vs URL/token/permissions.

## Hosting and Redash network path

Redash queries are executed with **Python `requests` on the machine that runs Streamlit**. Traffic to Redash therefore uses the **server’s network path**, not each user’s VPN. If Redash (or its query database) is only reachable when **analysts** are on VPN, the Streamlit host must be able to reach Redash by your org’s network policy, or analysts need a workflow that does not rely on server-side Redash (for example Excel upload only, or a future client-side Redash integration). ERPNext in this project is expected to be reachable from the app server without per-user VPN.

## Project layout

| File | Role |
|------|------|
| [`app.py`](app.py) | Streamlit UI, Redash + Excel flows, classification, enrichment orchestration |
| [`erpnext_client.py`](erpnext_client.py) | ERPNext REST helpers (Bin, Purchase Order list, API diagnostics) |

## License / attribution

Maintained by Madhav Shaurya.
