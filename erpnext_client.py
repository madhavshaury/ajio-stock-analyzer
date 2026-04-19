"""ERPNext (Frappe) REST helpers: Bin stock/valuation and open Purchase Orders."""

from __future__ import annotations

import json
import re
from typing import Any
from urllib.parse import quote, urlparse, urlunparse

import requests

PO_DOCTYPE = "Purchase Order"
# Child doctype name used in parent list filters (same as ERPNext/curl, not the parent fieldname "items").
PO_LINE_DOCTYPE = "Purchase Order Item"
BIN_DOCTYPE = "Bin"

# Open POs with no GRN yet (per_received = 0) and receivable statuses (cURL 3).
# Include "To Receive" — ERPNext often uses it when billing is complete but receiving is not (or vice versa);
# omitting it caused missed POs vs Desk.
OPEN_PO_STATUS_IN = ["To Receive and Bill", "To Bill", "To Receive"]


def normalize_base_url(url: str | None) -> str:
    """
    Site root for Frappe REST API (e.g. https://erp.example.com).

    Desk URLs like https://erp.example.com/app or .../app/purchase-order
    must not be used as the base — API lives at /api/... on the site root.
    Subfolder installs keep the path prefix (e.g. https://host/mybench → /mybench/api).
    """
    if not url or not str(url).strip():
        return ""
    u = str(url).strip()
    if u and not u.startswith("http"):
        u = f"https://{u}"
    parsed = urlparse(u)
    path = (parsed.path or "").rstrip("/")

    # Strip Frappe Desk: /app, /app/..., /desk, or trailing .../app
    if path == "/app" or path.startswith("/app/"):
        path = ""
    elif path == "/desk" or path.startswith("/desk/"):
        path = ""
    elif path.endswith("/app"):
        path = path[: -len("/app")].rstrip("/")
    elif path.endswith("/desk"):
        path = path[: -len("/desk")].rstrip("/")

    path = path or ""
    rebuilt = urlunparse(
        (parsed.scheme or "https", parsed.netloc, path, "", "", "")
    )
    return rebuilt.rstrip("/")


def resource_api_url(
    base_url: str, doctype: str, document_name: str | None = None
) -> str:
    """
    Build /api/resource/<Doctype> or /api/resource/<Doctype>/<name> like curl/Frappe Desk.
    Uses percent-encoding for spaces (Purchase%20Order) and special characters in names.
    """
    root = (base_url or "").rstrip("/")
    dt = quote(str(doctype).strip(), safe="")
    if not document_name or not str(document_name).strip():
        return f"{root}/api/resource/{dt}"
    dn = quote(str(document_name).strip(), safe="")
    return f"{root}/api/resource/{dt}/{dn}"


def normalize_warehouse_whitespace(warehouse: str | None) -> str:
    if not warehouse or not str(warehouse).strip():
        return ""
    return re.sub(r"\s+", " ", str(warehouse).strip())


def _auth_headers(api_key: str, api_secret: str) -> dict[str, str]:
    return {
        "Authorization": f"token {api_key}:{api_secret}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _parse_frappe_error_snippet(response: requests.Response) -> str:
    """Extract a short message; avoid dumping full Frappe tracebacks into the UI."""
    try:
        j = response.json()
    except Exception:
        return (response.text or "")[:280].replace("\n", " ").strip()

    exc = j.get("exc")
    if isinstance(exc, str) and exc.strip():
        # Prefer the last exception line (often the real error)
        for line in reversed(exc.strip().splitlines()):
            line = line.strip()
            if not line or line.startswith("File "):
                continue
            if "frappe.exceptions." in line or "Permission" in line or "Error" in line:
                return line[:450]
        return exc.strip().splitlines()[-1][:450]

    msg = j.get("message")
    if msg is not None:
        return str(msg)[:450]

    return (response.text or "")[:280].replace("\n", " ").strip()


def _api_http_error(response: requests.Response, doctype: str) -> str:
    code = response.status_code
    if code == 403:
        return (
            f"HTTP 403: Permission denied — the ERPNext user for this API token cannot read "
            f"'{doctype}'. Assign a role with access (e.g. Stock User, Purchase User) or grant "
            "Read permission on this DocType in Role Permission Manager."
        )
    if code == 401:
        return "HTTP 401: Unauthorized — check API key and secret, or that the user is enabled."
    if code == 404:
        return (
            f"HTTP 404 for '{doctype}'. Wrong site URL or path if /api/method/ping also fails; "
            "or filtered GET is blocked by a proxy (try from same network as ERPNext)."
        )
    snippet = _parse_frappe_error_snippet(response)
    return f"HTTP {code}: {snippet}"


def _resource_list(
    base_url: str,
    api_key: str,
    api_secret: str,
    doctype: str,
    *,
    filters: list | None = None,
    fields: list[str] | None = None,
    limit_page_length: int = 50,
    timeout: int = 30,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """
    GET /api/resource/<Doctype> only (no POST).

    Passes Frappe list filters as query params: `filters` and `fields` are JSON-encoded
    lists (Frappe filter DSL). Child-table / “link” style filters on Purchase Order use
    the table field name, e.g. ["items", "item_code", "=", "SKU"].
    """
    url = resource_api_url(base_url, doctype)
    params: dict[str, Any] = {"limit_page_length": limit_page_length}
    if filters is not None:
        params["filters"] = json.dumps(filters)
    if fields is not None:
        params["fields"] = json.dumps(fields)
    try:
        r = requests.get(
            url,
            headers=_auth_headers(api_key, api_secret),
            params=params,
            timeout=timeout,
        )
        if r.status_code >= 400:
            return None, _api_http_error(r, doctype)
        data = r.json().get("data")
        if data is None:
            return [], None
        return data, None
    except requests.exceptions.RequestException as e:
        return None, str(e)[:500]


def _dedupe_po_rows(rows: list[dict[str, Any]] | None) -> list[dict[str, Any]]:
    by_name: dict[str, dict[str, Any]] = {}
    for row in rows or []:
        n = row.get("name")
        if n and n not in by_name:
            by_name[str(n)] = row
    return list(by_name.values())


def _open_po_loose_submitted_lines(
    base_url: str,
    api_key: str,
    api_secret: str,
    item_code: str,
    warehouse: str,
    *,
    timeout: int,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    """Submitted POs with a line for this item + warehouse (no per_received/status filter)."""
    filters = [
        [PO_LINE_DOCTYPE, "item_code", "=", item_code],
        [PO_LINE_DOCTYPE, "warehouse", "=", warehouse],
        ["Purchase Order", "docstatus", "=", 1],
    ]
    return _resource_list(
        base_url,
        api_key,
        api_secret,
        PO_DOCTYPE,
        filters=filters,
        fields=["name", "status", "per_received", "supplier"],
        limit_page_length=50,
        timeout=timeout,
    )


def get_open_pos_for_item_warehouse(
    base_url: str,
    api_key: str,
    api_secret: str,
    item_code: str,
    warehouse: str,
    *,
    timeout: int = 30,
) -> dict[str, Any]:
    """
    Open POs per documented cURL 3: line matches item + warehouse, per_received = 0,
    submitted, status in OPEN_PO_STATUS_IN (GRN not done yet).

    Returns a dict with:
      - open_po_count: int
      - open_po_names: comma-separated PO names
      - open_po_statuses: comma-separated statuses
      - open_po_detail: "name | supplier | status; ..."
      - matching_po_count: same as open_po_count (API already filters)
      - open_po_diagnostic: when strict query is empty, explains if a looser query still finds POs
      - error: optional API error string
    """
    base_url = normalize_base_url(base_url)
    item_code = (item_code or "").strip()
    warehouse = normalize_warehouse_whitespace(warehouse)
    empty = {
        "open_po_count": 0,
        "open_po_names": "",
        "open_po_statuses": "",
        "open_po_detail": "",
        "matching_po_count": 0,
        "open_po_diagnostic": "",
        "error": None,
    }
    if not base_url or not urlparse(base_url).netloc:
        return {**empty, "error": "Invalid ERPNext URL"}
    if not api_key or not api_secret:
        return {**empty, "error": "Missing API key or secret"}
    if not item_code or not warehouse:
        return {**empty, "error": "Missing item_code or warehouse"}

    filters = [
        [PO_LINE_DOCTYPE, "item_code", "=", item_code],
        [PO_LINE_DOCTYPE, "warehouse", "=", warehouse],
        ["Purchase Order", "per_received", "=", 0],
        ["Purchase Order", "docstatus", "=", 1],
        ["Purchase Order", "status", "in", OPEN_PO_STATUS_IN],
    ]
    po_fields = [
        "name",
        "supplier",
        "transaction_date",
        "status",
        "per_received",
        "per_billed",
        "grand_total",
        "schedule_date",
    ]
    rows, err = _resource_list(
        base_url,
        api_key,
        api_secret,
        PO_DOCTYPE,
        filters=filters,
        fields=po_fields,
        limit_page_length=50,
        timeout=timeout,
    )
    if err:
        return {**empty, "error": err}

    if not rows:
        loose, loose_err = _open_po_loose_submitted_lines(
            base_url, api_key, api_secret, item_code, warehouse, timeout=timeout
        )
        diag = ""
        if not loose_err and loose:
            parts = []
            for r in _dedupe_po_rows(loose):
                pr = r.get("per_received")
                try:
                    prf = float(pr) if pr is not None else None
                except (TypeError, ValueError):
                    prf = None
                st = (r.get("status") or "").strip()
                allowed = st in OPEN_PO_STATUS_IN
                per_received_ok = prf is not None and prf <= 0
                parts.append(
                    f"{r.get('name')}: status={st!r} (in_allowed_list={allowed}), "
                    f"per_received={pr!r} (strict_needs_per_received_0={per_received_ok})"
                )
            diag = (
                "Strict open-PO filter returned 0 rows, but submitted PO line(s) exist for this "
                f"item+warehouse. Check status/per_received vs filters {OPEN_PO_STATUS_IN} and "
                f"per_received=0. Details: {'; '.join(parts)}"
            )
        elif loose_err:
            diag = f"(Could not run loose PO diagnostic: {loose_err})"
        return {**empty, "open_po_diagnostic": diag}

    rows_unique = _dedupe_po_rows(rows)
    n_open = len(rows_unique)

    out_names: list[str] = []
    out_statuses: list[str] = []
    detail_parts: list[str] = []
    for row in rows_unique:
        pname = str(row.get("name", "")).strip()
        if not pname:
            continue
        st = (row.get("status") or "").strip()
        sup = str(row.get("supplier") or "").strip()
        out_names.append(pname)
        out_statuses.append(st)
        detail_parts.append(f"{pname} | {sup or '-'} | {st}")

    return {
        "open_po_count": n_open,
        "open_po_names": ", ".join(out_names),
        "open_po_statuses": ", ".join(out_statuses),
        "open_po_detail": "; ".join(detail_parts),
        "matching_po_count": n_open,
        "open_po_diagnostic": "",
        "error": None,
    }


def _bin_health_from_qty_val(
    actual: float | None,
    valuation_rate: float | None,
    bin_found: bool,
) -> tuple[str, bool, bool]:
    """
    Returns (bin_health_code, zero_valuation_risk, strict_stock_and_valuation_ok).
    Mirrors documented cURL response logic (never inwarded handled by caller).
    """
    if not bin_found:
        return "never_inwarded", False, False
    a = 0.0 if actual is None else float(actual)
    if a <= 0:
        return "out_of_stock", False, False
    vr = 0.0 if valuation_rate is None else float(valuation_rate)
    if a > 0 and vr <= 0:
        return "zero_valuation_with_stock", True, False
    if a > 0 and vr > 0:
        return "stock_and_valuation_ok", False, True
    return "unknown", False, False


def get_bin_stock_for_item_warehouse(
    base_url: str,
    api_key: str,
    api_secret: str,
    item_code: str,
    warehouse: str,
    *,
    timeout: int = 30,
) -> dict[str, Any]:
    """
    Single Bin GET (cURL 1): item_code + warehouse; fields include valuation_rate and ordered_qty.
    Derives health codes (cURL 2 strict OK and cURL 6 zero valuation) in Python without a second GET.
    """
    base_url = normalize_base_url(base_url)
    item_code = (item_code or "").strip()
    warehouse = normalize_warehouse_whitespace(warehouse)
    empty: dict[str, Any] = {
        "actual_qty": None,
        "reserved_qty": None,
        "projected_qty": None,
        "ordered_qty": None,
        "valuation_rate": None,
        "bin_found": False,
        "bin_never_inwarded": False,
        "bin_health_code": "",
        "zero_valuation_risk": False,
        "strict_stock_and_valuation_ok": False,
        "error": None,
    }
    if not base_url or not urlparse(base_url).netloc:
        return {**empty, "error": "Invalid ERPNext URL"}
    if not api_key or not api_secret:
        return {**empty, "error": "Missing API key or secret"}
    if not item_code or not warehouse:
        return {**empty, "error": "Missing item_code or warehouse"}

    filters = [
        ["item_code", "=", item_code],
        ["warehouse", "=", warehouse],
    ]
    bin_fields = [
        "item_code",
        "warehouse",
        "actual_qty",
        "valuation_rate",
        "reserved_qty",
        "projected_qty",
        "ordered_qty",
    ]
    rows, err = _resource_list(
        base_url,
        api_key,
        api_secret,
        BIN_DOCTYPE,
        filters=filters,
        fields=bin_fields,
        limit_page_length=10,
        timeout=timeout,
    )
    if err:
        return {**empty, "error": err}
    if not rows:
        return {
            **empty,
            "actual_qty": 0.0,
            "reserved_qty": 0.0,
            "projected_qty": 0.0,
            "ordered_qty": 0.0,
            "valuation_rate": None,
            "bin_found": False,
            "bin_never_inwarded": True,
            "bin_health_code": "never_inwarded",
            "zero_valuation_risk": False,
            "strict_stock_and_valuation_ok": False,
            "error": None,
        }

    row = rows[0]

    def _f(x: Any) -> float | None:
        if x is None:
            return None
        try:
            return float(x)
        except (TypeError, ValueError):
            return None

    actual = _f(row.get("actual_qty"))
    val_rate = _f(row.get("valuation_rate"))
    health, zv_risk, strict_ok = _bin_health_from_qty_val(actual, val_rate, True)

    return {
        "actual_qty": actual,
        "reserved_qty": _f(row.get("reserved_qty")),
        "projected_qty": _f(row.get("projected_qty")),
        "ordered_qty": _f(row.get("ordered_qty")),
        "valuation_rate": val_rate,
        "bin_found": True,
        "bin_never_inwarded": False,
        "bin_health_code": health,
        "zero_valuation_risk": zv_risk,
        "strict_stock_and_valuation_ok": strict_ok,
        "error": None,
    }


def diagnose_erpnext_api(
    base_url: str,
    api_key: str,
    api_secret: str,
    *,
    test_po_name: str | None = None,
    timeout: int = 15,
) -> dict[str, Any]:
    """
    Call a few standard endpoints so you can see *where* failures happen.

    - ping: should be 200 JSON even without auth (proves /api reaches Frappe).
    - get_logged_user: 200 with token = credentials OK.
    - Purchase Order / Bin list: 200 = read permission; Bin list requests valuation_rate like enrichment.
    """
    base = normalize_base_url(base_url)
    out: dict[str, Any] = {"base_url": base, "steps": []}

    def add_step(name: str, url: str, r: requests.Response | None, err: str | None) -> None:
        entry: dict[str, Any] = {"name": name, "url": url}
        if err:
            entry["status_code"] = None
            entry["error"] = err[:500]
        elif r is not None:
            entry["status_code"] = r.status_code
            txt = (r.text or "")[:500].replace("\n", " ")
            entry["body_preview"] = txt
        out["steps"].append(entry)

    if not base or not urlparse(base).netloc:
        out["hint"] = "ERPNext URL is empty or invalid after normalization."
        return out

    # 1) Public ping — proves Frappe /api is reachable on this host
    ping_ok = False
    try:
        r0 = requests.get(f"{base}/api/method/ping", timeout=timeout)
        add_step("GET /api/method/ping (no token)", f"{base}/api/method/ping", r0, None)
        ping_ok = r0.status_code == 200
    except requests.RequestException as e:
        add_step("GET /api/method/ping (no token)", f"{base}/api/method/ping", None, str(e))

    if not (api_key or "").strip() or not (api_secret or "").strip():
        out["hint"] = (
            "Add API key and secret to run authenticated checks. "
            "If ping alone fails, the hostname/VPN/proxy is wrong — not your token."
        )
        return out

    hdrs = _auth_headers(api_key, api_secret)

    try:
        r1 = requests.get(
            f"{base}/api/method/frappe.auth.get_logged_user",
            headers=hdrs,
            timeout=timeout,
        )
        add_step(
            "GET /api/method/frappe.auth.get_logged_user",
            f"{base}/api/method/frappe.auth.get_logged_user",
            r1,
            None,
        )
    except requests.RequestException as e:
        add_step(
            "GET /api/method/frappe.auth.get_logged_user",
            f"{base}/api/method/frappe.auth.get_logged_user",
            None,
            str(e),
        )

    po_list_url = resource_api_url(base, PO_DOCTYPE)
    try:
        r2 = requests.get(
            po_list_url,
            headers=hdrs,
            params={"limit_page_length": 1},
            timeout=timeout,
        )
        add_step(
            "(list) GET /api/resource/Purchase Order?limit_page_length=1",
            po_list_url,
            r2,
            None,
        )
    except requests.RequestException as e:
        add_step(
            "(list) GET /api/resource/Purchase Order",
            po_list_url,
            None,
            str(e),
        )

    name = (test_po_name or "").strip()
    if name:
        po_one_url = resource_api_url(base, PO_DOCTYPE, name)
        try:
            r2b = requests.get(po_one_url, headers=hdrs, timeout=timeout)
            add_step(
                f"GET single Purchase Order (curl-style): …/{name}",
                po_one_url,
                r2b,
                None,
            )
        except requests.RequestException as e:
            add_step(
                "GET single Purchase Order",
                po_one_url,
                None,
                str(e),
            )

    bin_list_url = resource_api_url(base, BIN_DOCTYPE)
    try:
        r3 = requests.get(
            bin_list_url,
            headers=hdrs,
            params={
                "limit_page_length": 1,
                "fields": json.dumps(
                    ["item_code", "warehouse", "actual_qty", "valuation_rate"]
                ),
            },
            timeout=timeout,
        )
        add_step(
            "(list) GET /api/resource/Bin (incl. valuation_rate)",
            bin_list_url,
            r3,
            None,
        )
    except requests.RequestException as e:
        add_step(
            "(list) GET /api/resource/Bin",
            bin_list_url,
            None,
            str(e),
        )

    # Short interpretation
    hints: list[str] = []
    if not ping_ok:
        hints.append(
            "Ping is not HTTP 200 — this URL is probably not your Frappe site root "
            "(wrong host, VPN required, or /api not routed). Env can be 'correct' but unreachable from this machine."
        )
    else:
        hints.append("Ping OK — Frappe /api is reachable.")
    list_po_404 = False
    single_po_ok = False
    saw_other_resource_404 = False
    for s in out["steps"]:
        nm = (s.get("name") or "").lower()
        sc = s.get("status_code")
        if sc == 404 and "(list)" in nm and "purchase order" in nm:
            list_po_404 = True
        if sc == 200 and "single" in nm and "purchase order" in nm:
            single_po_ok = True
        if sc == 404 and "(list)" in nm and "bin" in nm:
            saw_other_resource_404 = True

    if list_po_404 and single_po_ok:
        hints.append(
            "PO list returned 404 but single-doc GET worked — token and URL match your curl; "
            "check reverse proxy/WAF for GET requests with query strings vs path-only URLs."
        )
    elif list_po_404 or saw_other_resource_404:
        hints.append(
            "404 on /api/resource/… while ping works: wrong site on multi-tenant bench, reverse proxy only "
            "serving /app, or infra blocking /api — not a typo in .env keys."
        )

    out["hint"] = " ".join(hints)
    return out
