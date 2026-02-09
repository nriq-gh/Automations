#!/usr/bin/env python3
import os
import sys
import time
import csv
import argparse
import requests
from typing import Dict, List, Set

# ---------- Tunables ----------
DEFAULT_PAGE_SIZE = 100
REQUEST_TIMEOUT = 30
RATE_SLEEP_SECS = 0.2  # default; can be overridden via --sleep

# ---------- Utilities ----------
def die(msg: str, code: int = 1):
    print(f"ERROR: {msg}", file=sys.stderr)
    sys.exit(code)

def get_env(name: str, required: bool = True, default: str = "") -> str:
    val = os.getenv(name, default)
    if required and not val:
        die(f"missing environment variable {name}")
    return val

def jira_session(base_url: str, email: str, api_token: str) -> requests.Session:
    s = requests.Session()
    s.auth = (email, api_token)
    s.headers.update({"Accept": "application/json"})
    s.base_url = base_url.rstrip("/")
    return s

def get_json(s: requests.Session, path: str, params: Dict = None) -> Dict:
    url = f"{s.base_url}{path}"
    r = s.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
    # Simple retry on 429
    if r.status_code == 429:
        time.sleep(2)
        r = s.get(url, params=params or {}, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json()

# ---------- Jira fetchers ----------
def fetch_all_fields(s: requests.Session) -> List[Dict]:
    """
    GET /rest/api/3/field
    Returns all fields; we filter for custom == True.
    """
    data = get_json(s, "/rest/api/3/field")
    return [f for f in data if f.get("custom") is True]

def fetch_all_screens(s: requests.Session) -> List[Dict]:
    """
    GET /rest/api/3/screens (paginated)
    Only meaningful for Company-managed projects. Team-managed 'screens' may exist but not expose tabs.
    """
    results: List[Dict] = []
    start_at = 0
    while True:
        params = {"startAt": start_at, "maxResults": DEFAULT_PAGE_SIZE}
        data = get_json(s, "/rest/api/3/screens", params=params)
        values = data.get("values", [])
        results.extend(values)
        if data.get("isLast", True) or not values:
            break
        start_at += len(values)
        time.sleep(RATE_SLEEP_SECS)
    return results

def fetch_screen_tabs(s: requests.Session, screen_id: int) -> List[Dict]:
    """
    GET /rest/api/3/screens/{screenId}/tabs
    Team-managed screens often return 400 here. Treat 400 as 'no tabs'.
    """
    try:
        return get_json(s, f"/rest/api/3/screens/{screen_id}/tabs")
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 400:
            # Tabs not supported (likely Team-managed)
            return []
        raise

def fetch_tab_fields(s: requests.Session, screen_id: int, tab_id: int) -> List[Dict]:
    """
    GET /rest/api/3/screens/{screenId}/tabs/{tabId}/fields
    """
    return get_json(s, f"/rest/api/3/screens/{screen_id}/tabs/{tab_id}/fields")

def collect_field_ids_on_screens(s: requests.Session) -> Set[str]:
    """
    Walk all Company-managed screens -> tabs -> fields.
    For Team-managed screens tabs() returns [] by design.
    """
    present: Set[str] = set()
    screens = fetch_all_screens(s)
    for scr in screens:
        sid = scr.get("id")
        if sid is None:
            continue
        tabs = fetch_screen_tabs(s, sid)
        if not tabs:
            # Likely a Team-managed screen, tabs not available → skip silently
            continue
        for tab in tabs:
            tid = tab.get("id")
            if tid is None:
                continue
            try:
                tfields = fetch_tab_fields(s, sid, tid)
            except requests.HTTPError:
                continue
            for f in tfields:
                fid = f.get("fieldId")
                if fid:
                    present.add(fid)
        time.sleep(RATE_SLEEP_SECS)
    return present

def fetch_field_contexts(s: requests.Session, field_id: str) -> List[Dict]:
    """
    GET /rest/api/3/field/{fieldId}/contexts (paginated)
    Works for both Company-managed and Team-managed projects.
    """
    contexts: List[Dict] = []
    start_at = 0
    while True:
        params = {"startAt": start_at, "maxResults": DEFAULT_PAGE_SIZE}
        data = get_json(s, f"/rest/api/3/field/{field_id}/contexts", params=params)
        values = data.get("values", [])
        contexts.extend(values)
        if data.get("isLast", True) or not values:
            break
        start_at += len(values)
        time.sleep(RATE_SLEEP_SECS)
    return contexts

def jql_count_for_field(s: requests.Session, field_id: str) -> int:
    """
    Count issues where cf[id] is not EMPTY using /rest/api/3/search with maxResults=0.
    field_id like 'customfield_12345' → numeric '12345'
    """
    try:
        numeric = field_id.split("_", 1)[1]
    except Exception:
        return 0
    jql = f"cf[{numeric}] is not EMPTY"
    params = {"jql": jql, "maxResults": 0, "fields": "none"}
    data = get_json(s, "/rest/api/3/search", params=params)
    return int(data.get("total", 0))

# ---------- CSV ----------
def write_csv(path: str, rows: List[Dict], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in fieldnames})

# ---------- Main ----------
def main():
    global RATE_SLEEP_SECS  # must be before any usage in this scope

    parser = argparse.ArgumentParser(description="Jira Cloud: count and list unused custom fields.")
    parser.add_argument("--check-data", action="store_true",
                        help="Also check if each field has any data (JQL probe). Slower; avoids false positives.")
    parser.add_argument("--skip-screens", action="store_true",
                        help="Skip walking screens/tabs. Use contexts (+ data if enabled) to decide usage.")
    parser.add_argument("--sleep", type=float, default=RATE_SLEEP_SECS,
                        help="Seconds to sleep between requests (default: 0.2). Increase if you hit rate limits.")
    parser.add_argument("--out-all", default="custom_fields_all.csv",
                        help="CSV path for all custom fields (default: custom_fields_all.csv).")
    parser.add_argument("--out-unused", default="custom_fields_unused.csv",
                        help="CSV path for unused custom fields (default: custom_fields_unused.csv).")
    args = parser.parse_args()

    RATE_SLEEP_SECS = args.sleep

    base_url = get_env("JIRA_BASE_URL")
    email = get_env("JIRA_EMAIL")
    token = get_env("JIRA_API_TOKEN")
    s = jira_session(base_url, email, token)

    # 1) Fields
    print("Fetching custom fields...")
    custom_fields = fetch_all_fields(s)
    total_custom = len(custom_fields)
    print(f"Total custom fields: {total_custom}")

    # Prepare rows
    def row_from_field(f: Dict) -> Dict:
        schema = f.get("schema", {}) or {}
        return {
            "id": f.get("id", ""),
            "name": f.get("name", ""),
            "type": schema.get("type", ""),
            "customType": schema.get("custom", ""),
            "searcherKey": f.get("searcherKey", ""),
            "onAnyScreen": "",  # fill later
            "hasContext": "",   # fill later
            "hasData": "",      # fill later if requested
        }

    all_rows: List[Dict] = [row_from_field(f) for f in custom_fields]

    # 2) Screens (optional)
    fields_on_screens: Set[str] = set()
    if not args.skip_screens:
        print("Scanning screens/tabs/fields to find usage on screens...")
        fields_on_screens = collect_field_ids_on_screens(s)
        print(f"Fields present on any screen: {len(fields_on_screens)}")
        for row in all_rows:
            row["onAnyScreen"] = "Yes" if row["id"] in fields_on_screens else "No"
    else:
        print("Skipping screens walk (requested).")
        for row in all_rows:
            row["onAnyScreen"] = "Unknown"  # we didn't scan; contexts will be main signal

    # 3) Contexts (strong signal; works in both project types)
    print("Checking field contexts (project/issue type associations)...")
    field_has_context: Dict[str, bool] = {}
    for f in custom_fields:
        fid = f["id"]
        try:
            ctxs = fetch_field_contexts(s, fid)
        except requests.HTTPError as e:
            print(f"Warn: contexts fetch failed for {fid}: {e}", file=sys.stderr)
            ctxs = []
        field_has_context[fid] = len(ctxs) > 0
        time.sleep(RATE_SLEEP_SECS)
    for row in all_rows:
        row["hasContext"] = "Yes" if field_has_context.get(row["id"], False) else "No"

    # 4) Optional data probe
    if args.check_data:
        print("Checking data presence via JQL (this may take a while)...")
        for idx, row in enumerate(all_rows, start=1):
            fid = row["id"]
            try:
                cnt = jql_count_for_field(s, fid)
            except requests.HTTPError as e:
                print(f"Warn: JQL count failed for {fid}: {e}", file=sys.stderr)
                cnt = 0
            row["hasData"] = "Yes" if cnt > 0 else "No"
            if idx % 25 == 0:
                print(f"  ...processed {idx}/{total_custom}")
            time.sleep(RATE_SLEEP_SECS)
    else:
        for row in all_rows:
            row["hasData"] = ""

    # 5) Decide unused
    # Definition:
    # - If screens were scanned: onAnyScreen == "No"
    # - AND hasContext == "No"
    # - AND (if --check-data) hasData == "No"
    def is_unused(r: Dict) -> bool:
        screen_ok = True
        if not args.skip_screens:
            screen_ok = (r["onAnyScreen"] == "No")
        # if skip-screens, we don't require screen_ok
        context_ok = (r["hasContext"] == "No")
        data_ok = True if not args.check_data else (r["hasData"] == "No")
        if args.skip_screens:
            return context_ok and data_ok
        return screen_ok and context_ok and data_ok

    unused_rows = [r for r in all_rows if is_unused(r)]
    print(f"Unused custom fields (per criteria): {len(unused_rows)}")

    # 6) Write CSVs
    fieldnames = ["id", "name", "type", "customType", "searcherKey", "onAnyScreen", "hasContext", "hasData"]
    write_csv(args.out_all, all_rows, fieldnames)
    write_csv(args.out_unused, unused_rows, fieldnames)

    print(f"Saved: {args.out_all}")
    print(f"Saved: {args.out_unused}")
    print("Done.")

if __name__ == "__main__":
    main()
