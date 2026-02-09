#!/usr/bin/env python3
"""
Jira Technical Audit (Cloud) — resilient version

Outputs CSV/JSON in ./jira_audit_output/:
- projects.csv
- project_last_activity.csv    (note column includes archived / error notes)
- fields.csv
- permission_schemes.csv
- project_workflow_schemes.csv
- project_issuetype_screen_schemes.csv
- project_field_config_schemes.csv
- screens.csv (or screens_raw.json / screens_error.json)
- groups.csv (+ groups_raw.json, global_permissions.json)
- apps_raw.json (merged results and/or errors)
- automation_rules.csv (best-effort)
- flag_abandoned_projects.csv
- flag_duplicate_fields.csv
- README.json
"""

import os, csv, json, time
from datetime import datetime, timedelta, timezone
import requests
from requests.auth import HTTPBasicAuth
from dateutil.parser import isoparse
from tqdm import tqdm
from collections import defaultdict

# --- Env & output folder ---
BASE_URL = os.environ.get("JIRA_BASE_URL", "").rstrip("/")
EMAIL = os.environ.get("JIRA_EMAIL", "")
TOKEN = os.environ.get("JIRA_API_TOKEN", "")

OUTDIR = "jira_audit_output"
os.makedirs(OUTDIR, exist_ok=True)

auth = HTTPBasicAuth(EMAIL, TOKEN)
HEADERS = {"Accept": "application/json"}

# --- HTTP helpers ---

def get(path, params=None, api="3", absolute=False):
    """Generic GET with 429 backoff and rich error details."""
    if absolute:
        # Accept full URL or root-relative path (prepend BASE_URL)
        url = path if path.startswith("http") else f"{BASE_URL}{path}"
    else:
        url = f"{BASE_URL}/rest/api/{api}{path}"

    resp = requests.get(url, headers=HEADERS, auth=auth, params=params, timeout=60)
    if resp.status_code == 429:
        time.sleep(int(resp.headers.get("Retry-After", "5")))
        return get(path, params, api, absolute)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise requests.HTTPError(f"{e} | details: {detail}") from e
    return resp.json()

def post(path, payload=None, api="3", absolute=False):
    """Generic POST with 429 backoff and rich error details (for /search, etc.)."""
    if absolute:
        url = path if path.startswith("http") else f"{BASE_URL}{path}"
    else:
        url = f"{BASE_URL}/rest/api/{api}{path}"

    resp = requests.post(
        url,
        headers={**HEADERS, "Content-Type": "application/json"},
        auth=auth,
        json=(payload or {}),
        timeout=60,
    )
    if resp.status_code == 429:
        time.sleep(int(resp.headers.get("Retry-After", "5")))
        return post(path, payload, api, absolute)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        try:
            detail = resp.json()
        except Exception:
            detail = resp.text
        raise requests.HTTPError(f"{e} | details: {detail}") from e
    return resp.json()

def get_paged(path, result_key, params=None, api="3", start_key="startAt", max_key="maxResults"):
    """Paginator for endpoints that return {total, startAt, maxResults, <result_key>: []}."""
    start_at = 0
    max_results = 100
    while True:
        p = dict(params or {})
        p[start_key] = start_at
        p[max_key] = max_results
        data = get(path, p, api=api)
        values = data.get(result_key, [])
        for v in values:
            yield v
        total = data.get("total", 0)
        if start_at + max_results >= total:
            break
        start_at += max_results

# --- Writers ---

def write_csv(path, rows, headers):
    with open(os.path.join(OUTDIR, path), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow({h: r.get(h, "") for h in headers})

def write_json(path, obj):
    with open(os.path.join(OUTDIR, path), "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)

# --- Collectors ---

def collect_projects():
    rows = []
    for p in get_paged("/project/search", "values", params={"expand": "lead,insight"}):
        rows.append({
            "id": p.get("id"),
            "key": p.get("key"),
            "name": p.get("name"),
            "projectTypeKey": p.get("projectTypeKey"),
            "style": p.get("style"),  # company-managed / team-managed
            "leadAccountId": (p.get("lead") or {}).get("accountId", ""),
            "leadDisplayName": (p.get("lead") or {}).get("displayName", ""),
            "isPrivate": p.get("isPrivate", False),
            "archived": p.get("archived", False),
            "permissionSchemeId": (p.get("permissionScheme") or {}).get("id", ""),
        })
    write_csv("projects.csv", rows, [
        "id","key","name","projectTypeKey","style","leadAccountId","leadDisplayName","isPrivate","archived","permissionSchemeId"
    ])
    return rows

def collect_last_activity_by_project(project_rows):
    """
    Resilient per-project last-issue-updated:
    - Skips archived projects.
    - Uses POST /search (preferred) with minimal fields.
    - Logs per-project errors (410/403/404…) in 'note' column and continues.
    """
    results = []
    for p in tqdm(project_rows, desc="Last issue activity"):
        key = p["key"]
        if p.get("archived", False):
            results.append({"projectKey": key, "lastIssueUpdated": "", "note": "archived"})
            continue
        try:
            data = post("/search", {
                "jql": f'project = "{key}" ORDER BY updated DESC',
                "maxResults": 1,
                "fields": ["updated"]
            })
            last_updated = ""
            if data.get("issues"):
                last_updated = data["issues"][0]["fields"]["updated"]
            results.append({"projectKey": key, "lastIssueUpdated": last_updated, "note": ""})
        except requests.HTTPError as e:
            results.append({"projectKey": key, "lastIssueUpdated": "", "note": f"search_error: {str(e)}"})
            continue

    write_csv("project_last_activity.csv", results, ["projectKey", "lastIssueUpdated", "note"])
    return results

def collect_fields():
    fields = get("/field")
    name_bucket = defaultdict(list)
    for f in fields:
        name_bucket[f.get("name","")].append(f.get("id",""))
    rows = []
    for f in fields:
        rows.append({
            "id": f.get("id",""),
            "name": f.get("name",""),
            "custom": f.get("custom", False),
            "schema": json.dumps(f.get("schema",{}), ensure_ascii=False),
            "isDuplicateName": len(name_bucket[f.get("name","")]) > 1
        })
    write_csv("fields.csv", rows, ["id","name","custom","schema","isDuplicateName"])
    return rows

def collect_permission_schemes():
    data = get("/permissionscheme")
    rows = []
    for s in data.get("permissionSchemes", []):
        scheme_id = s.get("id")
        detail = get(f"/permissionscheme/{scheme_id}")
        for p in detail.get("permissions", []):
            holder = p.get("holder", {})
            rows.append({
                "schemeId": scheme_id,
                "schemeName": s.get("name",""),
                "permission": p.get("permission",""),
                "holderType": holder.get("type",""),
                "holderParam": holder.get("parameter",""),
            })
    write_csv("permission_schemes.csv", rows, ["schemeId","schemeName","permission","holderType","holderParam"])
    return rows

def collect_workflow_schemes_by_project(project_ids):
    rows = []
    for pid in tqdm(project_ids, desc="Workflow schemes"):
        data = get("/workflowscheme/project", params={"projectId": pid})
        val = (data.get("values") or [{}])[0]
        rows.append({
            "projectId": pid,
            "workflowSchemeId": val.get("workflowSchemeId",""),
            "workflowSchemeName": val.get("workflowSchemeName","")
        })
    write_csv("project_workflow_schemes.csv", rows, ["projectId","workflowSchemeId","workflowSchemeName"])
    return rows

def collect_issue_type_screen_schemes_by_project(project_ids):
    rows = []
    for pid in tqdm(project_ids, desc="Issue type screen schemes"):
        data = get("/issuetypescreenscheme/project", params={"projectId": pid})
        val = (data.get("values") or [{}])[0]
        rows.append({
            "projectId": pid,
            "issueTypeScreenSchemeId": val.get("issueTypeScreenSchemeId",""),
            "issueTypeScreenSchemeName": val.get("issueTypeScreenSchemeName","")
        })
    write_csv("project_issuetype_screen_schemes.csv", rows, ["projectId","issueTypeScreenSchemeId","issueTypeScreenSchemeName"])
    return rows

def collect_field_config_schemes_by_project(project_ids):
    rows = []
    for pid in tqdm(project_ids, desc="Field config schemes"):
        data = get("/fieldconfigurationscheme/project", params={"projectId": pid})
        val = (data.get("values") or [{}])[0]
        rows.append({
            "projectId": pid,
            "fieldConfigurationSchemeId": val.get("fieldConfigurationSchemeId",""),
            "fieldConfigurationSchemeName": val.get("fieldConfigurationSchemeName","")
        })
    write_csv("project_field_config_schemes.csv", rows, ["projectId","fieldConfigurationSchemeId","fieldConfigurationSchemeName"])
    return rows

def collect_screens():
    """
    Robust screens collector:
    - Try v3 PageBean: /rest/api/3/screens -> { values: [...] }
    - Fallback to v2:  /rest/api/2/screens  -> [ ... ]
    - If response is not JSON list/dict, write to screens_raw.json and skip.
    """
    try:
        # Preferred: v3
        data3 = get("/screens", api="3")
        if isinstance(data3, dict) and isinstance(data3.get("values"), list):
            rows = [{"id": s.get("id"), "name": s.get("name", "")} for s in data3["values"] if isinstance(s, dict)]
            write_csv("screens.csv", rows, ["id", "name"])
            return rows
        else:
            write_json("screens_raw.json", data3 if isinstance(data3, (dict, list)) else {"note": "Unexpected v3 shape", "raw": str(data3)})

        # Fallback: v2
        data2 = get("/screens", api="2")
        if isinstance(data2, list):
            rows = [{"id": s.get("id"), "name": s.get("name", "")} for s in data2 if isinstance(s, dict)]
            write_csv("screens.csv", rows, ["id", "name"])
            return rows
        else:
            write_json("screens_raw.json", data2 if isinstance(data2, (dict, list)) else {"note": "Unexpected v2 shape", "raw": str(data2)})
            return []
    except requests.HTTPError as e:
        write_json("screens_error.json", {"error": str(e)})
        return []

def collect_admin_like_groups_and_roles():
    gp = get("/permissions")
    write_json("global_permissions.json", gp)

    groups = []
    start = 0
    while True:
        g = get("/group/bulk", params={"startAt": start, "maxResults": 50})
        groups.extend(g.get("values", []))
        if g.get("isLast", True):
            break
        start += g.get("maxResults", 50)

    write_json("groups_raw.json", groups)
    rows = [{"groupName": g.get("name",""), "groupId": g.get("groupId","")} for g in groups]
    write_csv("groups.csv", rows, ["groupName","groupId"])
    return rows

def collect_apps():
    """
    Tries several Jira Cloud endpoints to list installed apps/add-ons.
    Falls back gracefully and writes whatever it finds to apps_raw.json.
    """
    results = {}
    errors = {}

    endpoints = [
        ("/rest/plugins/1.0/", True),                 # classic plugin manager
        ("/rest/atlassian-connect/1/addons", True),   # Atlassian Connect installed apps
    ]

    for path, is_absolute in endpoints:
        try:
            data = get(path, absolute=is_absolute)
            results[path] = data
        except requests.HTTPError as e:
            errors[path] = str(e)

    if results:
        write_json("apps_raw.json", {"sources": results, "errors": errors} if errors else {"sources": results})
    else:
        write_json("apps_raw.json", {"errors": errors or "No endpoints returned data"})

    return results

def collect_automation_rules_optional():
    try:
        rules = get("/rules", api="automation/1.0")
        flat = []
        for r in rules.get("values", []):
            flat.append({
                "id": r.get("id",""),
                "name": r.get("name",""),
                "enabled": r.get("enabled", True),
                "projects": ",".join([p.get("key","") for p in r.get("projects",[])]),
                "triggersCount": len(r.get("triggers",[])),
                "actionsCount": len(r.get("actions",[])),
                "conditionsCount": len(r.get("conditions",[])),
                "owner": (r.get("creator") or {}).get("displayName","")
            })
        write_csv("automation_rules.csv", flat, ["id","name","enabled","projects","triggersCount","actionsCount","conditionsCount","owner"])
    except requests.HTTPError as e1:
        try:
            rules = get("/rules", api="cb-automation/latest")
            flat = []
            for r in rules.get("values", []):
                flat.append({
                    "id": r.get("id",""),
                    "name": r.get("name",""),
                    "enabled": r.get("enabled", True),
                    "projects": ",".join([p.get("key","") for p in r.get("projects",[])]),
                    "triggersCount": len(r.get("triggers",[])),
                    "actionsCount": len(r.get("actions",[])),
                    "conditionsCount": len(r.get("conditions",[])),
                    "owner": (r.get("creator") or {}).get("displayName","")
                })
            write_csv("automation_rules.csv", flat, ["id","name","enabled","projects","triggersCount","actionsCount","conditionsCount","owner"])
        except requests.HTTPError as e2:
            write_json("automation_rules_error.json", {"error_primary": str(e1), "error_fallback": str(e2)})

# --- Quality checks ---

def quality_checks(projects, last_activity_rows, field_rows):
    # Flag abandoned projects (>180 days no updates)
    last_map = {r["projectKey"]: r["lastIssueUpdated"] for r in last_activity_rows}
    stale = []
    cutoff = datetime.now(timezone.utc) - timedelta(days=180)
    for p in projects:
        key = p["key"]
        lu = last_map.get(key, "")
        if not lu:
            stale.append({"projectKey": key, "reason": "No recent activity / no issues / error"})
            continue
        try:
            dt = isoparse(lu)
            if dt < cutoff:
                stale.append({"projectKey": key, "reason": f"Last issue update {dt.isoformat()}"})
        except Exception:
            stale.append({"projectKey": key, "reason": f"Unparsable date: {lu}"})
    write_csv("flag_abandoned_projects.csv", stale, ["projectKey","reason"])

    # Duplicate field names
    dups = [f for f in field_rows if f["isDuplicateName"]]
    write_csv("flag_duplicate_fields.csv", dups, ["id","name","custom","schema","isDuplicateName"])

# --- Main ---

def main():
    if not (BASE_URL and EMAIL and TOKEN):
        raise SystemExit("Please set JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN environment variables.")

    print("Collecting projects…")
    projects = collect_projects()
    project_ids = [p["id"] for p in projects]

    print("Collecting last activity per project…")
    last_activity = collect_last_activity_by_project(projects)

    print("Collecting fields…")
    fields = collect_fields()

    print("Collecting permission schemes…")
    collect_permission_schemes()

    print("Collecting scheme links per project…")
    collect_workflow_schemes_by_project(project_ids)
    collect_issue_type_screen_schemes_by_project(project_ids)
    collect_field_config_schemes_by_project(project_ids)

    print("Collecting screens…")
    collect_screens()

    print("Collecting groups/admin-like info…")
    collect_admin_like_groups_and_roles()

    print("Collecting apps… (best effort)")
    collect_apps()

    print("Trying to collect automation rules (optional)…")
    collect_automation_rules_optional()

    print("Running quality checks…")
    quality_checks(projects, last_activity, fields)

    readme = {
        "generatedAt": datetime.utcnow().isoformat() + "Z",
        "files": sorted(os.listdir(OUTDIR)),
        "notes": [
            "Use projects.csv + project_last_activity.csv to spot abandoned/duplicate projects.",
            "flag_duplicate_fields.csv highlights custom field name collisions.",
            "permission_schemes.csv surfaces risky role/group holders.",
            "project_*_schemes.csv map each project to its workflow/screen/field-config schemes.",
            "automation_rules.csv is optional—requires Automation API access on your site.",
            "project_last_activity.csv includes a 'note' column for archived/not browsable/error cases."
        ]
    }
    write_json("README.json", readme)
    print(f"Done. See the '{OUTDIR}' folder.")

if __name__ == "__main__":
    main()
