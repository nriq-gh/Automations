#!/usr/bin/env python3
"""
Bulk deactivate Slack users via Slack SCIM API (v1), using SLACK_TOKEN env var.

Changes in this version:
- EXACT email matching only:
  - A user is considered a match only if the input email equals either:
    1) user.emails[].value (case-insensitive), or
    2) user.userName (case-insensitive) if your org uses email-as-userName
- Adds "similar_matches" column in the report:
  - If no exact match is found, we do a safe "similar search" (by local-part)
    and list up to N candidate users (id + userName + primary email if present)
  - This helps you understand what Slack returns without accidentally matching wrong users.

Input:
- CSV file with header 'email' (required)
- Optional column: scim_id (if you already know it)

Usage:
  export SLACK_TOKEN="xoxp-..."
  python disable_slack_accounts.py --csv users.csv --dry-run
  python disable_slack_accounts.py --csv users.csv
"""

import argparse
import csv
import os
import time
from typing import Optional, Dict, Any, Tuple, List

import requests

SCIM_BASE = "https://api.slack.com/scim/v1/"


def get_token() -> str:
    token = os.environ.get("SLACK_TOKEN", "").strip()
    if not token:
        raise SystemExit("ERROR: SLACK_TOKEN environment variable is not set.")
    return token


def scim_request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    json_body: Optional[Dict[str, Any]] = None,
    max_retries: int = 6,
) -> Tuple[int, Dict[str, Any] | str, requests.Response]:
    last_resp: Optional[requests.Response] = None
    for attempt in range(max_retries):
        resp = session.request(method, url, json=json_body, timeout=30)
        last_resp = resp

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else 5
            time.sleep(sleep_s)
            continue

        if 500 <= resp.status_code <= 599:
            time.sleep(min(2 ** attempt, 20))
            continue

        try:
            return resp.status_code, resp.json(), resp
        except Exception:
            return resp.status_code, resp.text, resp

    if last_resp is None:
        raise SystemExit("ERROR: Request failed without a response.")
    return 599, "Max retries exceeded", last_resp


def assert_scim_access(session: requests.Session, base_url: str) -> None:
    # Real auth check: /Users is protected
    url = f"{base_url}Users?count=1"
    code, data, _ = scim_request(session, "GET", url)
    if code == 401:
        raise SystemExit(
            "ERROR: 401 invalid_authentication calling SCIM /Users.\n"
            "Your SLACK_TOKEN is not authorized for Slack SCIM user management."
        )
    if code == 403:
        raise SystemExit(
            "ERROR: 403 forbidden calling SCIM /Users.\n"
            "Token is valid but lacks required privileges/scopes for SCIM."
        )
    if code >= 400:
        raise SystemExit(f"ERROR: SCIM auth check failed (HTTP {code}): {data}")


def read_rows_from_csv(path: str) -> List[dict]:
    if not os.path.exists(path):
        raise SystemExit(f"ERROR: file not found: {path}")

    rows: List[dict] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit("ERROR: CSV has no headers.")

        headers = [h.strip().lower() for h in reader.fieldnames]
        if "email" not in headers:
            raise SystemExit("ERROR: CSV must include an 'email' column header.")

        for r in reader:
            rr = {k.strip().lower(): (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
            if rr.get("email"):
                rows.append(rr)

    return rows


def extract_primary_email(user: dict) -> str:
    """
    Best effort: return the first email value, prefer primary=true if present.
    """
    emails = user.get("emails") or []
    primary = ""
    first = ""
    for e in emails:
        val = str(e.get("value", "")).strip()
        if not val:
            continue
        if not first:
            first = val
        if e.get("primary") is True:
            primary = val
    return primary or first


def user_matches_email_exact(user: dict, email: str) -> bool:
    """
    Exact match if email equals userName OR any emails[].value (case-insensitive).
    """
    target = email.lower().strip()
    if str(user.get("userName", "")).lower().strip() == target:
        return True
    for e in (user.get("emails") or []):
        if str(e.get("value", "")).lower().strip() == target:
            return True
    return False


def list_similar_candidates(session: requests.Session, base_url: str, email: str, limit: int = 10) -> str:
    """
    Fetch candidates by local-part and return a compact string for the report.
    This is informational only; it does NOT cause a "found" match.
    """
    local = email.split("@", 1)[0]
    url = f'{base_url}Users?filter=userName%20co%20%22{local}%22&count=100'
    code, data, _ = scim_request(session, "GET", url)
    if code != 200 or not isinstance(data, dict):
        return f"similar_search_failed_http_{code}"

    candidates = []
    for u in (data.get("Resources") or []):
        scim_id = str(u.get("id", "")).strip()
        uname = str(u.get("userName", "")).strip()
        pemail = extract_primary_email(u)
        # show as: id|userName|primaryEmail
        candidates.append(f"{scim_id}|{uname}|{pemail}")

    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for c in candidates:
        if c in seen:
            continue
        seen.add(c)
        uniq.append(c)

    return "; ".join(uniq[:limit]) if uniq else ""


def find_user_by_email_exact(
    session: requests.Session,
    base_url: str,
    email: str,
) -> Tuple[Optional[str], str, Optional[bool], Optional[str], str]:
    """
    Returns: (scim_id, note, active, matched_userName, similar_matches)

    Strategy:
    1) Try direct filter on userName eq "email" (fast if org uses email-as-userName)
    2) If not found, do a safe candidate search by local-part and then enforce exact email
       match against emails[].value/userName in returned users.
    3) If still not found, return similar_matches for debugging/reporting.
    """
    # 1) Fast path: exact userName == email
    url = f'{base_url}Users?filter=userName%20eq%20%22{email}%22'
    code, data, _ = scim_request(session, "GET", url)
    if code != 200 or not isinstance(data, dict):
        sim = list_similar_candidates(session, base_url, email)
        return None, f"lookup_failed_http_{code}", None, None, sim

    if data.get("totalResults", 0) > 0:
        u = (data.get("Resources") or [{}])[0] or {}
        # still enforce exact match logic (just in case)
        if user_matches_email_exact(u, email):
            return u.get("id"), "found_exact", u.get("active"), u.get("userName"), ""
        # If weird mismatch, treat as not found but include candidates
        sim = list_similar_candidates(session, base_url, email)
        return None, "no_exact_email_match", None, None, sim

    # 2) Candidate search by local-part, then enforce exact email match
    local = email.split("@", 1)[0]
    url = f'{base_url}Users?filter=userName%20co%20%22{local}%22&count=100'
    code, data, _ = scim_request(session, "GET", url)
    if code != 200 or not isinstance(data, dict):
        return None, f"fallback_failed_http_{code}", None, None, ""

    for u in (data.get("Resources") or []):
        if user_matches_email_exact(u, email):
            return u.get("id"), "found_exact", u.get("active"), u.get("userName"), ""

    # 3) Not found: return similar candidates for visibility
    sim = list_similar_candidates(session, base_url, email)
    return None, "user_not_found", None, None, sim


def deactivate_user(session: requests.Session, base_url: str, scim_id: str) -> Tuple[bool, str]:
    url = f"{base_url}Users/{scim_id}"
    patch_body = {"active": False}
    code, data, _ = scim_request(session, "PATCH", url, json_body=patch_body)
    if code in (200, 204):
        return True, "deactivated"
    return False, f"deactivate_failed_http_{code}:{data}"


def main() -> None:
    ap = argparse.ArgumentParser(description="Bulk deactivate Slack users via SCIM v1 (exact email match).")
    ap.add_argument("--csv", required=True, help="CSV path with header 'email' (optional 'scim_id')")
    ap.add_argument("--out", default="slack_disable_report.csv", help="Output CSV report path")
    ap.add_argument("--dry-run", action="store_true", help="Only lookup; do not deactivate")
    ap.add_argument("--skip-already-inactive", action="store_true", help="Skip if active=false (when known)")
    ap.add_argument("--similar-limit", type=int, default=8, help="Max similar candidates to include in report")
    args = ap.parse_args()

    token = get_token()

    session = requests.Session()
    session.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    })

    assert_scim_access(session, SCIM_BASE)

    rows = read_rows_from_csv(args.csv)
    report: List[dict] = []

    for r in rows:
        email = (r.get("email") or "").strip()
        provided_scim_id = (r.get("scim_id") or "").strip()
        scim_id: Optional[str] = provided_scim_id if provided_scim_id else None

        note = ""
        active: Optional[bool] = None
        matched_userName: Optional[str] = None
        similar_matches: str = ""

        if not scim_id:
            scim_id, note, active, matched_userName, similar_matches = find_user_by_email_exact(
                session, SCIM_BASE, email
            )
            # Re-limit similar output size (find_user_by_email_exact may return many)
            if similar_matches and args.similar_limit > 0:
                parts = [p.strip() for p in similar_matches.split(";") if p.strip()]
                similar_matches = "; ".join(parts[: args.similar_limit])

            if not scim_id:
                report.append({
                    "email": email,
                    "scim_id": "",
                    "matched_userName": "",
                    "action": "lookup",
                    "status": "failed",
                    "error": note,
                    "was_active": "" if active is None else str(active).lower(),
                    "similar_matches": similar_matches,
                })
                continue
        else:
            note = "provided_scim_id"

        if args.dry_run:
            report.append({
                "email": email,
                "scim_id": scim_id,
                "matched_userName": matched_userName or "",
                "action": "dry_run",
                "status": "ok",
                "error": "",
                "was_active": "" if active is None else str(active).lower(),
                "similar_matches": similar_matches,
            })
            continue

        if args.skip_already_inactive and active is False:
            report.append({
                "email": email,
                "scim_id": scim_id,
                "matched_userName": matched_userName or "",
                "action": "skip",
                "status": "ok",
                "error": "already_inactive",
                "was_active": "false",
                "similar_matches": similar_matches,
            })
            continue

        ok, msg = deactivate_user(session, SCIM_BASE, scim_id)
        report.append({
            "email": email,
            "scim_id": scim_id,
            "matched_userName": matched_userName or "",
            "action": "deactivate",
            "status": "ok" if ok else "failed",
            "error": "" if ok else msg,
            "was_active": "" if active is None else str(active).lower(),
            "similar_matches": similar_matches,
        })

    with open(args.out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["email", "scim_id", "matched_userName", "action", "status", "error", "was_active", "similar_matches"],
        )
        writer.writeheader()
        writer.writerows(report)

    print(f"Done. Report written to: {args.out}")


if __name__ == "__main__":
    main()
