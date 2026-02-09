#!/usr/bin/env python3
"""
Bulk deactivate Slack users from a CSV that contains a column named 'email'.

Requires:
  pip install requests python-dotenv

Environment variables:
  SLACK_ADMIN_TOKEN   # xoxp-... token with admin scopes for Enterprise Grid
  SLACK_TEAM_ID       # (optional) workspace ID if needed in your org flows
"""

import os
import csv
import time
import argparse
from typing import Optional, Dict, Any

import requests

SLACK_API_BASE = "https://slack.com/api"


def slack_api(
    method: str,
    token: str,
    endpoint: str,
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
) -> Dict[str, Any]:
    """
    Call Slack Web API with basic rate-limit handling (HTTP 429) and retries.
    """
    url = f"{SLACK_API_BASE}/{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=utf-8",
    }

    for attempt in range(max_retries + 1):
        resp = requests.request(method, url, headers=headers, params=params, json=json_body, timeout=30)

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "1"))
            time.sleep(retry_after)
            continue

        data = resp.json()

        # Slack sometimes returns ok=false with "ratelimited" even if not 429
        if not data.get("ok") and data.get("error") == "ratelimited":
            time.sleep(2 + attempt)
            continue

        return data

    return {"ok": False, "error": "max_retries_exceeded"}


def lookup_user_id_by_email(token: str, email: str) -> Optional[str]:
    """
    users.lookupByEmail requires scope: users:read.email (and appropriate token type).
    On Enterprise Grid admin flows, this may still work; otherwise you might need SCIM.
    """
    data = slack_api("GET", token, "users.lookupByEmail", params={"email": email})
    if data.get("ok"):
        return data["user"]["id"]
    return None


def deactivate_user(token: str, user_id: str) -> Dict[str, Any]:
    """
    Deactivate a user. This endpoint is Enterprise Grid admin-only.
    admin.users.remove requires admin scope and an appropriate token.
    """
    return slack_api("POST", token, "admin.users.remove", json_body={"user_id": user_id})


def read_emails_from_csv(path: str) -> list[str]:
    emails: list[str] = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if "email" not in (reader.fieldnames or []):
            raise ValueError(f"CSV must include an 'email' column. Found: {reader.fieldnames}")
        for row in reader:
            email = (row.get("email") or "").strip()
            if email:
                emails.append(email)
    return emails


def main():
    parser = argparse.ArgumentParser(description="Bulk deactivate Slack users from CSV (column: email).")
    parser.add_argument("--csv", required=True, help="Path to input CSV containing 'email' column.")
    parser.add_argument("--out", default="slack_deactivation_results.csv", help="Path to output results CSV.")
    parser.add_argument("--dry-run", action="store_true", help="Do not deactivate; only report what would happen.")
    parser.add_argument("--sleep", type=float, default=0.5, help="Delay between users (helps avoid rate limits).")
    args = parser.parse_args()

    token = os.getenv("SLACK_ADMIN_TOKEN")
    if not token:
        raise SystemExit("Missing env var SLACK_ADMIN_TOKEN")

    emails = read_emails_from_csv(args.csv)

    results = []
    for email in emails:
        print(f"Processing: {email}")

        user_id = lookup_user_id_by_email(token, email)
        if not user_id:
            results.append(
                {"email": email, "user_id": "", "action": "lookup", "status": "failed", "error": "user_not_found"}
            )
            time.sleep(args.sleep)
            continue

        if args.dry_run:
            results.append({"email": email, "user_id": user_id, "action": "deactivate", "status": "skipped", "error": ""})
            time.sleep(args.sleep)
            continue

        resp = deactivate_user(token, user_id)
        if resp.get("ok"):
            results.append({"email": email, "user_id": user_id, "action": "deactivate", "status": "success", "error": ""})
        else:
            results.append(
                {
                    "email": email,
                    "user_id": user_id,
                    "action": "deactivate",
                    "status": "failed",
                    "error": resp.get("error", "unknown_error"),
                }
            )

        time.sleep(args.sleep)

    # Write output CSV
    with open(args.out, "w", newline="", encoding="utf-8") as f:
        fieldnames = ["email", "user_id", "action", "status", "error"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\nDone. Results written to: {args.out}")


if __name__ == "__main__":
    main()
