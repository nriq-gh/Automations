import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import os

# 🔧 CONFIGURATION
subdomain = 'subdomain' # Your Zendesk subdomain
email = 'email' # Your Zendesk email
api_token = os.getenv("ZENDESK_API_TOKEN")  # Export this variable before running
auth = (f"{email}/token", api_token)

# Paths
timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
base_dir = f"zendesk_export_{timestamp}"
os.makedirs(base_dir, exist_ok=True)
attachments_dir = os.path.join(base_dir, "attachments")
os.makedirs(attachments_dir, exist_ok=True)

# 🚀 FETCH TICKETS

start_time = 0
url = f"https://{subdomain}.zendesk.com/api/v2/incremental/tickets.json?start_time={start_time}"
all_tickets = []
page_count = 0

print("🔄 Fetching all tickets...")
while url:
    try:
        response = requests.get(url, auth=auth)
        response.raise_for_status()
        data = response.json()
    except requests.exceptions.RequestException as e:
        print(f"[❌] Error: {e}. Retrying in 60s...")
        time.sleep(60)
        continue

    tickets = data.get("tickets", [])
    all_tickets.extend(tickets)
    page_count += 1
    print(f"[{datetime.now().isoformat()}] Page {page_count}: Retrieved {len(tickets)} tickets. Total: {len(all_tickets)}")

    if data.get("end_of_stream"):
        print("✅ Reached end of stream.")
        break

    url = data.get("next_page")
    time.sleep(60)  # Respect incremental export rate limit


# 📎 FETCH COMMENTS & DOWNLOAD ATTACHMENTS
ticket_threads = []

print("🔄 Fetching ticket threads and attachments...")
for i, ticket in enumerate(all_tickets, 1):
    ticket_id = ticket["id"]
    comment_url = f"https://{subdomain}.zendesk.com/api/v2/tickets/{ticket_id}/comments.json"
    
    try:
        comment_resp = requests.get(comment_url, auth=auth)
        comment_resp.raise_for_status()
        comment_data = comment_resp.json()
    except requests.exceptions.RequestException as e:
        print(f"[❌] Failed to get comments for Ticket {ticket_id}: {e}")
        continue

    for comment in comment_data.get("comments", []):
        thread = {
            "ticket_id": ticket_id,
            "author_id": comment.get("author_id"),
            "created_at": comment.get("created_at"),
            "body": comment.get("body"),
            "via": comment.get("via", {}).get("channel"),
            "public": comment.get("public")
        }

        # Download attachments
        attachments = comment.get("attachments", [])
        for attachment in attachments:
            file_url = attachment.get("content_url")
            file_name = f"{ticket_id}_{attachment.get('file_name')}"
            file_path = os.path.join(attachments_dir, file_name)

            try:
                file_resp = requests.get(file_url, auth=auth)
                file_resp.raise_for_status()
                with open(file_path, "wb") as f:
                    f.write(file_resp.content)
                print(f"📥 Downloaded: {file_name}")
            except Exception as e:
                print(f"[⚠️] Failed to download attachment {file_name}: {e}")
            
            # Include last attachment in thread record
            thread["attachment_name"] = file_name
            thread["attachment_url"] = file_url
        
        ticket_threads.append(thread)

    # Cooldown to avoid exceeding 60 requests/min
    time.sleep(1)

    # Extra cooldown safety every 100 tickets
    if i % 100 == 0:
        print(f"⏳ Reached {i} tickets, pausing for 60 seconds to avoid rate limits...")
        time.sleep(60)

    if i % 10 == 0:
        print(f"📌 Processed {i}/{len(all_tickets)} tickets")


# 💾 SAVE FILES

# Save tickets
tickets_df = pd.json_normalize(all_tickets)
tickets_df.to_csv(os.path.join(base_dir, "tickets.csv"), index=False)

# Save threads
threads_df = pd.DataFrame(ticket_threads)
threads_df.to_csv(os.path.join(base_dir, "ticket_threads.csv"), index=False)

print(f"\n✅ All done! Files saved in: {base_dir}")
