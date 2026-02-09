import requests

# Replace with your GitHub personal access token
GITHUB_TOKEN = "token"

# GitHub API URL for creating an org repo
ORG_NAME = "Test_org"
REPO_NAME = "automate-test"
GITHUB_API_URL = f"https://api.github.com/orgs/{ORG_NAME}/repos"

# Headers for authentication
headers = {
    "Authorization": f"token {GITHUB_TOKEN}",
    "Accept": "application/vnd.github.v3+json"
}

# Payload to create the repo
payload = {
    "name": REPO_NAME,
    "private": True,  # Ensure the repo is private
    "description": "Automated test repo created via API",
    "auto_init": True  # Automatically initialize with README
}

# Make the request
response = requests.post(GITHUB_API_URL, json=payload, headers=headers)

# Check response
if response.status_code == 201:
    print(f"Repository '{REPO_NAME}' created successfully!")
    print(f"URL: {response.json().get('html_url')}")
else:
    print(f"Error: {response.status_code} - {response.json().get('message')}")
