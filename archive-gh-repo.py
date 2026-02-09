import requests

TOKEN = "your_github_token"
headers = {
    "Authorization": f"token {TOKEN}",
    "Accept": "application/vnd.github+json"
}

repos = [
    "your-org/repo1",
    "your-org/repo2",
    "your-org/repo3"
]

for repo in repos:
    url = f"https://api.github.com/repos/{repo}"
    response = requests.patch(url, headers=headers, json={"archived": True})
    if response.status_code == 200:
        print(f"Archived {repo}")
    else:
        print(f"Failed to archive {repo}: {response.status_code}")
