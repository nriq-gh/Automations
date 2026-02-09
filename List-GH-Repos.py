import requests
from datetime import datetime, timedelta

# Replace with your GitHub organization name and personal access token
ORG_NAME = 'test_org'
ACCESS_TOKEN = 'token'

# Calculate the date 12 months ago from today
twelve_months_ago = datetime.now() - timedelta(days=365)
twelve_months_ago_str = twelve_months_ago.strftime('%Y-%m-%dT%H:%M:%SZ')

# GitHub API endpoint to list repositories in an organization
api_url = f'https://api.github.com/orgs/{ORG_NAME}/repos'

# Parameters for the API request
params = {
    'type': 'all',  # Include all types of repositories (public, private, forks, sources, etc.)
    'per_page': 100  # Number of results per page (maximum 100)
}

headers = {
    'Authorization': f'token {ACCESS_TOKEN}'
}

def get_inactive_repos():
    inactive_repos = []
    page = 1
    while True:
        response = requests.get(api_url, headers=headers, params={**params, 'page': page})
        response_data = response.json()

        if response.status_code != 200 or not response_data:
            break

        for repo in response_data:
            last_updated = repo['updated_at']
            if last_updated < twelve_months_ago_str:
                inactive_repos.append({
                    'name': repo['name'],
                    'updated_at': repo['updated_at']
                })

        page += 1

    return inactive_repos

def main():
    inactive_repos = get_inactive_repos()
    if inactive_repos:
        print(f"Found {len(inactive_repos)} inactive repositories:")
        for repo in inactive_repos:
            print(f"- {repo['name']} (last updated: {repo['updated_at']})")
    else:
        print("No inactive repositories found.")

if __name__ == "__main__":
    main()
