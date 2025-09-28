import pandas as pd
import requests

HF_ALL = "hf://datasets/hao-li/AIDev/all_pull_request.parquet"

# Load dataset
all_pr_df = pd.read_parquet(HF_ALL, engine="pyarrow")

# Pick one PR (example: row 800000)
row = all_pr_df.iloc[800000]
repo_url = row["repo_url"]
html_url= row["html_url"]

# Extract owner/repo from the repo_url
# repo_url looks like "https://github.com/owner/repo"
owner_repo = "/".join(repo_url.split("/")[-2:])

# GitHub API endpoint
api_url = f"https://api.github.com/repos/{owner_repo}"

# Optional: use token to avoid rate limits
headers = {"Accept": "application/vnd.github+json"}
# headers["Authorization"] = "Bearer YOUR_GITHUB_TOKEN"   # if you have a token

response = requests.get(api_url, headers=headers)

if response.status_code == 200:
    data = response.json()
    print(f"Repo: {repo_url}")
    print(f"Repo Stars: {data['stargazers_count']}")
    print(f"url: {html_url}")
else:
    print(f"Failed to fetch data for {owner_repo}, status: {response.status_code}")
