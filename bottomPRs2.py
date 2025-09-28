import pandas as pd
import requests
import time

HF_ALL = "hf://datasets/hao-li/AIDev/all_pull_request.parquet"

# Load dataset
all_pr_df = pd.read_parquet(HF_ALL, engine="pyarrow")

# Deduplicate repositories (many PRs may belong to the same repo)
unique_repos = all_pr_df["repo_url"].dropna().unique()

def get_star_count(repo_url, token=None):
    owner_repo = "/".join(repo_url.split("/")[-2:])
    api_url = f"https://api.github.com/repos/{owner_repo}"
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    resp = requests.get(api_url, headers=headers)
    if resp.status_code == 200:
        return resp.json().get("stargazers_count", None)
    else:
        return None

# Example: collect star counts for first 50 repos (avoid rate limits for demo)
repo_to_stars = {}
for repo_url in unique_repos[:50]:
    stars = get_star_count(repo_url)
    repo_to_stars[repo_url] = stars
    print(f"{repo_url}: {stars}")
    time.sleep(1)  # avoid hitting API limits (esp. without token)

# Map star counts back into the PR DataFrame
all_pr_df["stars"] = all_pr_df["repo_url"].map(repo_to_stars)

# Sort PRs by stars
sorted_prs = all_pr_df.sort_values("stars", ascending=True)

# Take bottom 200–400 PRs
bottom_prs = sorted_prs.head(400)

print(bottom_prs[["repo_url", "stars", "html_url"]])
