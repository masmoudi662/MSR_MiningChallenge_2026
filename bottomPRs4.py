import pandas as pd
import sqlite3
import requests
import time

HF_ALL = "hf://datasets/hao-li/AIDev/all_pull_request.parquet"

print("Loading dataset...")
all_pr_df = pd.read_parquet(HF_ALL, engine="pyarrow")
print(f"Loaded {len(all_pr_df):,} PRs.")

# Step 1: Deduplicate repo URLs
unique_repos = all_pr_df["repo_url"].dropna().unique()
print(f"Found {len(unique_repos):,} unique repositories.")

# GitHub API helper
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

# Step 2: Fetch stars (demo: only first 100 repos to avoid rate limit)
repo_to_stars = {}
for idx, repo_url in enumerate(unique_repos[:100], 1):  # adjust slice later
    stars = get_star_count(repo_url)
    repo_to_stars[repo_url] = stars
    print(f"[{idx}/{len(unique_repos[:100])}] {repo_url} → {stars} stars")
    time.sleep(1)  # avoid rate limits

# Step 3: Attach stars back
all_pr_df["stars"] = all_pr_df["repo_url"].map(repo_to_stars)

# Step 4: Sort ascending and keep bottom 400 PRs
bottom_prs = all_pr_df.sort_values("stars", ascending=True).head(400)

print("Selected bottom 400 PRs based on star count:")
print(bottom_prs[["repo_url", "stars", "html_url"]].head(10))  # preview first 10

# Step 5: Save to SQLite
db_file = "bottom_pull_requests.db"
print(f"Saving bottom 400 PRs to {db_file}...")
conn = sqlite3.connect(db_file)

bottom_prs.to_sql("bottom_pull_requests", conn, if_exists="replace", index=False)
print("Export complete ✅")

conn.close()
