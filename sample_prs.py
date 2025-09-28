import pandas as pd

# Load pull request metadata
prs = pd.read_csv("prs.csv")

# Filter: worst PRs (not merged OR many comments OR oversized)
worst = prs[(prs["merged"] == False) |
            (prs["review_comments_count"] > 10) |
            (prs["additions"] > 500)]

# Keep only interesting columns
worst = worst[["pr_id","repo","agent","lang","semgrep_high","bandit_total",
               "bandit_high","eslint_total","eslint_high"]]

# Sample 300 PRs (adjust size as needed)
sample = worst.sample(300, random_state=42)
sample.to_csv("prs.csv", index=False)

print("Saved prs.csv with", len(sample), "PRs")
