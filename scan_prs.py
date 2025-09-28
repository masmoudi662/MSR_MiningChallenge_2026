import os
import sqlite3
import subprocess
import json
import csv
import shutil

# --------------------------
# CONFIG
# --------------------------
DB_FILE = "bottom_pull_requests.db"
OUTPUT_CSV = "results.csv"
WORK_DIR = "repos"  # all repos cloned here

# Ensure working dir exists
os.makedirs(WORK_DIR, exist_ok=True)

# --------------------------
# HELPER FUNCTIONS
# --------------------------
def run_cmd(cmd, cwd=None):
    """Run shell command and return (stdout, stderr)."""
    result = subprocess.run(cmd, cwd=cwd, shell=True,
                            capture_output=True, text=True)
    return result.stdout.strip(), result.stderr.strip()

def safe_json_load(path):
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return json.load(f)
    except Exception:
        return None

def summarize_semgrep(path):
    data = safe_json_load(path)
    if not data or "results" not in data:
        return (0, 0)
    total = len(data["results"])
    high = sum(1 for r in data["results"]
               if (r.get("extra", {}).get("severity", "").lower()
                   in {"error", "high", "critical"}))
    return (total, high)

def summarize_bandit(path):
    data = safe_json_load(path)
    if not data or "results" not in data:
        return (0, 0)
    total = len(data["results"])
    high = sum(1 for r in data["results"]
               if r.get("issue_severity", "").upper() == "HIGH")
    return (total, high)

def summarize_eslint(path):
    data = safe_json_load(path)
    if not isinstance(data, list):
        return (0, 0)
    total = 0
    high = 0
    for file_report in data:
        for msg in file_report.get("messages", []):
            total += 1
            if str(msg.get("ruleId", "")).startswith("security/") or msg.get("severity", 0) == 2:
                high += 1
    return (total, high)

# --------------------------
# MAIN PIPELINE
# --------------------------
def main():
    # Connect to DB
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Adjust table/column names if needed
    cur.execute("SELECT owner, repo, pr_number, language, agent FROM pull_requests LIMIT 400;")
    prs = cur.fetchall()
    conn.close()

    # Prepare CSV
    write_header = not os.path.exists(OUTPUT_CSV)
    with open(OUTPUT_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow([
                "pr_id", "repo", "agent", "language",
                "semgrep_total", "semgrep_high",
                "bandit_total", "bandit_high",
                "eslint_total", "eslint_high"
            ])

        # Process each PR
        for i, (owner, repo, pr_number, language, agent) in enumerate(prs, start=1):
            pr_id = f"{owner}/{repo}#{pr_number}"
            print(f"\n[{i}/{len(prs)}] Scanning {pr_id} ...")

            # Repo folder
            repo_dir = os.path.join(WORK_DIR, f"{owner}_{repo}_{pr_number}")
            if os.path.exists(repo_dir):
                shutil.rmtree(repo_dir)
            os.makedirs(repo_dir, exist_ok=True)

            # Clone + checkout PR
            clone_cmd = f"git clone https://github.com/{owner}/{repo}.git ."
            out, err = run_cmd(clone_cmd, cwd=repo_dir)
            if err:
                print("Clone error:", err)
                continue
            fetch_cmd = f"git fetch origin pull/{pr_number}/head:pr-{pr_number}"
            run_cmd(fetch_cmd, cwd=repo_dir)
            checkout_cmd = f"git checkout pr-{pr_number}"
            run_cmd(checkout_cmd, cwd=repo_dir)

            # Run scanners
            semgrep_cmd = f"semgrep --config p/security-audit --config p/secrets --json -o semgrep.json ."
            bandit_cmd = f"bandit -r . -f json -o bandit.json"
            eslint_cmd = f"npx eslint . --ext .js,.ts -f json -o eslint.json || true"

            run_cmd(semgrep_cmd, cwd=repo_dir)
            run_cmd(bandit_cmd, cwd=repo_dir)
            run_cmd(eslint_cmd, cwd=repo_dir)

            # Summarize results
            semgrep_total, semgrep_high = summarize_semgrep(os.path.join(repo_dir, "semgrep.json"))
            bandit_total, bandit_high = summarize_bandit(os.path.join(repo_dir, "bandit.json"))
            eslint_total, eslint_high = summarize_eslint(os.path.join(repo_dir, "eslint.json"))

            # Write row
            writer.writerow([
                pr_number,
                f"{owner}/{repo}",
                agent,
                language,
                semgrep_total, semgrep_high,
                bandit_total, bandit_high,
                eslint_total, eslint_high
            ])

            print(f"Done {pr_id} → S:{semgrep_total}/{semgrep_high}, B:{bandit_total}/{bandit_high}, E:{eslint_total}/{eslint_high}")

    print(f"\n✅ All results saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
