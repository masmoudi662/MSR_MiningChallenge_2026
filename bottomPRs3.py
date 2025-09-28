import pandas as pd
import sqlite3

HF_ALL = "hf://datasets/hao-li/AIDev/all_pull_request.parquet"

# Load dataset
print("Loading dataset...")
all_pr_df = pd.read_parquet(HF_ALL, engine="pyarrow")
print(f"Dataset loaded with {len(all_pr_df):,} rows and {len(all_pr_df.columns)} columns.")

# Connect to SQLite (creates file if it doesn’t exist)
db_file = "all_pull_requests.db"
print(f"Connecting to database: {db_file}")
conn = sqlite3.connect(db_file)

# Export with chunks and live progress
print("Exporting to SQLite...")
chunksize = 50000  # adjust as needed
total_rows = len(all_pr_df)

for i in range(0, total_rows, chunksize):
    chunk = all_pr_df.iloc[i:i+chunksize]
    chunk.to_sql("pull_requests", conn, if_exists="append", index=False)
    print(f"Inserted rows {i+1:,} to {min(i+chunksize, total_rows):,} / {total_rows:,}")

print("Export complete ✅")

# Quick verification
cursor = conn.cursor()
cursor.execute("SELECT COUNT(*) FROM pull_requests;")
print("Total rows stored in DB:", cursor.fetchone()[0])

conn.close()
print("Database connection closed.")
