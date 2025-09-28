import sqlite3
import pandas as pd

conn = sqlite3.connect("bottom_pull_requests.db")

# Example: show first 5 rows
df = pd.read_sql_query("SELECT repo_url, html_url,stars FROM bottom_pull_requests;", conn)
print(df)

conn.close()
