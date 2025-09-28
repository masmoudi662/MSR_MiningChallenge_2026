import pandas as pd
df = pd.read_parquet("hf://datasets/stanfordnlp/imdb/plain_text/train-00000-of-00001.parquet")

HF_ALL = "hf://datasets/hao-li/AIDev/all_pull_request.parquet"
HF_POP = "hf://datasets/hao-li/AIDev/pull_request.parquet" # popular ones (AIDev-pop)

# Load from Hugging Face parquet
all_pr_df = pd.read_parquet(HF_ALL, engine="pyarrow")
pop_pr_df = pd.read_parquet(HF_POP, engine="pyarrow") # popular subset

def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure datatime columns are parsed and basic fields normalized."""
    df = df.copy()

    # Robust parsing (handles strings like "None" or actual None)
    for col in ["created_at", "closed_at", "merged_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Normalize state (lowercase)
    if "state" in df.columns:
        df["state"] = df["state"].astype(str).str.lower()

    # Ensure id is string for safe de-dup and merge logic
    if "id" in df.columns:
        df["id"] = df["id"].astype(str)


    return df

all_pr_df = preprocess(all_pr_df)

print(all_pr_df)

#print(all_pr_df.iloc[800000])
