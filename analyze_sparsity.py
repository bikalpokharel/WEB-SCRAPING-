import pandas as pd
import os

DATA_DIR = "/Users/bikal/Library/CloudStorage/OneDrive-Personal/Nepal_Job_Market_Live_Data/xlsx"

FILES = {
    "merojob": os.path.join(DATA_DIR, "merojob_jobs.xlsx"),
    "jobsnepal": os.path.join(DATA_DIR, "jobsnepal_jobs.xlsx"),
    "linkedin": os.path.join(DATA_DIR, "linkedin_jobs.xlsx"),
}

OUTPUT_FILE = os.path.join(DATA_DIR, "sparsity_comparison.xlsx")

summary_rows = []

for portal, path in FILES.items():

    if not os.path.exists(path):
        print(f"❌ File not found: {portal}")
        continue

    df = pd.read_excel(path)

    # Treat placeholders as missing
    PLACEHOLDER_COLS = [
    "company", "company_link", "location", "posted_date", "num_applicants",
    "work_mode", "employment_type", "position", "type", "compensation",
    "commitment", "skills", "category_primary"
]

for c in PLACEHOLDER_COLS:
    if c in df.columns:
        df[c] = df[c].replace(["Non", "non", ""], pd.NA)


    rows = df.shape[0]
    cols = df.shape[1]

    total_cells = rows * cols
    total_missing = df.isna().sum().sum()
    overall_sparsity = (total_missing / total_cells) * 100 if total_cells else 0

    # Columns >70% missing
    column_missing_percent = df.isna().mean() * 100
    high_sparse_count = (column_missing_percent > 70).sum()

    summary_rows.append({
        "portal": portal,
        "rows": rows,
        "columns": cols,
        "overall_sparsity_%": round(overall_sparsity, 2),
        "columns_above_70%_missing": int(high_sparse_count)
    })

summary_df = pd.DataFrame(summary_rows)

print("\n📊 SPARSITY COMPARISON TABLE")
print("="*50)
print(summary_df)

summary_df.to_excel(OUTPUT_FILE, index=False)

print("\n✅ Comparison table saved to:")
print(OUTPUT_FILE)

print("\n📌 Column-wise Missing % for:", portal)
print(column_missing_percent.sort_values(ascending=False))
