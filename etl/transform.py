from pathlib import Path
import pandas as pd

# --- Paths (hard-coded) ---
data_path = Path("/home/alyona/personal_projects/foil_case_study/data/online_retail_raw.csv")
transactions_output_path = Path("/home/alyona/personal_projects/foil_case_study/data/transactions.csv")
cancellations_output_path = Path("/home/alyona/personal_projects/foil_case_study/data/cancellations.csv")

# --- Read CSV, force InvoiceNo as string to preserve 'C' prefix ---
df = pd.read_csv(data_path, dtype={"InvoiceNo": str})
print("Initial number of records:", len(df))

# --- Cleaning Steps ---
# Remove duplicates
df = df.drop_duplicates()
df.columns = df.columns.str.strip()

# Remove rows with missing Description or CustomerID
df = df[df["Description"].notna()]
df = df[df["CustomerID"].notna()]

# Keep only StockCode that are 5-digit numbers
df = df[df["StockCode"].astype(str).str.match(r"^\d{5}$")]

# Keep only Description containing letters
df = df[df["Description"].str.contains(r"[a-zA-Z]", na=False)]

# Convert InvoiceDate to datetime, drop invalid
df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
df = df[df["InvoiceDate"].notna()]

# Keep only CustomerID that are 5-digit numbers
df = df[df["CustomerID"].between(10000, 99999)]
df["CustomerID"] = df["CustomerID"].astype(int).astype(str)  # convert to string categorical

# Remove rows with missing Country
df = df[df["Country"].notna()]

# Keep only valid InvoiceNo (6-digit numbers, optionally prefixed with 'C')
df = df[df["InvoiceNo"].str.match(r"^C?\d{6}$")]

# --- Split transactions vs cancellations ---
transactions_df = df[~df["InvoiceNo"].str.startswith("C")].copy()
cancellations_df = df[df["InvoiceNo"].str.startswith("C")].copy()

# Optional: remove negative Quantity/UnitPrice only from transactions
transactions_df = transactions_df[(transactions_df["Quantity"] > 0) & (transactions_df["UnitPrice"] > 0)]

# --- Save cleaned datasets ---
transactions_df.to_csv(transactions_output_path, index=False)
cancellations_df.to_csv(cancellations_output_path, index=False)

print(f"Transactions dataset saved to: {transactions_output_path}, rows: {len(transactions_df)}")
print(f"Cancellations dataset saved to: {cancellations_output_path}, rows: {len(cancellations_df)}")