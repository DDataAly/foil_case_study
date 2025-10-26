from pathlib import Path
import pandas as pd
import yaml

# --- 1️⃣ Load config ---
def load_config(config_path: Path = Path(__file__).resolve().parents[1] / "config.yaml"):
    with open(config_path, "r") as f:
        return yaml.safe_load(f)

# --- 2️⃣ Load CSVs from transform.py outputs ---
def load_transform_outputs(paths):
    transactions = pd.read_csv(paths["transactions"], dtype={"InvoiceNo": str, "CustomerID": str})
    cancellations = pd.read_csv(paths["cancellations"], dtype={"InvoiceNo": str, "CustomerID": str})
    outliers = pd.read_csv(paths["outliers"], dtype={"InvoiceNo": str, "CustomerID": str})
    return transactions, cancellations, outliers

# --- 3️⃣ Merge and prepare fact table ---
def prepare_fact_table(transactions: pd.DataFrame, cancellations: pd.DataFrame) -> pd.DataFrame:
    transactions = transactions.copy()
    cancellations = cancellations.copy()

    transactions["transaction_type"] = "SALE"
    transactions["sign"] = 1

    cancellations["transaction_type"] = "CANCEL"
    cancellations["sign"] = -1

    fact_table = pd.concat([transactions, cancellations], ignore_index=True)

    # Rename InvoiceDate column to match fact table schema - not necessary but reduces potential errors and not expensive

    fact_table.rename(columns={"InvoiceDate": "transaction_datetime"}, inplace=True)

    return fact_table

# --- 4️⃣ Save DataFrame to Parquet ---
def save_parquet(df: pd.DataFrame, output_dir: Path, filename: str):
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename
    df.to_parquet(output_path, index=False)
    print(f"✅ Saved {filename} with {len(df):,} rows → {output_path}")

# --- 5️⃣ Orchestration ---
def main():
    config = load_config()
    paths = config["paths"]

    # Load outputs of transform.py
    transactions, cancellations, outliers = load_transform_outputs(paths)

    # Prepare fact_sales table
    fact_sales = prepare_fact_table(transactions, cancellations)

    warehouse_dir = Path(paths["warehouse"])

    # Save fact_sales and outliers to warehouse
    save_parquet(fact_sales, warehouse_dir, paths["fact_sales"])
    save_parquet(outliers, warehouse_dir, paths["outliers_parquet"])

if __name__ == "__main__":
    main()