from pathlib import Path
import pandas as pd
import yaml


# --- Load config file ---
def load_config(config_path: Path = Path(__file__).resolve().parents[1] / "config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


# --- Load datasets using config paths ---
def load_datasets(config):
    paths = config["paths"]

    transactions_df = pd.read_csv(
        paths["transactions"], dtype={"InvoiceNo": str, "CustomerID": str}
    )
    cancellations_df = pd.read_csv(
        paths["cancellations"], dtype={"InvoiceNo": str, "CustomerID": str}
    )
    outliers_df = pd.read_csv(
        paths["outliers"], dtype={"InvoiceNo": str, "CustomerID": str}
    )

    print("✅ Datasets loaded successfully from config paths.")
    return transactions_df, cancellations_df, outliers_df


# --- Validation logic ---
def validate_table(df, table_name):
    print(f"\n🔍 Validating {table_name}...")

    total_rows = len(df)
    print(f"Total rows: {total_rows:,}")

    issues = []

    # Check required columns
    required_columns = [
        "InvoiceNo", "StockCode", "Description", "Quantity",
        "InvoiceDate", "UnitPrice", "CustomerID", "Country"
    ]
    for col in required_columns:
        null_count = df[col].isna().sum()
        if null_count > 0:
            issues.append(f"{col} has {null_count} nulls")

    # Quantity and UnitPrice rules
    if table_name in ["transactions", "outliers"]:
        neg_qty = (df["Quantity"] <= 0).sum()
        neg_price = (df["UnitPrice"] <= 0).sum()
        if neg_qty > 0:
            issues.append(f"{neg_qty} rows have non-positive Quantity")
        if neg_price > 0:
            issues.append(f"{neg_price} rows have non-positive UnitPrice")
    elif table_name == "cancellations":
        neg_price = (df["UnitPrice"] <= 0).sum()
        if neg_price > 0:
            issues.append(f"{neg_price} rows have non-positive UnitPrice (should not happen)")

    # CustomerID format
    invalid_custid = df[~df["CustomerID"].str.match(r"^\d{5}$")]
    if len(invalid_custid) > 0:
        issues.append(f"{len(invalid_custid)} rows have invalid CustomerID")

    # InvoiceNo format
    if table_name == "transactions" or table_name == "outliers":
        invalid_invoice = df[~df["InvoiceNo"].str.match(r"^\d{6}$")]
    else:  # cancellations
        invalid_invoice = df[~df["InvoiceNo"].str.match(r"^C\d{6}$")]

    if len(invalid_invoice) > 0:
        issues.append(f"{len(invalid_invoice)} rows have invalid InvoiceNo format")

    # Print results
    if issues:
        print("⚠️  Issues found:")
        for issue in issues:
            print(f" - {issue}")
    else:
        print("✅ No issues found.")


# --- Main orchestration ---
def main():
    config = load_config()
    transactions_df, cancellations_df, outliers_df = load_datasets(config)

    validate_table(transactions_df, "transactions")
    validate_table(cancellations_df, "cancellations")
    validate_table(outliers_df, "outliers")


if __name__ == "__main__":
    main()