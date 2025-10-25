from pathlib import Path
import pandas as pd
import yaml


# --- 1️⃣ Load config file ---
def load_config(config_path: Path = Path(__file__).resolve().parents[1] / "config.yaml"):
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
    return config


# --- 2️⃣ Load and clean data ---
def load_and_clean_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype={"InvoiceNo": str})
    print(f"Initial records: {len(df):,}")

    df = df.drop_duplicates()
    df.columns = df.columns.str.strip()

    # Remove rows with missing Description, CustomerID, Country
    df = df[df["Description"].notna()]
    df = df[df["CustomerID"].notna()]
    df = df[df["Country"].notna()]

    # Convert date
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
    df = df[df["InvoiceDate"].notna()]

    # Convert and clean CustomerID
    df["CustomerID"] = df["CustomerID"].astype(int).astype(str)

    # Keep only valid formats
    df = df[df["InvoiceNo"].str.match(r"^C?\d{6}$")]
    df = df[df["StockCode"].astype(str).str.match(r"^\d{5}$")]
    df = df[df["Description"].str.contains(r"[a-zA-Z]", na=False)]
    df = df[df["CustomerID"].astype(str).str.match(r"^\d{5}$")]

    print(f"After cleaning: {len(df):,}")
    return df


# --- 3️⃣ Split transactions vs cancellations ---
def split_transactions(df: pd.DataFrame):
    transactions = df[~df["InvoiceNo"].str.startswith("C")].copy()
    cancellations = df[df["InvoiceNo"].str.startswith("C")].copy()
    print(f"Transactions: {len(transactions):,}, Cancellations: {len(cancellations):,}")
    return transactions, cancellations


# --- 4️⃣ Detect outliers ---
def detect_outliers(df: pd.DataFrame, qty_threshold: int, price_threshold: float):
    qty_outliers = df[df["Quantity"] > qty_threshold]
    price_outliers = df[df["UnitPrice"] > price_threshold]
    outliers = pd.concat([qty_outliers, price_outliers]).drop_duplicates()

    print(f"Outliers found: {len(outliers):,}")

    # Remove outliers from clean dataset
    df_clean = df[~df.index.isin(outliers.index)].copy()
    return df_clean, outliers


# --- 5️⃣ Save outputs ---
def save_datasets(transactions, cancellations, outliers, paths):
    transactions.to_csv(paths["transactions"], index=False)
    cancellations.to_csv(paths["cancellations"], index=False)
    outliers.to_csv(paths["outliers"], index=False)

    print(f"✅ Saved transactions: {len(transactions):,} → {paths['transactions']}")
    print(f"✅ Saved cancellations: {len(cancellations):,} → {paths['cancellations']}")
    print(f"✅ Saved outliers: {len(outliers):,} → {paths['outliers']}")


# --- 6️⃣ Orchestrate pipeline ---
def main():
    config = load_config()
    paths = config["paths"]
    thresholds = config["outlier_thresholds"]

    df = load_and_clean_data(Path(paths["raw_data"]))
    transactions, cancellations = split_transactions(df)
    clean_transactions, outliers = detect_outliers(
        transactions, thresholds["quantity"], thresholds["unit_price"]
    )
    save_datasets(clean_transactions, cancellations, outliers, paths)


if __name__ == "__main__":
    main()