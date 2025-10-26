import logging
import pandas as pd
import yaml
from pathlib import Path


# --- 1️⃣ Load config file ---
def load_config(config_path: Path = Path(__file__).resolve().parents[1] / "config.yaml") -> dict:
    """Load pipeline configuration from YAML file."""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logging.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load config file: {e}")
        raise


# --- 2️⃣ Load and clean data ---
def load_and_clean_data(path: Path) -> pd.DataFrame:
    """Load raw dataset and perform initial cleaning."""
    try:
        df = pd.read_csv(path, dtype={"InvoiceNo": str})
        logging.info(f"Loaded raw dataset: {len(df):,} records")

        df = df.drop_duplicates()
        df.columns = df.columns.str.strip()

        # Remove missing key fields
        df = df[df["Description"].notna()]
        df = df[df["CustomerID"].notna()]
        df = df[df["Country"].notna()]
        df = df[df["StockCode"].notna()]

        # Convert and validate date field
        df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"], errors="coerce")
        df = df[df["InvoiceDate"].notna()]

        # Convert CustomerID to string (after coercion)
        df["CustomerID"] = df["CustomerID"].astype(int).astype(str)

        # Keep only valid formats
        df = df[df["InvoiceNo"].str.match(r"^C?\d{6}$", na=False)]
        df = df[df["StockCode"].astype(str).str.match(r"^\d{5}$", na=False)]
        df = df[df["Description"].str.contains(r"[a-zA-Z]", na=False)]
        df = df[df["CustomerID"].astype(str).str.match(r"^\d{5}$", na=False)]

        logging.info(f"After cleaning: {len(df):,} records remaining")
        return df

    except Exception as e:
        logging.error(f"Error during data loading or cleaning: {e}")
        raise


# --- 3️⃣ Filter invalid UnitPrices ---
def filter_invalid_prices(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where UnitPrice <= 0."""
    try:
        before = len(df)
        df = df[df["UnitPrice"] > 0]
        removed = before - len(df)
        logging.info(f"Filtered out {removed:,} rows with non-positive UnitPrice")
        return df
    except Exception as e:
        logging.error(f"Error filtering invalid prices: {e}")
        raise


# --- 4️⃣ Split transactions vs cancellations ---
def split_transactions(df: pd.DataFrame):
    """Split dataset into transactions and cancellations."""
    try:
        transactions = df[~df["InvoiceNo"].str.startswith("C")].copy()
        cancellations = df[df["InvoiceNo"].str.startswith("C")].copy()
        logging.info(f"Split into {len(transactions):,} transactions and {len(cancellations):,} cancellations")
        return transactions, cancellations
    except Exception as e:
        logging.error(f"Error splitting transactions and cancellations: {e}")
        raise


# --- 5️⃣ Filter invalid Quantities ---
def filter_invalid_quantities(df: pd.DataFrame) -> pd.DataFrame:
    """Remove transactions with Quantity <= 0."""
    try:
        before = len(df)
        df = df[df["Quantity"] > 0]
        removed = before - len(df)
        logging.info(f"Filtered out {removed:,} rows with non-positive Quantity")
        return df
    except Exception as e:
        logging.error(f"Error filtering invalid quantities: {e}")
        raise


# --- 6️⃣ Detect outliers ---
def detect_outliers(df: pd.DataFrame, qty_threshold: int, price_threshold: float):
    """Identify and separate outliers based on thresholds."""
    try:
        qty_outliers = df[df["Quantity"] > qty_threshold]
        price_outliers = df[df["UnitPrice"] > price_threshold]
        outliers = pd.concat([qty_outliers, price_outliers]).drop_duplicates()

        logging.info(f"Detected {len(outliers):,} outliers")

        df_clean = df[~df.index.isin(outliers.index)].copy()
        return df_clean, outliers
    except Exception as e:
        logging.error(f"Error detecting outliers: {e}")
        raise


# --- 7️⃣ Save outputs ---
def save_datasets(transactions, cancellations, outliers, paths):
    """Save transformed datasets to CSV files."""
    try:
        transactions.to_csv(paths["transactions"], index=False)
        cancellations.to_csv(paths["cancellations"], index=False)
        outliers.to_csv(paths["outliers"], index=False)

        logging.info(f"Saved transactions → {paths['transactions']}")
        logging.info(f"Saved cancellations → {paths['cancellations']}")
        logging.info(f"Saved outliers → {paths['outliers']}")
    except Exception as e:
        logging.error(f"Error saving transformed datasets: {e}")
        raise


# --- 8️⃣ Orchestrate transform ---
def main():
    try:
        config = load_config()
        paths = config["paths"]
        thresholds = config["outlier_thresholds"]

        df = load_and_clean_data(Path(paths["raw_data"]))
        df = filter_invalid_prices(df)
        transactions, cancellations = split_transactions(df)
        transactions = filter_invalid_quantities(transactions)

        clean_transactions, outliers = detect_outliers(
            transactions, thresholds["quantity"], thresholds["unit_price"]
        )

        save_datasets(clean_transactions, cancellations, outliers, paths)
        logging.info("Transform stage completed successfully")

    except Exception as e:
        logging.error(f"Transform stage failed: {e}")
        raise


if __name__ == "__main__":
    main()