from pathlib import Path
import pandas as pd
import yaml
import logging


def load_config(config_path: Path = Path(__file__).resolve().parents[1] / "config.yaml") -> dict:
    """Load configuration file."""
    try:
        with open(config_path, "r") as file:
            config = yaml.safe_load(file)
        logging.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logging.error(f"Failed to load configuration file: {e}")
        raise


def load_datasets(config: dict):
    """Load datasets from paths specified in the config file."""
    paths = config["paths"]
    datasets = {}

    try:
        datasets["transactions"] = pd.read_csv(
            paths["transactions"], dtype={"InvoiceNo": str, "CustomerID": str}
        )
        datasets["cancellations"] = pd.read_csv(
            paths["cancellations"], dtype={"InvoiceNo": str, "CustomerID": str}
        )
        datasets["outliers"] = pd.read_csv(
            paths["outliers"], dtype={"InvoiceNo": str, "CustomerID": str}
        )
        logging.info("Datasets loaded successfully.")
        return datasets
    except FileNotFoundError as e:
        logging.error(f"Dataset file not found: {e}")
        raise
    except pd.errors.EmptyDataError as e:
        logging.error(f"One of the CSV files is empty or corrupt: {e}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error while loading datasets: {e}")
        raise


def validate_table(df: pd.DataFrame, table_name: str):
    """Perform basic data quality checks on a single table."""
    logging.info(f"Validating {table_name} table...")

    try:
        total_rows = len(df)
        logging.info(f"{table_name}: {total_rows:,} rows loaded.")
        issues = []

        # Required columns
        required_columns = [
            "InvoiceNo", "StockCode", "Description", "Quantity",
            "InvoiceDate", "UnitPrice", "CustomerID", "Country"
        ]
        for col in required_columns:
            if col not in df.columns:
                issues.append(f"Missing required column: {col}")
                continue
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
            issues.append(f"{len(invalid_custid)} rows have invalid CustomerID format")

        # InvoiceNo format
        if table_name in ["transactions", "outliers"]:
            invalid_invoice = df[~df["InvoiceNo"].str.match(r"^\d{6}$")]
        else:
            invalid_invoice = df[~df["InvoiceNo"].str.match(r"^C\d{6}$")]
        if len(invalid_invoice) > 0:
            issues.append(f"{len(invalid_invoice)} rows have invalid InvoiceNo format")

        # Summary
        if issues:
            logging.warning(f"Issues found in {table_name}:")
            for issue in issues:
                logging.warning(f" - {issue}")
        else:
            logging.info(f"No issues found in {table_name}.")

    except Exception as e:
        logging.error(f"Validation failed for {table_name}: {e}")
        raise


def main():
    """Run validation for all datasets."""
    try:
        config = load_config()
        datasets = load_datasets(config)

        for name, df in datasets.items():
            validate_table(df, name)

        logging.info("Data validation completed successfully.")

    except Exception as e:
        logging.error(f"Validation pipeline failed: {e}")
        raise


if __name__ == "__main__":
    main()