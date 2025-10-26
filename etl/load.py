
from pathlib import Path
import pandas as pd
import yaml
import logging


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


def load_transform_outputs(paths: dict):
    """Load outputs from transform.py"""
    try:
        transactions = pd.read_csv(paths["transactions"], dtype={"InvoiceNo": str, "CustomerID": str})
        cancellations = pd.read_csv(paths["cancellations"], dtype={"InvoiceNo": str, "CustomerID": str})
        outliers = pd.read_csv(paths["outliers"], dtype={"InvoiceNo": str, "CustomerID": str})
        logging.info("Transform outputs loaded successfully")
        return transactions, cancellations, outliers
    except Exception as e:
        logging.error(f"Error loading transform outputs: {e}")
        raise


def prepare_fact_table(transactions: pd.DataFrame, cancellations: pd.DataFrame) -> pd.DataFrame:
    """Merge transactions and cancellations into fact_sales table"""
    try:
        transactions = transactions.copy()
        cancellations = cancellations.copy()

        transactions["transaction_type"] = "SALE"
        transactions["sign"] = 1

        cancellations["transaction_type"] = "CANCEL"
        cancellations["sign"] = -1

        fact_table = pd.concat([transactions, cancellations], ignore_index=True)
        fact_table.rename(columns={"InvoiceDate": "transaction_datetime"}, inplace=True)

        logging.info(f"Fact table prepared with {len(fact_table):,} rows")
        return fact_table
    except Exception as e:
        logging.error(f"Error preparing fact table: {e}")
        raise


def save_parquet(df: pd.DataFrame, output_dir: Path, filename: str):
    """Save DataFrame to Parquet format"""
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        output_path = output_dir / filename
        df.to_parquet(output_path, index=False)
        logging.info(f"Saved {filename} with {len(df):,} rows → {output_path}")
    except Exception as e:
        logging.error(f"Failed to save {filename} to Parquet: {e}")
        raise


def main():
    try:
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

        logging.info("Load stage completed successfully")
    except Exception as e:
        logging.error(f"Load stage failed: {e}")
        raise


if __name__ == "__main__":
    main()
