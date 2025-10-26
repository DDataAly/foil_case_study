import os
import pandas as pd
import yaml
import logging
from ucimlrepo import fetch_ucirepo
from pathlib import Path


def load_config(config_path: Path = Path(__file__).resolve().parents[1] / "config.yaml") -> dict:
    """Load pipeline configuration from YAML file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def download_dataset(dataset_id=352, config_path: Path = None):
    """
    Download the Online Retail dataset from UCI and save locally
    using path from config.yaml if provided. Performs basic early
    data quality checks.
    """
    # Load config
    if config_path is None:
        config_path = Path(__file__).resolve().parents[1] / "config.yaml"
    config = load_config(config_path)

    save_path = Path(config["paths"]["raw_data"])
    save_dir = save_path.parent
    os.makedirs(save_dir, exist_ok=True)

    try:
        logging.info(f"Downloading dataset ID {dataset_id} from UCI repository...")
        dataset = fetch_ucirepo(id=dataset_id)
        df = dataset.data.original

        # --- Early data quality checks ---
        required_columns = [
            "InvoiceNo", "StockCode", "Description", "Quantity",
            "InvoiceDate", "UnitPrice", "CustomerID", "Country"
        ]
        missing_cols = [col for col in required_columns if col not in df.columns]
        if missing_cols:
            logging.error(f"Missing required columns in raw dataset: {missing_cols}")
            raise ValueError(f"Raw dataset is missing columns: {missing_cols}")

        # Check types for numeric columns
        numeric_cols = ["Quantity", "UnitPrice", "CustomerID"]
        for col in numeric_cols:
            if not pd.api.types.is_numeric_dtype(df[col]):
                logging.warning(f"Column '{col}' is not numeric")

        # Critical columns for downstream processing
        critical_cols = ["InvoiceNo", "CustomerID", "InvoiceDate", "StockCode"]
        for col in critical_cols:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                logging.warning(f"{missing_count} missing values in critical column '{col}'")

        # Save CSV
        df.to_csv(save_path, index=False)
        logging.info(f"Dataset saved to: {save_path}")
        logging.info(f"Shape: {df.shape[0]:,} rows x {df.shape[1]} columns")

        return save_path

    except Exception as e:
        logging.error(f"Failed to download or save dataset: {e}")
        raise


def main():
    download_dataset()


if __name__ == "__main__":
    main()


