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
    using path from config.yaml if provided.
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
        df.to_csv(save_path, index=False)
        logging.info(f"Dataset saved to: {save_path}")
        logging.info(f"ℹShape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        return save_path

    except Exception as e:
        logging.error(f"Failed to download or save dataset: {e}")
        raise


def main():
    download_dataset()


if __name__ == "__main__":
    main()
