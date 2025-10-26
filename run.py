
import logging
from src.helpers.logger import setup_logging
from etl import extract, transform, validate, load


def main():
    # --- 1️⃣ Setup logging ---
    setup_logging()  # This creates logs/pipeline.log and prints to console as well
    logging.info("🚀 Starting ETL pipeline...")

    try:
        # --- 2️⃣ Extract ---
        logging.info("Step 1: Extracting raw dataset...")
        extract.main()

    #     # --- 3️⃣ Transform ---
    #     logging.info("Step 2: Transforming dataset...")
    #     transform.main()

    #     # --- 4️⃣ Validate ---
    #     logging.info("Step 3: Validating cleaned datasets...")
    #     validate.main()

    #     # --- 5️⃣ Load ---
    #     logging.info("Step 4: Loading datasets into warehouse...")
    #     load.main()

    #     logging.info("✅ ETL pipeline completed successfully!")

    except Exception as e:
        logging.error(f"❌ ETL pipeline failed: {e}")


if __name__ == "__main__":
    main()
