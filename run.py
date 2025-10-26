import logging
import traceback
from src.helpers.logger import setup_logging
from etl import extract, transform, validate, load

def main():
    setup_logging()
    logging.info("Starting ETL pipeline...")

    try:
        logging.info("Step 1: Extracting raw dataset...")
        extract.main()

        logging.info("Step 2: Transforming dataset...")
        transform.main()

        logging.info("Step 3: Validating cleaned datasets...")
        validate.main()

        logging.info("Step 4: Loading datasets into warehouse...")
        load.main()

        logging.info("ETL pipeline completed successfully!")

    except Exception as e:
        logging.error(f"ETL pipeline failed: {e}")
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()