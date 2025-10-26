import pytest
from etl import extract
from pathlib import Path

@pytest.mark.describe("Extract tests")
class TestExtract:

    @pytest.mark.it("should download dataset and save CSV locally")
    def test_download_dataset_creates_file(self, tmp_path):
        """
        Test that download_dataset() returns a valid path and the CSV exists.
        Use tmp_path to avoid writing to actual data/ folder.
        """
        # Patch the config to use tmp_path
        test_csv_path = tmp_path / "online_retail_raw.csv"

        # Call the function with a local path override
        result_path = extract.download_dataset(config_path=None)  # default path
        path_obj = Path(result_path)

        # Check returned path exists
        assert path_obj.exists(), "CSV file was not created"

        # Check it's a CSV
        assert path_obj.suffix == ".csv"

        # Check it has content
        import pandas as pd
        df = pd.read_csv(path_obj)
        assert not df.empty, "Downloaded CSV is empty"
        # Optionally check expected columns
        expected_columns = ["InvoiceNo", "StockCode", "Description",
                            "Quantity", "InvoiceDate", "UnitPrice",
                            "CustomerID", "Country"]
        for col in expected_columns:
            assert col in df.columns, f"Missing column {col}"