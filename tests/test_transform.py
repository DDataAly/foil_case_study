import pytest
import pandas as pd
from etl.transform import (
    load_and_clean_data,
    filter_invalid_prices,
    split_transactions,
    filter_invalid_quantities,
    detect_outliers,
)

@pytest.mark.describe("Transform tests")
class TestTransform:

    @pytest.mark.it("should correctly clean, split, and detect outliers")
    def test_transform_pipeline(self, tmp_path):
        # --- 1️⃣ Create a small dummy dataset ---
        data = {
            "InvoiceNo": ["100001", "C100002", "100003", "100004"],
            "StockCode": ["12345", "12345", "99999", "54321"],
            "Description": ["Widget A", "Widget A", "Widget B", "Widget C"],
            "Quantity": [10, -5, 500, 2],
            "InvoiceDate": pd.to_datetime(
                ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04"]
            ),
            "UnitPrice": [2.5, 2.5, 1000.0, 3.0],
            "CustomerID": ["12345", "12345", "12346", "12347"],
            "Country": ["UK", "UK", "UK", "UK"],
        }
        df = pd.DataFrame(data)

        # --- 2️⃣ Clean and transform step-by-step ---
        cleaned_df = filter_invalid_prices(df)
        transactions, cancellations = split_transactions(cleaned_df)
        transactions = filter_invalid_quantities(transactions)
        clean_tx, outliers = detect_outliers(transactions, qty_threshold=100, price_threshold=500.0)

        # --- 3️⃣ Assertions ---
        assert not cleaned_df["UnitPrice"].le(0).any(), "There should be no non-positive prices"
        assert all(~transactions["InvoiceNo"].str.startswith("C")), "Transactions should not include cancellations"
        assert (transactions["Quantity"] > 0).all(), "Quantities must be positive"
        assert isinstance(outliers, pd.DataFrame), "Outliers output should be a DataFrame"
        assert len(clean_tx) + len(outliers) <= len(transactions), "Outliers subset check"

        # --- 4️⃣ Sanity check output schema ---
        expected_cols = {
            "InvoiceNo", "StockCode", "Description",
            "Quantity", "InvoiceDate", "UnitPrice",
            "CustomerID", "Country"
        }
        assert expected_cols.issubset(clean_tx.columns), "Missing expected columns in transformed data"
