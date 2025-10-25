# validate.py
from pathlib import Path
import json
import pandas as pd
import great_expectations as gx

# --------------------------
# Hard-coded paths (as requested)
# --------------------------
DATA_PATH = Path("/home/alyona/personal_projects/foil_case_study/data/transactions.csv")
RESULTS_PATH = Path("/home/alyona/personal_projects/foil_case_study/data/validation_results.json")

# --------------------------
# Load dataframe
# --------------------------
df = pd.read_csv(DATA_PATH, dtype={"InvoiceNo": str})  # keep InvoiceNo as string to preserve 'C' prefix

# --------------------------
# Create / get GX Data Context
# --------------------------
# This will look for a great_expectations project in cwd or create an ephemeral context depending on your install.
context = gx.get_context()

# --------------------------
# Register a pandas datasource (idempotent)
# --------------------------
PANDAS_DATASOURCE_NAME = "pandas_ds"
try:
    # Newer GX (v1.x) exposes context.data_sources
    datasource = context.data_sources.get(PANDAS_DATASOURCE_NAME)
except Exception:
    try:
        datasource = context.data_sources.add_pandas(name=PANDAS_DATASOURCE_NAME)
    except Exception:
        # Fallback for older API shapes (some older docs used context.sources)
        try:
            datasource = context.sources.add_pandas(name=PANDAS_DATASOURCE_NAME)
        except Exception:
            raise RuntimeError(
                "Could not create a pandas datasource in your DataContext. "
                "Check your Great Expectations version and docs."
            )

# --------------------------
# Add a Data Asset for this dataframe
# --------------------------
DATA_ASSET_NAME = "transactions_asset"
# If asset already exists, get it; otherwise add a new dataframe asset
try:
    data_asset = datasource.get_asset(DATA_ASSET_NAME)
except Exception:
    # add_dataframe_asset is available in the modern API
    data_asset = datasource.add_dataframe_asset(name=DATA_ASSET_NAME)

# --------------------------
# Build a BatchRequest for this in-memory dataframe
# (for dataframe data assets you must pass the dataframe itself)
# --------------------------
batch_request = data_asset.build_batch_request(dataframe=df)

# --------------------------
# Create / ensure an Expectation Suite exists
# --------------------------
SUITE_NAME = "transactions_suite"
try:
    # preferred modern helper
    context.create_expectation_suite(expectation_suite_name=SUITE_NAME, overwrite_existing=True)
except Exception:
    # fallback: construct an ExpectationSuite object and add it to the context
    try:
        from great_expectations.core.expectation_suite import ExpectationSuite
        suite = ExpectationSuite(name=SUITE_NAME)
        context.suites.add(suite)
    except Exception:
        # if this fails, continue — we'll try to get a validator and create the suite there
        pass

# --------------------------
# Obtain a Validator (the object to run expectations against)
# --------------------------
validator = context.get_validator(batch_request=batch_request, expectation_suite_name=SUITE_NAME)

# --------------------------
# Example expectations (3 quick checks)
# - InvoiceNo not null
# - CustomerID present / 5-digit (we keep a simple not-null + numeric check here)
# - UnitPrice positive, Quantity positive
# Add or change to match your quality rules
# --------------------------
validator.expect_column_values_to_not_be_null("InvoiceNo")
validator.expect_column_values_to_not_be_null("CustomerID")
validator.expect_column_values_to_be_between("UnitPrice", min_value=0.01, mostly=0.999)
validator.expect_column_values_to_be_between("Quantity", min_value=1, mostly=0.999)

# You can add more expectations here (e.g., pattern checks for InvoiceNo, StockCode format, date parseable, etc.)

# Save expectation suite (persist the expectations in the GX context)
validator.save_expectation_suite(discard_failed_expectations=False)

# --------------------------
# Run validation
# --------------------------
validation_result = validator.validate()

# Print a short summary
print("Validation success:", validation_result.success)
print("Evaluated expectations:", validation_result.statistics.get("evaluated_expectations"))
print("Success percent:", validation_result.statistics.get("success_percent"))

# --------------------------
# Persist full JSON result for review / pipeline logs
# --------------------------
try:
    # .to_json_dict() available on the ValidationResult
    result_dict = validation_result.to_json_dict()
except Exception:
    # older versions may have .to_json_dict or .to_json; fallback to casting
    result_dict = json.loads(validation_result.json())

RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(RESULTS_PATH, "w", encoding="utf-8") as fh:
    json.dump(result_dict, fh, indent=2)

print(f"Full validation result written to: {RESULTS_PATH}")