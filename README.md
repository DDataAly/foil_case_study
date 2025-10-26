# Online Retail Data Pipeline — Case Study

## 📘 Project Summary

This project implements a **complete end-to-end data engineering pipeline** for an online retail company, based on the *UCI Online Retail dataset (2010–2011)*.  
The goal is to deliver a **reliable, high-quality, and analytically optimized data pipeline** that ingests, cleans, validates, and models transaction data for reporting and insights.

The pipeline produces a **dimensional data model** designed for analytical queries, including:

- **Fact table:** `fact_sales` — combines sales and cancellations  
- **Dimension tables:** `dim_customers`, `dim_products`, `dim_time`  
- **Quarantine table:** `outliers` — stores anomalous records pending review

The design focuses on:

- ✅ **Data quality and validation** — automated integrity and completeness checks  
- ✅ **Maintainability** — config-driven file paths and thresholds  
- ✅ **Analytical readiness** — star schema warehouse design  
- ✅ **Transparency and reproducibility** — clear documentation and modular scripts


## 🗂️ Project Structure

The repository is organized for clarity, modularity, and reproducibility.  

Below is the folder layout showing all key project components and their purpose:

```bash
foil_case_study/
├── data/                           # Data assets generated and used by the pipeline
│   ├── online_retail_raw.csv        # Raw dataset downloaded from UCI repository
│   ├── transactions.csv             # Processed — cleaned sales transactions
│   ├── cancellations.csv            # Processed — cleaned cancellation records
│   ├── outliers.csv                 # Processed — records flagged as anomalies
│   └── warehouse/                   # Analytical layer (star schema)
│       ├── fact_sales.parquet       # Fact table — sales and cancellations combined
│       └── outliers.parquet         # Quarantine table for anomalous records
│
├── db/
│   └── schema.sql                   # SQL schema defining fact and dimension tables
│
├── etl/                             # Core ETL pipeline scripts
│   ├── extract.py                   # Data extraction from UCI repository
│   ├── transform.py                 # Data cleaning, filtering, and outlier detection
│   ├── validate.py                  # Automated data validation checks
│   └── load.py                      # Load step — builds warehouse tables in Parquet
│
├── src/
│   └── helpers/
│       └── logger.py                # Centralized logging utility (future use)
│
├── tests/                           # Unit tests for ETL modules
│   ├── test_extract.py
│   └── test_transform.py
│
├── exploration/                     # Exploratory analysis notebooks
│   ├── initial_exploration.ipynb
│   └── outliers_check.ipynb
│
├── config.yaml                      # Central configuration (paths, thresholds, parameters)
├── requirements.txt                 # Python dependencies
├── .gitignore                       # Excludes unnecessary files from version control
└── README.md                        # Project documentation
```

## ⚙️ Setup & Instructions

### 1. Environment setup

Clone the repository and navigate to the project root:


```bash
git clone <your-repo-url>
cd foil_case_study
```
Create virtual environment (recommended)
```bash
python -m venv venv
```
Activate virtual environment
```bash
# macOS/Linux
source venv/bin/activate
```
```bash
# Windows
venv\Scripts\activate
```
Install dependencies
```bash
pip install -r requirements.txt
```

## 🛠 Configuration

The project uses a single `config.yaml` file to manage all file paths, thresholds, and parameters. This ensures the pipeline is maintainable and can run on different systems without code changes.

### Key points:

- **Paths**: Define locations for raw data, processed outputs (transactions, cancellations, outliers), and warehouse parquet files (fact and dimension tables).  
- **Outlier thresholds**: Specify quantity and unit price limits to flag anomalous records during transformation.

All ETL scripts (`extract.py`, `transform.py`, `validate.py`, `load.py`) read paths and thresholds from this file, keeping the pipeline fully configurable.


## 🚀 Pipeline Overview

The pipeline implements a full end-to-end ETL workflow for online retail transactions. A single entry point, `run.py`, orchestrates all stages, ensuring reproducibility and maintainability.

### Pipeline Stages

1. **Extract (`extract.py`)**
   - Downloads the raw dataset from the UCI repository.
   - Saves the raw CSV file under `data/online_retail_raw.csv`.

2. **Transform (`transform.py`)**
   - Cleans and filters the raw data (removes duplicates, invalid quantities/prices, and missing critical fields).
   - Splits data into **transactions**, **cancellations**, and **outliers**.
   - Applies outlier thresholds based on quantity and unit price.
   - Saves the outputs as CSV files under `data/`.

3. **Validate (`validate.py`)**
   - Performs automated data quality checks:
     - Completeness of critical fields.
     - Valid formats for `InvoiceNo` and `CustomerID`.
     - Referential checks for quantities and prices.
   - Flags any anomalies for review in logs.

4. **Load (`load.py`)**
   - Merges transactions and cancellations into a single fact table (`fact_sales`).
   - Converts the data into Parquet format for warehouse storage.
   - Saves dimension tables (`dim_customers`, `dim_products`, `dim_time`) and a quarantine table (`outliers`) to `data/warehouse/`.


### Orchestration (`run.py`)

`run.py` serves as the **single entry point** for the entire pipeline.  

- Executes all ETL stages in the correct order: **extract → transform → validate → load**.  
- Ensures reproducibility and simplifies pipeline execution.  
- Supports future automation or scheduling (e.g., via cron or Airflow).  
- Users only need to run `python run.py` from the project root to process the dataset end-to-end.

### Pipeline Architecture Diagram

For the end-to-end ETL workflow for the Online Retail dataset, including extraction, transformation, validation, loading, and the resulting star schema warehouse see [Pipeline Architecture](docs/architecture_diagram.png)

## 🗄️ Dimensional Data Model

To support analytical queries on retail transactions, the pipeline implements a **star schema** consisting of a central fact table and multiple dimension tables.

---

### Fact Table: `fact_sales`

**Purpose:** Stores individual sales transactions, including both completed and cancelled orders.

**Columns:**

- `transaction_key` – surrogate primary key (`SERIAL`)
- `invoice_no` – original invoice number (`VARCHAR(10) NOT NULL`)
- `customer_key` – foreign key to `dim_customers`
- `product_key` – foreign key to `dim_products`
- `date_key` – foreign key to `dim_time` (transaction date)
- `transaction_datetime` – exact timestamp of the transaction (`TIMESTAMP NOT NULL`)
- `quantity` – number of units sold (`INT NOT NULL`)
- `unit_price` – price per unit (`DECIMAL(10,2) NOT NULL`)
- `sign` – `+1` for sales, `-1` for cancellations (`SMALLINT`)
- `total_amount` – calculated as `quantity * unit_price * sign` (`DECIMAL(12,2) GENERATED ALWAYS AS ... STORED`)
- `transaction_type` – `"SALE"` or `"CANCEL"` (`VARCHAR(10)`)

**Rationale:**  
Including `sign` and `transaction_type` allows straightforward revenue calculations and proper handling of cancellations. Surrogate keys simplify joins and ensure uniqueness, while storing `transaction_datetime` preserves time-based analysis.

---

### Dimension Table: `dim_customers`

**Purpose:** Stores customer information for analysis and segmentation.

**Columns:**

- `customer_key` – surrogate primary key (`SERIAL`)
- `customer_id` – original 5-digit customer identifier (`VARCHAR(5) NOT NULL`)
- `country` – customer's country (`VARCHAR(50)`)

**Rationale:**  
Separating customer attributes reduces redundancy in the fact table and simplifies filtering by customer properties.

---

### Dimension Table: `dim_products`

**Purpose:** Stores product details to analyze sales by item.

**Columns:**

- `product_key` – surrogate primary key (`SERIAL`)
- `stock_code` – original product code (`VARCHAR(5) NOT NULL`)
- `description` – product name (`VARCHAR(255)`)

**Rationale:**  
Keeps product metadata separate from facts to reduce duplication and enable product-level analytics.

---

### Dimension Table: `dim_time`

**Purpose:** Stores date-level information for time-based analytics.

**Columns:**

- `date_key` – date of transaction (`DATE PRIMARY KEY`)
- `year` – year of the transaction (`INT`)
- `month` – month number (`INT`)
- `day` – day of month (`INT`)
- `weekday` – day of week (`VARCHAR(10)`)

**Rationale:**  
Supports flexible grouping and aggregation by year, quarter, month, or weekday without repeatedly extracting from `transaction_datetime`.

---

### Quarantine Table: `outliers`

**Purpose:** Stores anomalous or exceptional transactions for manual review.

**Columns:** Same as `fact_sales`

**Rationale:**  
Mirrors the fact table structure to allow easy reinsertion of reviewed records. Keeps the main warehouse clean and ready for analytics.

---

**Overall Design Notes:**

- Follows a **star schema pattern**: central fact table + dimension tables.  
- Surrogate keys simplify joins and ensure uniqueness.  
- Designed for efficient analytical queries and flexible aggregation.  
- Outliers are quarantined separately to maintain warehouse integrity.

---

For a detailed list of table structures and columns, see [Data Dictionary](docs/data_dictionary.md).


## ✅ Data Quality & Validation

Ensuring data integrity and reliability is a core part of this pipeline. Automated checks are implemented at multiple stages to catch anomalies early and maintain a high-quality dataset for analytics.

### Key Data Quality Checks

1. **Completeness Checks**
   - Critical fields (`InvoiceNo`, `StockCode`, `CustomerID`, `Quantity`, `UnitPrice`, `InvoiceDate`) are validated to ensure no missing values.
   - Missing or null records are removed during the transformation stage.

2. **Format and Consistency**
   - `InvoiceNo` must match expected patterns:  
     - Transactions: 6-digit numbers (e.g., `123456`)  
     - Cancellations: `C` prefix + 6-digit number (e.g., `C123456`)
   - `CustomerID` must be a 5-digit string.
   - `StockCode` must match the product code format.

3. **Business Logic Validation**
   - `Quantity` and `UnitPrice` must be positive for transactions.  
   - Negative quantities indicate cancellations and are handled separately.
   - Outlier detection:
     - Quantities exceeding the threshold (`config.yaml: outlier_thresholds.quantity`)  
     - Unit prices exceeding the threshold (`config.yaml: outlier_thresholds.unit_price`)  
     - Outliers are removed from the main dataset and stored in `outliers` for review.

4. **Referential Integrity**
   - Fact table foreign keys reference dimension tables (`dim_customers`, `dim_products`, `dim_time`) using surrogate keys.
   - Ensures consistent joins for analytical queries.

5. **Logging and Alerts**
   - Validation results are printed during pipeline execution.
   - Any detected anomalies or errors are flagged for review, ensuring transparency and reproducibility.

### Summary

These automated checks guarantee that:

- Only clean, consistent, and reliable data enters the warehouse.
- Analysts can trust metrics derived from the fact table.
- Anomalous data is quarantined without affecting overall analytics.

## 🏃‍♂️ Usage & Execution

The pipeline is designed for **end-to-end execution** with minimal user intervention. A single orchestration script, `run.py`, handles all stages in the correct order.

### Running the Pipeline

From the project root:

```bash
python run.py
```
### Pipeline Execution

Running the pipeline via `run.py` executes the following stages:

- **Extract** – Downloads the raw dataset from the UCI repository (`data/online_retail_raw.csv`).
- **Transform** – Cleans the raw data, splits transactions and cancellations, and detects outliers.
- **Validate** – Runs automated data quality checks and prints any issues found.
- **Load** – Builds the dimensional warehouse tables (`fact_sales`, `dim_customers`, `dim_products`, `dim_time`) and saves the `outliers` quarantine table in Parquet format under `data/warehouse/`.

### Output Files

After successful execution, the following files are available in the warehouse layer (`data/warehouse/`):

- `fact_sales.parquet` – Consolidated fact table (sales + cancellations)
- `dim_customers.parquet` – Customer dimension
- `dim_products.parquet` – Product dimension
- `dim_time.parquet` – Time dimension
- `outliers.parquet` – Quarantined anomalous records

### Notes

- The pipeline is **idempotent**: running `run.py` multiple times will overwrite outputs but not the raw data.
- All thresholds, paths, and configurable parameters are read from `config.yaml`.
- Logging and validation messages are printed to the console for transparency and debugging.

## 📚 Dependencies

This project uses Python 3.x and the following main libraries:

- `pandas` – Data manipulation and cleaning  
- `pyarrow` / `fastparquet` – Parquet I/O  
- `yaml` – Configuration parsing  
- `matplotlib` – Optional for data exploration  
- `pytest` – Unit testing for ETL scripts  

All dependencies are listed in `requirements.txt` and can be installed via:

```bash
pip install -r requirements.txt
```

## 📝 Assumptions & Notes

- The UCI Online Retail dataset covers **2010–2011 UK transactions**.  
- `InvoiceNo` starting with `"C"` indicates a **cancellation**.  
- **Outliers** are defined using configurable thresholds (`quantity` and `unit_price`) in `config.yaml`.  
- The warehouse uses a **star schema** with a fact table and three dimensions; outliers are **quarantined for review**.  
- The pipeline is designed to be **reproducible, maintainable, and idempotent**.  
- Logging is minimal to console for clarity; a future enhancement could include **persistent logging or monitoring**.



