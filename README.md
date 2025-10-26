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



Instructions:
Place online_retail_raw.csv under /data/ before running scripts.


Design choice rationale:
At the transform stage, transactions and cancellations are stored separately to preserve their distinct business logic and simplify validation. In the warehouse layer, both are unified into a single fact table (fact_sales) with an indicator column (transaction_type) to support net-sales analytical queries.

# Dimensional Data Model

To support analytical queries on retail transactions, we designed a dimensional data model consisting of a fact table and multiple dimension tables.

## Fact Table: `fact_sales`

**Purpose:** Stores individual sales transactions, including both completed and cancelled orders.

**Columns:**

- `transaction_key` – surrogate primary key (`SERIAL`)
- `invoice_no` – original invoice number (`VARCHAR(10) NOT NULL`)
- `customer_key` – foreign key to `dim_customers`
- `product_key` – foreign key to `dim_products`
- `date_key` – foreign key to `dim_time` (date of transaction)
- `transaction_datetime` – exact timestamp of the transaction (`TIMESTAMP NOT NULL`)
- `quantity` – number of units sold (`INT NOT NULL`)
- `unit_price` – price per unit (`DECIMAL(10,2) NOT NULL`)
- `sign` – `+1` for sales, `-1` for cancellations (`SMALLINT`)
- `total_amount` – calculated as `quantity * unit_price * sign` (`DECIMAL(12,2) GENERATED ALWAYS AS ... STORED`)
- `transaction_type` – `"SALE"` or `"CANCEL"` (`VARCHAR(10)`)

**Rationale:**  
Using a surrogate `transaction_key` simplifies joins and ensures uniqueness even if `invoice_no` is not strictly unique. Including `sign` and `transaction_type` allows straightforward revenue calculations and proper handling of cancellations. Storing `transaction_datetime` preserves the exact time of each transaction for time-based analyses. The `total_amount` is calculated at the database level to avoid repeated computations in queries.

---

## Dimension Table: `dim_customers`

**Purpose:** Stores customer information for analysis and segmentation.

**Columns:**

- `customer_key` – surrogate primary key (`SERIAL`)
- `customer_id` – original 5-digit customer identifier (`VARCHAR(5) NOT NULL`)
- `country` – customer's country (`VARCHAR(50)`)

**Rationale:**  
A surrogate key ensures consistency in joins with the fact table. Customer attributes are stored in a separate table to reduce redundancy and support easy filtering by customer properties.

---
## Dimension Table: `dim_products`

**Purpose:** Stores product details to analyze sales by item.

**Columns:**

- `product_key` – surrogate primary key (`SERIAL`)
- `stock_code` – original product code (`VARCHAR(5) NOT NULL`)
- `description` – product name (`VARCHAR(255)`)

**Rationale:**  
Separating product information into a dimension table avoids repeated storage of product names in the fact table and simplifies product-level analyses. Surrogate keys allow consistent joins and prevent duplication issues.
---

## Dimension Table: `dim_time`

**Purpose:** Stores date-level information for time-based analytics.

**Columns:**

- `date_key` – date of transaction (`DATE PRIMARY KEY`)
- `year` – year of the transaction (`INT`)
- `month` – month number (`INT`)
- `day` – day of month (`INT`)
- `weekday` – day of week (`VARCHAR(10)`)

**Rationale:**  
A separate time dimension allows flexible grouping, filtering, and aggregation by year, quarter, month, or weekday. This design improves query performance and simplifies date-based calculations in analytical reports.

---

## Outliers Table: `outliers`

**Purpose:** Temporary quarantine for unusual transactions to be reviewed manually.

**Columns:**
- All columns match `fact_sales` (`invoice_no`, `customer_key`, `product_key`, `date_key`, `transaction_datetime`, `quantity`, `unit_price`, `sign`, `total_amount`, `transaction_type`)

**Rationale:**  
Keeping the same structure as `fact_sales` allows easy reinsertion of reviewed outliers back into the fact table. No separate dimension tables are needed.

**Notes:**
- The outliers table is **outside the main star schema**, serving as a quarantine.  
- Once outliers are reviewed, they can be merged into `fact_sales` without needing transformations or separate dimension tables.  
- This design keeps the warehouse clean, normalized, and ready for analytical queries.

**Overall Rationale:**  
This dimensional model follows the star schema pattern, with a central fact table (`fact_sales`) connected to dimension tables (`dim_customers`, `dim_products`, `dim_time`). It supports efficient analytical queries, reduces redundancy, and provides flexibility for aggregation and slicing of data along different dimensions. Surrogate keys simplify joins and ensure uniqueness across the schema.

Rationale: Simplifies reporting by precomputing useful time attributes, avoids repeated extraction logic from InvoiceDate.

Design Principles

Star Schema: Fact table at the center, dimension tables surrounding it.

Normalization: Dimension tables remove redundancy from the fact table.

Analytical efficiency: Queries like total revenue per customer or sales by product and day are simplified.

Flexibility: Cancellations and outliers are handled via transaction_type and separate tables, preserving data integrity.