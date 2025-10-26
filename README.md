# CASE STUDY

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

**Overall Rationale:**  
This dimensional model follows the star schema pattern, with a central fact table (`fact_sales`) connected to dimension tables (`dim_customers`, `dim_products`, `dim_time`). It supports efficient analytical queries, reduces redundancy, and provides flexibility for aggregation and slicing of data along different dimensions. Surrogate keys simplify joins and ensure uniqueness across the schema.

Rationale: Simplifies reporting by precomputing useful time attributes, avoids repeated extraction logic from InvoiceDate.

Design Principles

Star Schema: Fact table at the center, dimension tables surrounding it.

Normalization: Dimension tables remove redundancy from the fact table.

Analytical efficiency: Queries like total revenue per customer or sales by product and day are simplified.

Flexibility: Cancellations and outliers are handled via transaction_type and separate tables, preserving data integrity.