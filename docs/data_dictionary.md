# Data Dictionary

This document provides a detailed description of all tables, columns, and their purposes within the Online Retail Data Pipeline warehouse. It covers the fact table, dimension tables, and the outliers quarantine table.

---

## Fact Table: `fact_sales`

**Purpose:** Stores individual sales transactions, including both completed and cancelled orders.

| Column               | Data Type      | Description                                      |
|----------------------|---------------|--------------------------------------------------|
| transaction_key       | SERIAL        | Surrogate primary key                             |
| invoice_no            | VARCHAR(10)   | Original invoice number                           |
| customer_key          | INT           | Foreign key referencing `dim_customers.customer_key` |
| product_key           | INT           | Foreign key referencing `dim_products.product_key`  |
| date_key              | DATE          | Foreign key referencing `dim_time.date_key`      |
| transaction_datetime  | TIMESTAMP     | Exact timestamp of the transaction              |
| quantity              | INT           | Number of units sold                             |
| unit_price            | DECIMAL(10,2) | Price per unit                                   |
| sign                  | SMALLINT      | +1 for sales, -1 for cancellations             |
| total_amount          | DECIMAL(12,2) | Calculated as `quantity * unit_price * sign`    |
| transaction_type      | VARCHAR(10)   | "SALE" or "CANCEL"                               |

**Notes:**  
- `transaction_key` ensures uniqueness and simplifies joins.  
- `sign` and `transaction_type` allow easy handling of cancellations.  
- `total_amount` is computed at the database level for query efficiency.

---

## Dimension Table: `dim_customers`

**Purpose:** Stores customer information for analysis and segmentation.

| Column        | Data Type    | Description                              |
|---------------|-------------|------------------------------------------|
| customer_key   | SERIAL       | Surrogate primary key                     |
| customer_id    | VARCHAR(5)   | Original 5-digit customer identifier     |
| country        | VARCHAR(50)  | Customer's country                        |

---

## Dimension Table: `dim_products`

**Purpose:** Stores product details to analyze sales by item.

| Column        | Data Type     | Description                             |
|---------------|--------------|-----------------------------------------|
| product_key    | SERIAL       | Surrogate primary key                     |
| stock_code     | VARCHAR(5)   | Original product code                     |
| description    | VARCHAR(255) | Product name                              |

---

## Dimension Table: `dim_time`

**Purpose:** Stores date-level information for time-based analytics.

| Column        | Data Type    | Description                             |
|---------------|-------------|-----------------------------------------|
| date_key       | DATE        | Date of the transaction                  |
| year           | INT         | Year of the transaction                  |
| month          | INT         | Month number                             |
| day            | INT         | Day of the month                         |
| weekday        | VARCHAR(10) | Name of the weekday                       |

---

## Quarantine Table: `outliers`

**Purpose:** Stores anomalous or exceptional transactions for manual review.

| Column                  | Data Type | Description                                |
|-------------------------|-----------|--------------------------------------------|
| (all columns same as `fact_sales`) | —         | Anomalous records quarantined for review  |

**Notes:**  
- Maintains the same structure as `fact_sales` for easy reinsertion after review.  
- Serves as a temporary quarantine to preserve warehouse integrity.

---

**Overall Notes:**  
- The warehouse follows a **star schema pattern**: one fact table (`fact_sales`) connected to three dimension tables.  
- Surrogate keys are used throughout for consistency and efficient joins.  
- The `outliers` table is excluded from the main schema but supports workflow transparency and data quality management.
