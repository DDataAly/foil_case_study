-- Dimension: Customers
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,
    customer_id VARCHAR(10) UNIQUE NOT NULL,
    country VARCHAR(100)
);

-- Dimension: Products
CREATE TABLE dim_products (
    product_key SERIAL PRIMARY KEY,
    stock_code VARCHAR(10) UNIQUE NOT NULL,
    description TEXT
);

-- Dimension: Time
CREATE TABLE dim_time (
    date_key DATE PRIMARY KEY,
    year INT,
    month INT,
    day INT,
    weekday VARCHAR(10)
);

-- Fact: Sales (includes both transactions and cancellations)
CREATE TABLE fact_sales (
    transaction_key SERIAL PRIMARY KEY,
    invoice_no VARCHAR(10) NOT NULL,
    customer_key INT REFERENCES dim_customers(customer_key),
    product_key INT REFERENCES dim_products(product_key),
    date_key DATE REFERENCES dim_time(date_key),
    transaction_datetime TIMESTAMP NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    sign SMALLINT,
    total_amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price * sign) STORED,
    transaction_type VARCHAR(10)
);

-- Quarantine table Outliers (includes both transactions and cancellations)
CREATE TABLE outliers (
    transaction_key SERIAL PRIMARY KEY,
    invoice_no VARCHAR(10) NOT NULL,
    customer_key INT REFERENCES dim_customers(customer_key),
    product_key INT REFERENCES dim_products(product_key),
    date_key DATE REFERENCES dim_time(date_key),
    transaction_datetime TIMESTAMP NOT NULL,
    quantity INT NOT NULL,
    unit_price DECIMAL(10,2) NOT NULL,
    sign SMALLINT,
    total_amount DECIMAL(12,2) GENERATED ALWAYS AS (quantity * unit_price * sign) STORED,
    transaction_type VARCHAR(10)
);