-- Data Warehouse schema for eCommerce analytics
-- Star schema designed to support product performance
-- and time-of-day sales analysis

CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.fact_sales;
DROP TABLE IF EXISTS analytics.dim_date;
DROP TABLE IF EXISTS analytics.dim_product;
DROP TABLE IF EXISTS analytics.dim_customer;

CREATE TABLE analytics.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL UNIQUE,
    customer_name VARCHAR(100),
    email VARCHAR(150),
    country VARCHAR(50),
    registration_date TIMESTAMP
);

CREATE TABLE analytics.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id INTEGER NOT NULL UNIQUE,
    product_name VARCHAR(200),
    category VARCHAR(100),
    description TEXT,
    base_price NUMERIC(10,2),
    base_currency VARCHAR(3)
);

CREATE TABLE analytics.dim_date (
    date_key INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20) NOT NULL,
    hour INTEGER NOT NULL
);

CREATE TABLE analytics.fact_sales (
    sales_key BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    order_item_id INTEGER NOT NULL,
    customer_key INTEGER NOT NULL REFERENCES analytics.dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES analytics.dim_product(product_key),
    date_key INTEGER NOT NULL REFERENCES analytics.dim_date(date_key),
    order_date TIMESTAMP NOT NULL,
    order_currency VARCHAR(3) NOT NULL,
    fx_rate_to_usd NUMERIC(18,6),
    quantity INTEGER NOT NULL,
    unit_price NUMERIC(10,2) NOT NULL,
    gross_amount_local NUMERIC(12,2) NOT NULL,
    gross_amount_usd NUMERIC(12,2),
    order_status VARCHAR(20) NOT NULL
);

CREATE INDEX idx_fact_sales_order_date ON analytics.fact_sales(order_date);
CREATE INDEX idx_fact_sales_product ON analytics.fact_sales(product_key);
CREATE INDEX idx_fact_sales_customer ON analytics.fact_sales(customer_key);
CREATE INDEX idx_fact_sales_date_key ON analytics.fact_sales(date_key);
