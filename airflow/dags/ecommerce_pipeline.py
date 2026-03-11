"""
ETL pipeline for Prosigliere Data Engineer Challenge.

Extracts eCommerce data from multiple PostgreSQL sources,
applies transformations including currency normalization,
and loads a dimensional data warehouse for analytics.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging
import requests
import pandas as pd
from sqlalchemy import create_engine, text


DB1_URI = "postgresql+psycopg2://postgres:postgres@ecommerce_db1:5432/ecommerce_orders"
DB2_URI = "postgresql+psycopg2://postgres:postgres@ecommerce_db2:5432/ecommerce_products"
DWH_URI = "postgresql+psycopg2://postgres:postgres@data_warehouse:5432/data_warehouse"
SCHEMA_SQL_PATH = "/opt/airflow/dags/warehouse/schema.sql"


default_args = {
    "owner": "hellen_data_engineer_candidate",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def get_engine(uri: str):
    return create_engine(uri)


def create_schema():
    dwh_engine = get_engine(DWH_URI)
    with open(SCHEMA_SQL_PATH, "r", encoding="utf-8") as f:
        ddl = f.read()

    with dwh_engine.begin() as conn:
        for statement in ddl.split(";"):
            stmt = statement.strip()
            if stmt:
                conn.execute(text(stmt))


def fetch_fx_rate_to_usd(currency: str) -> float:
    """
    Simples e defensivo:
    - USD -> 1
    - outras moedas tenta API
    - se falhar ou moeda inválida, retorna None
    """
    if currency == "USD":
        return 1.0

    try:
        url = f"https://api.exchangerate-api.com/v4/latest/{currency}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        rates = data.get("rates", {})
        usd_rate = rates.get("USD")
        return float(usd_rate) if usd_rate else None
    except Exception as e:
        logging.warning("Could not fetch FX rate for %s: %s", currency, e)
        return None


def extract_transform_load():
    engine_db1 = get_engine(DB1_URI)
    engine_db2 = get_engine(DB2_URI)
    engine_dwh = get_engine(DWH_URI)

    customers = pd.read_sql("SELECT * FROM customers", engine_db1)
    orders = pd.read_sql("SELECT * FROM orders", engine_db1)
    order_items = pd.read_sql("SELECT * FROM order_items", engine_db1)
    products = pd.read_sql("SELECT * FROM product_descriptions", engine_db2)

    # dims
    dim_customer = customers.rename(columns={
        "id": "customer_id",
        "name": "customer_name"
    })[["customer_id", "customer_name", "email", "country", "registration_date"]].drop_duplicates()

    dim_product = products.rename(columns={
        "id": "product_id",
        "name": "product_name",
        "currency": "base_currency"
    })[["product_id", "product_name", "category", "description", "base_price", "base_currency"]].drop_duplicates()

    # fact base
    fact = (
        order_items
        .merge(orders, left_on="order_id", right_on="id", suffixes=("_item", "_order"))
        .merge(customers, left_on="customer_id", right_on="id", suffixes=("", "_customer"))
        .merge(products, left_on="product_id", right_on="id", suffixes=("", "_product"))
    )

    fact["gross_amount_local"] = fact["quantity"] * fact["unit_price"]

    # fx
    unique_currencies = fact["currency_order"].dropna().unique().tolist()
    fx_map = {currency: fetch_fx_rate_to_usd(currency) for currency in unique_currencies}
    fact["fx_rate_to_usd"] = fact["currency_order"].map(fx_map)
    fact["gross_amount_usd"] = fact["gross_amount_local"] * fact["fx_rate_to_usd"]

    # dim_date
    fact["order_date"] = pd.to_datetime(fact["order_date"])
    dim_date = fact[["order_date"]].drop_duplicates().copy()
    dim_date["full_date"] = dim_date["order_date"].dt.date
    dim_date["year"] = dim_date["order_date"].dt.year
    dim_date["month"] = dim_date["order_date"].dt.month
    dim_date["day"] = dim_date["order_date"].dt.day
    dim_date["day_of_week"] = dim_date["order_date"].dt.dayofweek
    dim_date["day_name"] = dim_date["order_date"].dt.day_name()
    dim_date["hour"] = dim_date["order_date"].dt.hour
    dim_date["date_key"] = (
        dim_date["order_date"].dt.strftime("%Y%m%d").astype(int) * 100
        + dim_date["hour"]
    )
    dim_date = dim_date[[
        "date_key", "full_date", "year", "month", "day",
        "day_of_week", "day_name", "hour"
    ]].drop_duplicates()

    # load dimensions
    with engine_dwh.begin() as conn:
        conn.execute(text("TRUNCATE TABLE analytics.fact_sales RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE analytics.dim_date RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE analytics.dim_product RESTART IDENTITY CASCADE"))
        conn.execute(text("TRUNCATE TABLE analytics.dim_customer RESTART IDENTITY CASCADE"))

    dim_customer.to_sql("dim_customer", engine_dwh, schema="analytics", if_exists="append", index=False)
    dim_product.to_sql("dim_product", engine_dwh, schema="analytics", if_exists="append", index=False)
    dim_date.to_sql("dim_date", engine_dwh, schema="analytics", if_exists="append", index=False)

    # reload dims with surrogate keys
    d_customer = pd.read_sql("SELECT customer_key, customer_id FROM analytics.dim_customer", engine_dwh)
    d_product = pd.read_sql("SELECT product_key, product_id FROM analytics.dim_product", engine_dwh)
    d_date = pd.read_sql("SELECT date_key FROM analytics.dim_date", engine_dwh)

    fact["date_key"] = (
        fact["order_date"].dt.strftime("%Y%m%d").astype(int) * 100
        + fact["order_date"].dt.hour
    )

    fact_sales = (
        fact.merge(d_customer, on="customer_id", how="left")
            .merge(d_product, on="product_id", how="left")
    )

    fact_sales = fact_sales.rename(columns={
        "id_item": "order_item_id",
        "id_order": "order_id",
        "currency_order": "order_currency",
        "status": "order_status"
    })

    fact_sales = fact_sales[[
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "date_key",
        "order_date",
        "order_currency",
        "fx_rate_to_usd",
        "quantity",
        "unit_price",
        "gross_amount_local",
        "gross_amount_usd",
        "order_status"
    ]]

    fact_sales.to_sql("fact_sales", engine_dwh, schema="analytics", if_exists="append", index=False)

    logging.info("ETL completed successfully")


with DAG(
    dag_id="ecommerce_analytics_pipeline",
    default_args=default_args,
    description="ETL pipeline for Prosigliere data engineer challenge",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["challenge", "ecommerce", "analytics"],
) as dag:

    create_schema_task = PythonOperator(
        task_id="create_schema",
        python_callable=create_schema,
    )

    etl_task = PythonOperator(
        task_id="extract_transform_load",
        python_callable=extract_transform_load,
    )

    create_schema_task >> etl_task
