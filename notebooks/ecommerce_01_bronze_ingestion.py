# Databricks notebook source
from pyspark.sql import functions as F

BASE_PATH = "/Volumes/workspace/ecommerce/raw_files"

orders_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/olist_orders_dataset.csv")
)

customers_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/olist_customers_dataset.csv")
)

items_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/olist_order_items_dataset.csv")
)

products_raw = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(f"{BASE_PATH}/olist_products_dataset.csv")
)

print("Orders:", orders_raw.count())
print("Customers:", customers_raw.count())
print("Order items:", items_raw.count())
print("Products:", products_raw.count())

display(orders_raw.limit(5))

bronze_tables = {
    "bronze_orders": orders_raw,
    "bronze_customers": customers_raw,
    "bronze_order_items": items_raw,
    "bronze_products": products_raw,
}

for table_name, df in bronze_tables.items():
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"workspace.ecommerce.{table_name}")
    )

print("Bronze ingestion completed.")