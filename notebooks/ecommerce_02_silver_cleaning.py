# Databricks notebook source
from pyspark.sql import functions as F

orders = spark.table("workspace.ecommerce.bronze_orders")

orders_clean = (
    orders
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )
    .dropDuplicates(["order_id"])
)

orders_invalid = orders_clean.filter(
    F.col("order_id").isNull()
    | F.col("customer_id").isNull()
    | F.col("order_purchase_timestamp").isNull()
)

orders_silver = orders_clean.filter(
    F.col("order_id").isNotNull()
    & F.col("customer_id").isNotNull()
    & F.col("order_purchase_timestamp").isNotNull()
)

(
    orders_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.silver_orders")
)

(
    orders_invalid.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.quarantine_orders")
)

print("Invalid orders:", orders_invalid.count())
print("Valid orders:", orders_silver.count())


customers = spark.table("workspace.ecommerce.bronze_customers")

customers_clean = (
    customers
    .dropDuplicates(["customer_id"])
)

customers_invalid = customers_clean.filter(
    F.col("customer_id").isNull()
    | F.col("customer_unique_id").isNull()
)

customers_silver = customers_clean.filter(
    F.col("customer_id").isNotNull()
    & F.col("customer_unique_id").isNotNull()
)

(
    customers_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.silver_customers")
)

(
    customers_invalid.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.quarantine_customers")
)


items = spark.table("workspace.ecommerce.bronze_order_items")

items_clean = (
    items
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    .withColumn("price", F.col("price").cast("decimal(12,2)"))
    .withColumn(
        "freight_value",
        F.col("freight_value").cast("decimal(12,2)")
    )
    .dropDuplicates(["order_id", "order_item_id"])
)

items_invalid = items_clean.filter(
    F.col("order_id").isNull()
    | F.col("order_item_id").isNull()
    | F.col("product_id").isNull()
    | F.col("price").isNull()
    | F.col("freight_value").isNull()
    | (F.col("price") < 0)
    | (F.col("freight_value") < 0)
)

items_silver = items_clean.filter(
    F.col("order_id").isNotNull()
    & F.col("order_item_id").isNotNull()
    & F.col("product_id").isNotNull()
    & F.col("price").isNotNull()
    & F.col("freight_value").isNotNull()
    & (F.col("price") >= 0)
    & (F.col("freight_value") >= 0)
)

(
    items_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.silver_order_items")
)

(
    items_invalid.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.quarantine_order_items")
)


products = spark.table("workspace.ecommerce.bronze_products")

products_clean = products.dropDuplicates(["product_id"])

products_invalid = products_clean.filter(
    F.col("product_id").isNull()
)

products_silver = products_clean.filter(
    F.col("product_id").isNotNull()
)

(
    products_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.silver_products")
)

(
    products_invalid.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.quarantine_products")
)