# Databricks notebook source

from pyspark.sql import functions as F

def check_unique_key(table_name, key_columns):
    df = spark.table(table_name)

    # Count rows with at least one null key field
    null_condition = F.lit(False)

    for column_name in key_columns:
        null_condition = (
            null_condition | F.col(column_name).isNull()
        )

    null_key_rows = df.filter(null_condition).count()

    # Count duplicate key groups, excluding null-key rows
    valid_keys = df.filter(~null_condition)

    duplicate_key_groups = (
        valid_keys
        .groupBy(*key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    result = {
        "table": table_name,
        "null_key_rows": null_key_rows,
        "duplicate_key_groups": duplicate_key_groups,
        "passed": (
            null_key_rows == 0
            and duplicate_key_groups == 0
        )
    }

    print(result)
    return result


checks = [
    check_unique_key(
        "workspace.ecommerce.silver_orders",
        ["order_id"]
    ),
    check_unique_key(
        "workspace.ecommerce.silver_customers",
        ["customer_id"]
    ),
    check_unique_key(
        "workspace.ecommerce.silver_order_items",
        ["order_id", "order_item_id"]
    ),
    check_unique_key(
        "workspace.ecommerce.silver_products",
        ["product_id"]
    )
]

print("All primary-key checks passed:",
      all(check["passed"] for check in checks))


items = spark.table("workspace.ecommerce.silver_order_items")

invalid_prices = items.filter(
    F.col("price").isNull() |
    (F.col("price") < 0)
).count()

invalid_freight = items.filter(
    F.col("freight_value").isNull() |
    (F.col("freight_value") < 0)
).count()

print("Invalid prices:", invalid_prices)
print("Invalid freight values:", invalid_freight)

items = spark.table("workspace.ecommerce.silver_order_items")
orders = spark.table("workspace.ecommerce.silver_orders")
products = spark.table("workspace.ecommerce.silver_products")

orphan_orders = (
    items.select("order_id").distinct()
    .join(
        orders.select("order_id").distinct(),
        on="order_id",
        how="left_anti"
    )
    .count()
)

orphan_products = (
    items.select("product_id").distinct()
    .join(
        products.select("product_id").distinct(),
        on="product_id",
        how="left_anti"
    )
    .count()
)

print("Order references without matching orders:", orphan_orders)
print("Product references without matching products:", orphan_products)

expected_revenue = (
    spark.table("workspace.ecommerce.silver_order_items")
    .join(
        spark.table("workspace.ecommerce.silver_orders")
        .filter(F.col("order_status") == "delivered")
        .select("order_id"),
        on="order_id",
        how="inner"
    )
    .agg(F.sum("price").alias("revenue"))
    .first()["revenue"]
)

actual_revenue = (
    spark.table("workspace.ecommerce.gold_daily_sales")
    .agg(F.sum("total_revenue").alias("revenue"))
    .first()["revenue"]
)

print("Expected revenue:", expected_revenue)
print("Actual Gold revenue:", actual_revenue)

if expected_revenue == actual_revenue:
    print("PASS: Revenue reconciliation successful")
else:
    print("FAIL: Revenue mismatch detected")

