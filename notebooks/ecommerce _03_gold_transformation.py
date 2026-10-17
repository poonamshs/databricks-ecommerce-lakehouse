# Databricks notebook source
from pyspark.sql import functions as F

orders = spark.table("workspace.ecommerce.silver_orders")
items = spark.table("workspace.ecommerce.silver_order_items")
products = spark.table("workspace.ecommerce.silver_products")

# Only include delivered orders
delivered_orders = orders.filter(
    F.col("order_status") == "delivered"
)

# Join order items with delivered orders
sales_detail = (
    items
    .join(
        delivered_orders.select(
            "order_id",
            "customer_id",
            "order_purchase_timestamp"
        ),
        on="order_id",
        how="inner"
    )
    .withColumn(
        "revenue",
        F.col("price").cast("decimal(18,2)")
    )
    .withColumn(
        "freight",
        F.col("freight_value").cast("decimal(18,2)")
    )
    .withColumn(
        "order_date",
        F.to_date("order_purchase_timestamp")
    )
)

display(sales_detail.limit(10))

gold_daily_sales = (
    sales_detail
    .groupBy("order_date")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.count("*").alias("total_items"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("freight").alias("total_freight"),
        F.avg("revenue").alias("average_item_price")
    )
    .withColumn(
        "average_order_value",
        F.round(
            F.col("total_revenue") / F.col("total_orders"),
            2
        )
    )
)

(
    gold_daily_sales.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.gold_daily_sales")
)

display(
    spark.table("workspace.ecommerce.gold_daily_sales")
    .orderBy(F.desc("order_date"))
    .limit(10)
)

gold_product_performance = (
    sales_detail
    .join(
        products.select(
            "product_id",
            "product_category_name"
        ),
        on="product_id",
        how="left"
    )
    .groupBy(
        "product_id",
        "product_category_name"
    )
    .agg(
        F.countDistinct("order_id").alias("orders_count"),
        F.count("*").alias("items_sold"),
        F.sum("revenue").alias("total_revenue"),
        F.sum("freight").alias("total_freight")
    )
)

(
    gold_product_performance.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.gold_product_performance")
)

display(
    spark.table("workspace.ecommerce.gold_product_performance")
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

gold_customer_metrics = (
    sales_detail
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("delivered_orders"),
        F.sum("revenue").alias("lifetime_revenue"),
        F.max("order_purchase_timestamp").alias(
            "last_purchase_timestamp"
        )
    )
    .withColumn(
        "customer_segment",
        F.when(
            F.col("delivered_orders") >= 3, "Frequent"
        ).when(
            F.col("delivered_orders") == 2, "Repeat"
        ).otherwise("One-time")
    )
)

(
    gold_customer_metrics.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.ecommerce.gold_customer_metrics")
)

display(
    spark.table("workspace.ecommerce.gold_customer_metrics")
    .orderBy(F.desc("lifetime_revenue"))
    .limit(10)
)