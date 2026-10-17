# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT
# MAGIC     DATE_TRUNC('MONTH', order_date) AS sales_month,
# MAGIC     SUM(total_revenue) AS revenue,
# MAGIC     SUM(total_orders) AS orders,
# MAGIC     ROUND(
# MAGIC         SUM(total_revenue) /
# MAGIC         NULLIF(SUM(total_orders), 0),
# MAGIC         2
# MAGIC     ) AS average_order_value
# MAGIC FROM workspace.ecommerce.gold_daily_sales
# MAGIC GROUP BY DATE_TRUNC('MONTH', order_date)
# MAGIC ORDER BY sales_month;
# MAGIC
# MAGIC
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     product_category_name,
# MAGIC     items_sold,
# MAGIC     orders_count,
# MAGIC     ROUND(total_revenue, 2) AS total_revenue
# MAGIC FROM workspace.ecommerce.gold_product_performance
# MAGIC ORDER BY total_revenue DESC
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT
# MAGIC     customer_segment,
# MAGIC     COUNT(*) AS customers,
# MAGIC     ROUND(SUM(lifetime_revenue), 2) AS total_revenue,
# MAGIC     ROUND(AVG(lifetime_revenue), 2) AS average_customer_revenue
# MAGIC FROM workspace.ecommerce.gold_customer_metrics
# MAGIC GROUP BY customer_segment
# MAGIC ORDER BY total_revenue DESC;
# MAGIC
# MAGIC SELECT
# MAGIC     order_status,
# MAGIC     COUNT(*) AS total_orders
# MAGIC FROM workspace.ecommerce.silver_orders
# MAGIC GROUP BY order_status
# MAGIC ORDER BY total_orders DESC;