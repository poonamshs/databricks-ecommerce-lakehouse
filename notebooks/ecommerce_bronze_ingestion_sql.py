# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS workspace.ecommerce;
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.ecommerce.raw_files;
# MAGIC
# MAGIC SELECT COUNT(*) AS total_orders
# MAGIC FROM workspace.ecommerce.bronze_orders;
# MAGIC
# MAGIC SELECT *
# MAGIC FROM workspace.ecommerce.bronze_orders
# MAGIC LIMIT 10;
# MAGIC
# MAGIC SELECT 'orders' AS dataset, COUNT(*) AS row_count
# MAGIC FROM workspace.ecommerce.silver_orders;
# MAGIC
# MAGIC SELECT 'customers', COUNT(*)
# MAGIC FROM workspace.ecommerce.silver_customers;
# MAGIC
# MAGIC SELECT 'order_items', COUNT(*)
# MAGIC FROM workspace.ecommerce.silver_order_items;
# MAGIC
# MAGIC SELECT 'products', COUNT(*)
# MAGIC FROM workspace.ecommerce.silver_products;
# MAGIC
# MAGIC SHOW TABLES IN workspace.ecommerce;
# MAGIC
# MAGIC
# MAGIC SELECT 'gold_daily_sales' AS table_name, COUNT(*) AS row_count
# MAGIC FROM workspace.ecommerce.gold_daily_sales;
# MAGIC
# MAGIC SELECT 'gold_product_performance', COUNT(*)
# MAGIC FROM workspace.ecommerce.gold_product_performance;
# MAGIC
# MAGIC SELECT 'gold_customer_metrics', COUNT(*)
# MAGIC FROM workspace.ecommerce.gold_customer_metrics;