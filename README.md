E-commerce Lakehouse Pipeline on Databricks
Overview
An end-to-end batch data engineering project built with Databricks, PySpark, Spark SQL, and Delta Lake. The pipeline transforms raw Brazilian e-commerce data into validated, business-ready analytics using Medallion Architecture.

Architecture
Source CSVs → Bronze → Silver → Gold → SQL Analytics and Dashboard

Bronze: Ingest raw CSV data into Delta tables.
Silver: Convert data types, deduplicate records, validate business rules, and quarantine invalid data.
Gold: Create analytics tables for daily sales, product performance, and customer metrics.
Technology Stack
Databricks Free Edition
Python and PySpark
Spark SQL
Delta Lake
Unity Catalog
GitHub
Dataset
Brazilian E-Commerce Public Dataset by Olist

Source: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Refer to the dataset page for applicable licensing and attribution requirements. The raw CSV files are not included in this repository.

Repository Structure

databricks-ecommerce-lakehouse/
├── README.md
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleaning.py
│   ├── 03_gold_transformations.py
│   ├── 04_data_quality.py
│   └── 05_sales_analysis.sql
├── docs/
│   ├── architecture.png
│   ├── dashboard.png
│   └── job_run.png
└── tests/
    └── validation_checks.py
Gold Tables
gold_daily_sales — daily revenue, order counts, freight, and average order value.
gold_product_performance — product-level sales and revenue metrics.
gold_customer_metrics — customer revenue, delivered orders, and purchase-frequency segments.
Data Quality
The pipeline is designed to check:

Null and duplicate business keys.
Invalid prices and freight values.
Missing order and product references.
Reconciliation between Silver and Gold revenue.
Business Questions
How does delivered revenue change over time?
Which products generate the most revenue?
How many customers are repeat or frequent buyers?
What is the distribution of order statuses?
Orchestration
The intended Databricks Job workflow runs Bronze ingestion, Silver cleaning, Gold transformations, and data quality checks in sequence.

Document the actual execution status and add a screenshot of a successful Job run once verified.

How to Run
Set up a Databricks workspace with the required permissions.
Download the source dataset from Kaggle.
Upload the required CSV files to an appropriate Unity Catalog volume.
Update catalog, schema, and file paths to match your workspace.
Run the notebooks in pipeline order.
Review data quality results and execute the SQL analytics queries.
Limitations and Future Improvements
This project uses batch processing and may rebuild tables on each run, depending on the notebook implementation. Potential improvements include incremental ingestion, Delta MERGE, late-arriving data handling, stronger automated testing, and pipeline monitoring.

