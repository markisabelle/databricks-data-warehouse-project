"""
===================================================================================
Stored Procedure: Creating Fact Sales in Gold Layer
===================================================================================
Script Purpose:
    This script create views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables.

    Each view performs transformations and combines data from the Silver layers
    to product a clean, enriched, and business ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===================================================================================
"""


import pyspark.sql.functions as F
from pyspark.sql.window import Window


# Dropping table if it already exists
spark.sql("DROP TABLE IF EXISTS gold.fact_sales")


query = """
SELECT
sd.sales_order_number AS order_number,
pr.product_key,
cu.customer_key,
sd.sales_order_date AS order_date,
sd.sales_ship_date AS shipping_date,
sd.sales_due_date AS due_date,
sd.sales_sales AS sales_amount,
sd.sales_quantity AS amount,
sd.sales_price AS price
FROM workspace.silver.crm_sales sd
LEFT JOIN gold.dim_products pr
    ON sd.sales_product_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sales_customer_id = cu.customer_id
"""

df = spark.sql(query)
df.display()


# Writing into Gold table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.fact_sales")
)















