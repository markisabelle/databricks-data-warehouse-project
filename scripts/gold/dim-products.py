/*
===================================================================================
Stored Procedure: Creating Dim Products in Gold Layer
===================================================================================
Script Purpose:
    This script create views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables.

    Each view performs transformations and combines data from the Silver layers
    to product a clean, enriched, and business ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===================================================================================
*/

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Dropping table if it already exists
spark.sql("DROP TABLE IF EXISTS gold.dim_products")

query= """
SELECT 
ROW_NUMBER() OVER (ORDER BY pn.product_start_date, pn.product_key) AS product_key,
pn.product_id,
pn.product_key AS product_number,
pn.product_name,
pn.cat_id AS category_id,
pc.category,
pc.subcategory,
pc.maintenance,
pn.product_cost AS cost,
pn.product_line,
pn.product_start_date AS start_date
FROM workspace.silver.crm_products pn
LEFT JOIN workspace.silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE product_end_date IS NULL -- Filter out all historical data
"""

df = spark.sql(query)
df.display()



# Writing into Gold table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.dim_products")
)


