/*
===================================================================================
Stored Procedure: Creating Dim Customers in Gold Layer
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
spark.sql("DROP TABLE IF EXISTS gold.dim_customers")


query = """
SELECT
    ROW_NUMBER() OVER (ORDER BY ci.customer_id) AS customer_key,
    ci.customer_id,
    ci.customer_key AS customer_number,
    ci.firstname,
    ci.lastname,
    la.country,
    ci.marital_status,
    CASE WHEN ci.gender != 'n/a' THEN ci.gender -- CRM is the Master for gender info
        ELSE COALESCE(ca.gender, 'n/a')
    END AS gender,
    ca.birth_date,
    ci.created_date
FROM silver.crm_customers ci
LEFT JOIN workspace.silver.erp_cust_az12 ca
    ON ci.customer_key = ca.customer_id
LEFT JOIN workspace.silver.erp_loc_a101 la
    ON ci.customer_key = la.customer_id
"""

df = spark.sql(query)


# Writing into gold table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("gold.dim_customers")
)

