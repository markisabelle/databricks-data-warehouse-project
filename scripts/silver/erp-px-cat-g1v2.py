/*
===================================================================================
Stored Procedure: Creating px_cat_g1v2 in Silver Layer
===================================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' chema.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.
===================================================================================
*/


import pyspark.sql.functions as F
from pyspark.sql.types import StringType 
from pyspark.sql.functions import trim, col, row_number
from pyspark.sql.window import Window


# Dropping table if it already exists
spark.sql("DROP TABLE IF EXISTS silver.erp_px_cat_g1v2")


# Reading from bronze table
df = spark.table("workspace.bronze.erp_px_cat_g1v2")


# Renaming the columns
RENAME_MAP = {
    "id": "id",
    "cat": "category",
    "subcat": "subcategory",
    "maintenance": "maintenance"
}

for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


# Writing into silver table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_px_cat_g1v2")
)



