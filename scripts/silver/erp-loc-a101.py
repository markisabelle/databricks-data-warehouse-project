/*
===================================================================================
Stored Procedure: Creating loc_a101 Info in Silver Layer
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
spark.sql("DROP TABLE IF EXISTS silver.erp_loc_a101")


# Reading from bronze table
df = spark.table("workspace.bronze.erp_loc_a101")



df = (df
    .withColumn(
    'cid', 
    F.regexp_replace(F.col('cid'), '-', '')
    )
    .withColumn(
        "cntry",
        F.when(F.trim(F.col("cntry")) == "DE", "Germany")
         .when(F.trim(F.col("cntry")).isin(["US", "USA"]), "United States")
         .when((F.trim(F.col("cntry")) == "") | (F.col("cntry").isNull()), "n/a")
         .otherwise(F.trim(F.col("cntry")))
    )
)



# Renaming the columns
RENAME_MAP = {
    "cid": "customer_id",
    "cntry": "country"
}

for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


# Writing into silver table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_loc_a101")
)












