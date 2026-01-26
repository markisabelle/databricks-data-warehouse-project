/*
===================================================================================
Stored Procedure: Creating Product Info in Silver Layer
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

# Drop table it already exists
spark.sql("DROP TABLE IF EXISTS silver.crm_products")

# Reading from bronze table
df = spark.table("workspace.bronze.crm_prd_info")

w = Window.partitionBy("prd_key").orderBy("prd_start_dt")


df = (
    df.withColumn('cat_id', F.regexp_replace(F.col('prd_key').substr(1, 5), "-", "_")
    )
    .withColumn('prd_key', F.expr("substring(prd_key, 7, length(prd_key)-6)")
    )
    .fillna({'prd_cost': 0})
    .withColumn(
        "prd_line",
        F.when(F.upper(F.trim(F.col("prd_line"))) == "R", "Road")
        .when(F.upper(F.trim(F.col("prd_line"))) == "S", "other Sales")
        .when(F.upper(F.trim(F.col("prd_line"))) == "M", "Mountain")
        .when(F.upper(F.trim(F.col("prd_line"))) == "T", "Touring")
        .otherwise("n/a")
    )
    .withColumn(
            "prd_start_dt",
            F.col("prd_start_dt").cast("date")
    ).withColumn(
        "prd_end_dt",
        F.expr("""
            CAST(
                LEAD(prd_start_dt) OVER (
                    PARTITION BY prd_key
                    ORDER BY prd_start_dt
                ) - 1 AS DATE
            )
        """)
    )
)



RENAME_MAP = {
    "prd_id": "product_id",
    "prd_key": "product_key",
    "prd_nm": "product_name",
    "prd_cost": "product_cost",
    "prd_line": "product_line",
    "prd_start_dt": "product_start_date",
    "prd_end_dt": "product_end_date"
}

# Renaming the column names
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# Writing into silver Table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_products")
)














