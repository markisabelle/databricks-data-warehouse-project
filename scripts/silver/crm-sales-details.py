/*
===================================================================================
Stored Procedure: Creating Sales Details Info in Silver Layer
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

spark.sql("DROP TABLE IF EXISTS silver.crm_sales")

# Reading from bronze table
df = spark.table("workspace.bronze.crm_sales_details")


df = (df
      .withColumn(
        "sls_order_dt",
        F.when(
            (F.col("sls_order_dt") == 0) |
            (F.length(F.col("sls_order_dt").cast("string")) < 8),
            F.lit(None)
        ).otherwise(F.col("sls_order_dt"))
    )
      .withColumn(
          'sls_order_dt',
          F.to_date(F.col('sls_order_dt').cast('string'),'yyyyMMdd')
      )
      .withColumn(
        "sls_ship_dt",
        F.when(
            (F.col("sls_ship_dt") == 0) |
            (F.length(F.col("sls_ship_dt").cast("string")) < 8),
            F.lit(None)
        ).otherwise(F.col("sls_ship_dt"))
    )
      .withColumn(
          'sls_ship_dt',
          F.to_date(F.col('sls_ship_dt').cast('string'),'yyyyMMdd')
      )
      .withColumn(
        "sls_due_dt",
        F.when(
            (F.col("sls_due_dt") == 0) |
            (F.length(F.col("sls_due_dt").cast("string")) < 8),
            F.lit(None)
        ).otherwise(F.col("sls_due_dt"))
    )
      .withColumn(
          'sls_due_dt',
          F.to_date(F.col('sls_due_dt').cast('string'),'yyyyMMdd')
      )
      .withColumn(
            'sls_sales',
            F.when(
                  (F.col('sls_sales') <= 0) |
                  (F.col('sls_sales').isNull()) |
                  (F.col('sls_sales') != (F.col('sls_quantity') * F.abs(F.col('sls_price')))),
                  F.col('sls_quantity') * F.abs(F.col('sls_price'))
            ).otherwise(F.col('sls_sales'))
      )
      .withColumn(
          'sls_price',
          F.when(
              (F.col('sls_price').isNull()) |
              (F.col('sls_price') <= 0),
              F.col('sls_sales') / F.when(F.col('sls_quantity') != 0, F.col('sls_quantity')).otherwise(None)
          ).otherwise(F.col('sls_price'))
          
      )
)





RENAME_MAP = {
    "sls_ord_num": "sales_order_number",
    "sls_prd_key": "sales_product_key",
    "sls_cust_id": "sales_customer_id",
    "sls_order_dt": "sales_order_date",
    "sls_ship_dt": "sales_ship_date",
    "sls_due_dt": "sales_due_date",
    "sls_sales": "sales_sales",
    "sls_quantity": "sales_quantity",
    "sls_price": "sales_price"
}

# Renaming the columns
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)



# Writing into silver table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_sales")
)






