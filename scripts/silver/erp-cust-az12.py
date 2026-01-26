/*
===================================================================================
Stored Procedure: Creating Cust_az12 in Silver Layer
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

# Dropping table if it exists
spark.sql("DROP TABLE IF EXISTS silver.erp_cust_az12")

# Reading from bronze table
df = spark.table("workspace.bronze.erp_cust_az12")


df = (df
      .withColumn(
      'cid', 
      F.expr("substring(cid, 4, length(cid))")
      )
      .withColumn(
      'bdate',
      F.when(F.col('bdate') > F.current_date(), None)
      .otherwise(F.col('bdate'))
      )
      .withColumn(
          "gen", 
          F.when(F.upper(F.trim(F.col("gen"))).isin(["F", "FEMALE"]), "Female")
          .when(F.upper(F.trim(F.col("gen"))).isin(["M", "MALE"]), "Male")
          .otherwise("n/a") 
      ) 
)



RENAME_MAP = {
    "cid": "customer_id",
    "bdate": "birth_date",
    "gen": "gender",
}

# Renaming the Columns
for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)

# Writing into silver tavle
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.erp_cust_az12")
)










