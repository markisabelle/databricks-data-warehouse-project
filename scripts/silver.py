/*
===================================================================================
Stored Procedure: Creating Silver
*/





import pyspark.sql.functions as F
from pyspark.sql.types import StringType 
from pyspark.sql.functions import trim, col, row_number
from pyspark.sql.window import Window


# Dropping Table if already exists
spark.sql("DROP TABLE IF EXISTS silver.crm_customers")

# Reading From Bronze Table
df = spark.table("workspace.bronze.crm_cust_info")

window_spec = (
    Window
    .partitionBy("cst_id")
    .orderBy(col("cst_create_date").desc())
)

for field in df.schema.fields:
    if isinstance(field.dataType, StringType):
        df = df.withColumn(field.name, trim(col(field.name)))

df = (
    df.withColumn("flag_last", row_number().over(window_spec))
      .filter(col("flag_last") == 1)
      .drop("flag_last")
      .withColumn(
        "cst_marital_status",
        F.when(F.upper(F.col("cst_marital_status")) == "S", "Single")
        .when(F.upper(F.col("cst_marital_status")) == "M", "Married")
        .otherwise("n/a")
    )
    .withColumn(
        "cst_gndr",
        F.when(F.upper(F.col("cst_gndr")) == "M", "Male")
        .when(F.upper(F.col("cst_gndr")) == "F", "Female")
        .otherwise("n/a")
    )
)

# Renaming the Columns
RENAME_MAP = {
    "cst_id": "customer_id",
    "cst_key": "customer_key",
    "cst_firstname": "firstname",
    "cst_lastname": "lastname",
    "cst_marital_status": "marital_status",
    "cst_gndr": "gender",
    "cst_create_date": "created_date"
}

for old_name, new_name in RENAME_MAP.items():
    df = df.withColumnRenamed(old_name, new_name)


# Writing into Silver Table
(
    df.write
    .mode("overwrite")
    .format("delta")
    .saveAsTable("silver.crm_customers")
)
