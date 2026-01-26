/*
=============================================================================
Create the First Layer of the Database
=============================================================================
Script Purpose:
    This script creates the first layer (Bronze Layer) in Data Warehouse after checking if it already exists.
    If the database exists, it is dropped and recreated.

*/



- Dropping Tables if they already exist
spark.sql("DROP TABLE IF EXISTS workspace.bronze.crm_cust_info")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.crm_prd_info")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.crm_sales_details")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.erp_cust_az12")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.erp_loc_a101")
spark.sql("DROP TABLE IF EXISTS workspace.bronze.erp_px_cat_g1v2")


- Reading from the CSV files
cust_info_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/workspace/bronze/source_system/source_crm/cust_info.csv")
prd_info_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/workspace/bronze/source_system/source_crm/prd_info.csv")
sales_details_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/workspace/bronze/source_system/source_crm/sales_details.csv")
cust_az12_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/workspace/bronze/source_system/source_erp/CUST_AZ12.csv")
loc_a101_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/workspace/bronze/source_system/source_erp/LOC_A101.csv")
px_cat_g1v2_df = spark.read.option("header", "true").option("inferSchema", "true").csv("/Volumes/workspace/bronze/source_system/source_erp/PX_CAT_G1V2.csv")



- Writing into the Bronze Layer
cust_info_df.write.mode("overwrite").saveAsTable("workspace.bronze.crm_cust_info")
prd_info_df.write.mode("overwrite").saveAsTable("workspace.bronze.crm_prd_info")
sales_details_df.write.mode("overwrite").saveAsTable("workspace.bronze.crm_sales_details")
cust_az12_df.write.mode("overwrite").saveAsTable("workspace.bronze.erp_cust_az12")
loc_a101_df.write.mode("overwrite").saveAsTable("workspace.bronze.erp_loc_a101")
px_cat_g1v2_df.write.mode("overwrite").saveAsTable("workspace.bronze.erp_px_cat_g1v2")

