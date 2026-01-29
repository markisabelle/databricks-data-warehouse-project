/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
    customer_id, 
        COUNT(*)
FROM silver.crm_customers
GROUP BY customer_id
HAVING COUNT(*) > 1 OR customer_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    customer_key 
FROM silver.crm_customers
WHERE customer_key != TRIM(customer_key);

-- Data Standardization & Consistency
SELECT DISTINCT 
    marital_status 
FROM silver.crm_customers;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT 
  product_id, 
  COUNT(*)
FROM workspace.silver.crm_products
GROUP BY product_id
HAVING COUNT(*) > 1 OR product_id IS NULL;

-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
  product_name
FROM silver.crm_products
WHERE product_name != TRIM(product_name);

-- Check for NULLs or Negative Values in Cost
-- Expectation: No Results
SELECT 
  product_cost
FROM silver.crm_products
WHERE product_cost < 0 OR product_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT 
    product_line 
FROM silver.crm_products;

-- Check for Invalid Date Orders (Start Date > End Date)
-- Expectation: No Results
SELECT *
FROM silver.crm_products
WHERE product_end_date < product_start_date;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for Invalid Dates
-- Expectation: No Invalid Dates
SELECT 
    NULLIF(sales_due_date, 0) AS sales_due_date
FROM silver.crm_sales
WHERE sales_due_date <= 0 
    OR LEN(sales_due_date) != 8
    OR sales_due_date > 20500101
    OR sales_due_date < 19000101;

-- Check for Invalid Date Orders (Order Date > Shipping/Due Dates)
-- Expectation: No Results
SELECT *
FROM silver.crm_sales
WHERE sales_order_date > sales_ship_date 
    OR sales_order_date > sales_due_date;

-- Check Data Consistency: Sales = Quantity * Price
-- Expectation: No Results
SELECT DISTINCT 
  sales_sales, 
  sales_quantity, 
  sales_price
FROM silver.crm_sales
WHERE sales_sales != sales_quantity * sales_price
  OR sales_sales IS NULL 
  OR sales_quantity IS NULL 
  OR sales_price IS NULL 
  OR sales_sales <= 0 
  OR sales_quantity <= 0 
  OR sales_price <= 0
ORDER BY sales_sales, sales_quantity, sales_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Identify Out-of-Range Dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT DISTINCT 
    bdate 
FROM silver.erp_cust_az12
WHERE birth_date < '1924-01-01' 
   OR birth_date > GETDATE();

-- Data Standardization & Consistency
SELECT DISTINCT 
    gender 
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Data Standardization & Consistency
SELECT DISTINCT 
    country 
FROM silver.erp_loc_a101
ORDER BY country;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for Unwanted Spaces
-- Expectation: No Results
SELECT 
    * 
FROM silver.erp_px_cat_g1v2
WHERE category != TRIM(category) 
   OR subcategory != TRIM(subcategory) 
   OR maintenance != TRIM(maintenance);

-- Data Standardization & Consistency
SELECT DISTINCT 
    maintenance 
FROM silver.erp_px_cat_g1v2;
