/*
=============================
DDL Script: Create Gold Views
=============================

Script Purpose:

This script creates views for the Gold layer in the data warehouse.
The Gold layer represents the final dimension and fact tables (Star Schema)
Each view performs transformations and combines data from the Silver layer
to produce a clean, enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reporting.

====================================================================
*/


-- We have a scenerio here while integrating different tables that gender coming from crm and erp sources are not matching in few cases
-- like in few cases they are opposite to each other, one indicating male while the other table showing female to the same person
-- in some cases one source is havinh n/a or null as gender while the other is indicating proper gender

-- To resolve this issue we are assuming crm table as point of truth however in real world stakeholder or experts decide this
-- which source is more reliable

CREATE VIEW gold.dim_customers AS
SELECT 
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- this column will be working as surrogate key
cci.cst_id AS customer_id,
cci.cst_key AS customer_number,
cci.cst_firstname AS first_name,
cci.cst_lastname AS last_name,
CASE WHEN cci.cst_gndr != 'N/A'THEN cci.cst_gndr -- CRM is Master for gender information
	ELSE COALESCE(eca.gen,'N/A') 
END AS gender,
cci.cst_marital_status AS marital_status,
ela.cntry AS country,
eca.bdate AS birth_date,
cci.cst_create_date AS create_date
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eca
ON cci.cst_key = eca.cid
LEFT JOIN silver.erp_loc_a101 ela
ON cci.cst_key = ela.cid;


-- In this table we have historical data of product too 
-- but for now we dont need the historical data
-- so we will pick the current entries of each product only
-- cpi.prd_end_dt having null value indicates the latest record

CREATE VIEW gold.dim_products AS
SELECT 
ROW_NUMBER() OVER (ORDER by cpi.prd_start_dt, cpi.prd_key) AS product_key, -- surrogate key
cpi.prd_id AS product_id,
cpi.prd_key AS product_number,
cpi.prd_nm AS product_name,
cpi.cat_id AS category_id,
epc.cat AS category,
epc.subcat AS sub_category,
epc.maintenance,
cpi.prd_cost AS cost,
cpi.prd_line AS product_line,
cpi.prd_start_dt AS start_date
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 epc
ON cpi.cat_id = epc.id
WHERE cpi.prd_end_dt IS NULL; -- Filter out all historical data


-- This table will be working as fact tabkle as it is having event information
-- or transactional data
-- Moreover product key and customer id can be serve as connection with other dimension tables

CREATE VIEW gold.fact_sales AS
SELECT 
sls_ord_num AS order_number, 
pr.product_key, 
cu.customer_key, 
sls_order_dt AS order_date, 
sls_ship_dt AS shipping_date, 
sls_due_dt AS due_date, 
sls_sales AS sales_amount, 
sls_quantity AS quantity, 
sls_price AS price 
FROM silver.crm_sales_details csd
LEFT JOIN gold.dim_products pr
ON csd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON csd.sls_cust_id = cu.customer_id;