-- Data will be checked in this file
/*
Quality Checks
Script Purpose:
This script performs various quality checks for data consistency, accuracy, and standardization 
across the silver schemas. It includes checks for:
	- Null or duplicate primary keys.
	- Unwanted spaces in string fields.
	- Data standardization and consistency.
	- Invalid date ranges and orders.
	- Data consistency between related fields.
Usage Notes:
- Run these checks after loading Silver Layer.
- Investigate and resolve any discrepancies found during the checks.
*/

/*
==========================
Table Name: crm_cust_info
==========================
*/
-- Check for nulls or duplicates in primary key

select cst_id, count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id is null

-- We have some records having duplicate values and some records are having null key.

-- Check for unwanted spaces

select count(*)
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

-- 15 records having extra spaces in first name
-- 17 records having extra spaces in last name

-- Data Standardization and Consistency

select distinct cst_gndr
from bronze.crm_cust_info


/*
==========================
Table Name: crm_prd_info
==========================
*/

-- Check for nulls or duplicates in primary key

select prd_id, count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 OR prd_id is null

-- All records are good

-- Check for unwanted spaces

select count(*)
from silver.crm_prd_info
where prd_nm != trim(prd_nm)

-- All records are good

-- Check for negative cost or nulls

select count(*), prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null
group by prd_cost

-- 2 records are having null as cost

-- Data Standardization and Consistency

select distinct prd_line
from silver.crm_prd_info

-- M,R,S,T

-- Check for invalid date order

select *
from silver.crm_prd_info
where prd_start_dt > prd_end_dt

-- 200 records found where end data is way older(past) than its start date


/*
==========================
Table Name: crm_sales_details
==========================
*/

-- Check for invalid dates

select NULLIF(sls_order_dt,0) AS sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0
OR len(sls_order_dt) != 8
OR (sls_order_dt > 19000101 AND sls_order_dt < 20500101) -- just randomly setting date boundries to check outliers


-- we have records with 0 as date so we are making it null
-- we have date in integer with len 8 so have to convert it into date

-- Check for invalid date orders

select *
from silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt 

-- works fine

-- Check data consistency: sales, quantity and price
-- >> sales = quantity * price
-- >> values must not be null, zero, or negative

-- Let think we have been given these rules:
-- If Sales is negative, zero, or null, derive it using Quantity and Price
-- If Price is zero or null, calculate it using Sales and Quantity
-- If Price is negative, convert it to a positive value

select 
sls_sales AS old_sales,
sls_quantity AS old_quantity,
sls_price AS old_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales is null OR sls_quantity  is null OR sls_price is null
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price 

-- we are having null, -ve and 0 sales however other fields depicting diff values
-- same behaviour from price column

select * from silver.crm_sales_details


/*
==========================
Table Name: erp_cust_az12
==========================
*/

-- Checking the customer ids

select 
cid,
CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
	ELSE cid 
END AS cid,
CASE WHEN bdate > GETDATE() THEN NULL
	ELSE bdate
END bdate,
gen,
CASE WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	ELSE 'n/a'
END as gen
from bronze.erp_cust_az12

-- cid has reference in crm_cust_info but here there is additional letter padded in start of it which we donot have any info about it

-- Identifying out of range dates

select 
distinct bdate
from bronze.erp_cust_az12
where bdate < '1925-01-01' OR bdate > GETDATE()

-- we have such abnormal date ranges so we have to cater it according to business rules

-- Data Standardization and Consistency

select 
distinct gen
from bronze.erp_cust_az12


select * from silver.erp_cust_az12 

/*
==========================
Table Name: erp_loc_a101
==========================
*/

-- Data Standardization and Consistency + making column matching with other table so we can perform join

select distinct cntry
FROM silver.erp_loc_a101

select * from bronze.erp_loc_a101
-- null and short codes of countries


select * from silver.erp_loc_a101


/*
==========================
Table Name: erp_px_cat_g1v2
==========================
*/

-- Check for unwanted spaces

select *
from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)

-- Data Standardization and Consistency

select distinct maintenance
from bronze.erp_px_cat_g1v2

-- All are good

select * from silver.erp_px_cat_g1v2