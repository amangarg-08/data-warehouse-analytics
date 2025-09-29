/*
=====================================================================================
In the Silver Layer, we copy cleaned and transformed data from the Bronze tables.
The Bronze tables remain unchanged, as they serve as the raw data source.
All data cleansing and processing is applied during the transfer into the Silver tables.
=====================================================================================
*/

-- CRM TABLES
-- silver_crm_cust_info
-- Checking for Nulls & Duplicates in Primary Key (cst_id)
SELECT 
	cst_id,
    COUNT(*)
FROM
bronze_crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- There are duplicates and nulls both in cst_id
-- we will rank rows by latest dates and partition by cst_id and we will select the most recent record per customer
SELECT 
	* 
FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze_crm_cust_info
		WHERE cst_id != 0
) AS t1
WHERE flag_last = 1;

-- Quality Check - Check for unwanted spaces in string Values
SELECT 
	cst_firstname 
FROM bronze_crm_cust_info 
WHERE cst_firstname != TRIM(cst_firstname);

-- While reviewing the columns, we identified unwanted spaces in cst_firstname and cst_lastname.
SELECT 
	cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cat_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date,
    flag_last
FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze_crm_cust_info
		WHERE cst_id != 0
) AS t1
WHERE flag_last = 1;

--  Data Standardization & Consistency
SELECT DISTINCT cst_marital_status
FROM bronze_crm_cust_info;

-- Normalize marital status & gender values to readable format
INSERT INTO silver_crm_cust_info (
			cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date)
SELECT 
	cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cat_lastname,
    CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
         ELSE 'n/a'
	END AS cst_marital_status,
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
         ELSE 'n/a'
	END AS cst_gndr,
    cst_create_date
FROM (
		SELECT 
			*,
			ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM bronze_crm_cust_info
		WHERE cst_id != 0
) AS t1
WHERE flag_last = 1;



-- silver_crm_prd_info
-- Checking for Nulls & Duplicates in Primary Key (prd_id)
SELECT 
	prd_id,
    COUNT(*)
FROM bronze_crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;
-- There are no duplicates and nulls in prd_id

-- prd_key column contains category ID & product key both lets split them
SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7) AS prd_key,
    prd_nm, 
    prd_cost, 
    prd_line, 
    prd_start_dt, 
    prd_end_dt
FROM bronze_crm_prd_info;

-- Quality Check - Check for unwanted spaces in prd_nm
SELECT 
	prd_nm
FROM bronze_crm_prd_info 
WHERE prd_nm != TRIM(prd_nm);
-- No unwanted spaces in prd_nm

-- Check for Nulls & Negative numbers in prd_cost
SELECT
	prd_cost
FROM bronze_crm_prd_info 
WHERE prd_cost <= 0 OR prd_cost IS NULL ;
-- There are 2 values which are zero

SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7) AS prd_key,
    prd_nm, 
    IFNULL (prd_cost, 0) AS prd_cost, 
    prd_line, 
    prd_start_dt, 
    prd_end_dt
FROM bronze_crm_prd_info;

-- Map product line codes to descriptive values and cast prd_start_dt as date
SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7) AS prd_key,
    prd_nm, 
    IFNULL (prd_cost, 0) AS prd_cost, 
    CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
	END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    prd_end_dt
FROM bronze_crm_prd_info;

-- Check for invalid dates in prd_end_dt
SELECT * 
FROM bronze_crm_prd_info
WHERE prd_start_dt > prd_end_dt;

-- Calculate end date as one day before the next start date
SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7) AS prd_key,
    prd_nm, 
    IFNULL (prd_cost, 0) AS prd_cost, 
    CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
	END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    DATE_SUB(CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE), INTERVAL 1 DAY) AS prd_end_dt
FROM bronze_crm_prd_info;

-- Insert final table into silver_crm_prd_info
INSERT INTO silver_crm_prd_info (
				prd_id, 
                cat_id,
                prd_key, 
                prd_nm, 
                prd_cost, 
                prd_line, 
                prd_start_dt, 
                prd_end_dt)
	SELECT
	prd_id,
    REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
    SUBSTRING(prd_key, 7) AS prd_key,
    prd_nm, 
    IFNULL (prd_cost, 0) AS prd_cost, 
    CASE 
		WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
        WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
        WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
        WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
        ELSE 'n/a'
	END AS prd_line,
    CAST(prd_start_dt AS DATE) AS prd_start_dt,
    DATE_SUB(CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) AS DATE), INTERVAL 1 DAY) AS prd_end_dt
FROM bronze_crm_prd_info;


-- silver_crm_sales_details
-- Checking for Nulls in Primary Key (sls_ord_num)
SELECT 
	sls_ord_num,
    COUNT(*)
FROM bronze_crm_sales_details
GROUP BY sls_ord_num
HAVING sls_ord_num IS NULL;
-- There are no nulls in sls_ord_num

-- Quality Check - Check for unwanted spaces in sls_ord_num
SELECT 
	sls_ord_num
FROM bronze_crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);
-- No unwanted spaces in sls_ord_num

-- Checking sls_prd_key and sls_cust_id for unwanted values
SELECT *
FROM bronze_crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver_crm_prd_info);

SELECT *
FROM bronze_crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver_crm_cust_info);
-- There is no unwanted value in sls_prd_key and sls_cust_id

-- Check for invalid dataes in sls_order_dt, sls_ship_dt, sls_due_dt
SELECT 
	sls_order_dt 
FROM bronze_crm_sales_details 
WHERE sls_order_dt <= 0 OR LENGTH(sls_order_dt) != 8;

SELECT 
	sls_ship_dt 
FROM bronze_crm_sales_details 
WHERE sls_ship_dt <= 0 OR LENGTH(sls_ship_dt) != 8;

SELECT 
	sls_due_dt 
FROM bronze_crm_sales_details 
WHERE sls_due_dt <= 0 OR LENGTH(sls_due_dt) != 8;
-- we can see there are unwanted and null values in sls_order_dt but sls_ship_dt and sls_due_dt are good to go

-- Check for invalid dates order
Select * 
FROM bronze_crm_sales_details 
WHERE sls_order_dt >  sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Converting sls_order_dt, sls_ship_dt, sls_due_dt format from string to proper date format
SELECT 
	sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
    CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
	END AS sls_order_dt, 
    CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
	END AS sls_ship_dt,
    CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
	END AS sls_due_dt,
    sls_sales, 
    sls_quantity, 
    sls_price
FROM bronze_crm_sales_details;

-- Check Data Consistency: Between Sales, Quantity & Price
-- Sales = Quantity * Price
-- Values must not be Null, 0 or negative
SELECT 
	sls_sales,
    sls_quantity,
    sls_price
FROM bronze_crm_sales_details
WHERE sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 OR
sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR
sls_sales != sls_quantity * sls_price 
ORDER BY sls_sales, sls_quantity, sls_price ;
-- As we can see there are many bad values lets clean them


-- Recalculate sales if original value is missing or incorrect
-- Derive price if original value is invalid
SELECT 
	sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
	CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
	END AS sls_order_dt, 
    CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
	END AS sls_ship_dt,
    CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
	END AS sls_due_dt,
    CASE
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
	END AS sls_sales,
    sls_quantity, 
    CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
	END AS sls_price
FROM bronze_crm_sales_details;


-- Insert final table into silver_crm_sales_details
INSERT INTO silver_crm_sales_details(
		sls_ord_num, 
        sls_prd_key, 
        sls_cust_id, 
        sls_order_dt, 
        sls_ship_dt, 
        sls_due_dt, 
        sls_sales, 
        sls_quantity, 
        sls_price)
	SELECT 
	sls_ord_num, 
    sls_prd_key, 
    sls_cust_id, 
	CASE 
		WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_order_dt, '%Y%m%d')
	END AS sls_order_dt, 
    CASE 
		WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_ship_dt, '%Y%m%d')
	END AS sls_ship_dt,
    CASE 
		WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
        ELSE STR_TO_DATE(sls_due_dt, '%Y%m%d')
	END AS sls_due_dt,
    CASE
		WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
	END AS sls_sales,
    sls_quantity, 
    CASE 
		WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
	END AS sls_price
FROM bronze_crm_sales_details;


-- ERP TABLES
-- silver_erp_cust_az12
-- Checking for Nulls in Primary Key (cid)
SELECT 
	cid 
FROM bronze_erp_cust_az12 
WHERE cid IS NULL;
-- There are no nulls in cid

-- There is prefix NAS in cid which do not match with cust_info table cid_key
SELECT cid 
FROM bronze_erp_cust_az12
WHERE cid NOT IN (SELECT cst_key FROM bronze_crm_cust_info);

-- Remove 'NAS' prefix if present
SELECT 
    CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
	END AS cid,
    bdate, 
	gen
FROM bronze_erp_cust_az12;


-- unwanted values in cid which do not match with cust_info cid_key
SELECT * FROM
(SELECT 
    CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
	END AS cid,
    bdate, 
	gen
FROM bronze_crm_cust_info) AS t
WHERE cid NOT IN (SELECT cst_key FROM silver_crm_cust_info) OR cid IS NULL ;

-- SELECT Only cid values which match with cst_key in cust_info
SELECT * FROM
(SELECT 
    CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
	END AS cid,
    bdate, 
	gen
FROM bronze_erp_cust_az12) AS t
WHERE cid IN (SELECT cst_key FROM silver_crm_cust_info);

-- check for wrong date of birth
SELECT 
	bdate
FROM bronze_erp_cust_az12
WHERE bdate > NOW();
-- We can see that many dates of birth are set in the future, which is not valid

-- -- Set future birthdates to NULL
SELECT * FROM
(SELECT 
    CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
	END AS cid,
    CASE 
		WHEN bdate > NOW() THEN NULL
        ELSE bdate
	END AS bdate,
	gen
FROM bronze_erp_cust_az12) AS t
WHERE cid IN (SELECT cst_key FROM silver_crm_cust_info) ;

-- Quality Check - Check for unwanted values in gen
SELECT 
	DISTINCT gen 
FROM bronze_erp_cust_az12;
--  As we can see there are many unwanted values in gender

-- HEX(gen) will show you the exact character codes (spaces, tabs, etc.).
SELECT 
	DISTINCT gen, HEX(gen)
FROM bronze_erp_cust_az12;

-- Normalize gender values and handle unknown case
SELECT * FROM
(SELECT 
    CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
	END AS cid,
    CASE 
		WHEN bdate > NOW() THEN NULL
        ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M', 'MALE') THEN 'Male'
		ELSE NULL
	END AS gen
FROM bronze_erp_cust_az12) AS t
WHERE cid IN (SELECT cst_key FROM silver_crm_cust_info);


-- Inserting Data Into: silver_erp_cust_az12
INSERT INTO silver_erp_cust_az12 (
			cid,
			bdate,
			gen)
SELECT * FROM
(SELECT 
    CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
        ELSE cid
	END AS cid,
    CASE 
		WHEN bdate > NOW() THEN NULL
        ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(REPLACE(REPLACE(gen, '\r', ''), '\n', ''))) IN ('M', 'MALE') THEN 'Male'
		ELSE NULL
	END AS gen
FROM bronze_erp_cust_az12) AS t
WHERE cid IN (SELECT cst_key FROM silver_crm_cust_info);


-- silver_erp_loc_a101
-- Checking for Nulls in Primary Key (cid)
SELECT 
	cid 
FROM bronze_erp_loc_a101 
WHERE cid IS NULL;
-- There are no nulls in cid

-- There is - in cid which do not match with cust_info table cid_key
SELECT cid 
FROM bronze_erp_loc_a101
WHERE cid NOT IN (SELECT cst_key FROM bronze_crm_cust_info);

-- replacing - in cid column
SELECT
    REPLACE(cid, '-', '') AS cid,
    cntry
FROM bronze_erp_loc_a101;

-- check for missing or blank country codes
SELECT 
	DISTINCT cntry,
    COUNT(*)
 FROM bronze_erp_loc_a101
 GROUP BY cntry;
-- there are different name for united states aand germany also there are many null and blank values

-- lets use HEX to see what are blank values
SELECT 
	DISTINCT cntry,
    HEX(cntry)
 FROM bronze_erp_loc_a101
;

-- Normalize and Handle missing or blank country codes
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
		WHEN TRIM(REPLACE(cntry, '\r', '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, '\r', '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR '\r' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''))
	END AS cntry
FROM bronze_erp_loc_a101;


-- Inserting Data Into: silver_erp_loc_a101
INSERT INTO silver_erp_loc_a101(
	cid,
	cntry)
SELECT
    REPLACE(cid, '-', '') AS cid,
    CASE 
		WHEN TRIM(REPLACE(cntry, '\r', '')) = 'DE' THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, '\r', '')) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(cntry) = '' OR '\r' OR cntry IS NULL THEN 'n/a'
		ELSE TRIM(REPLACE(REPLACE(cntry, '\r', ''), '\n', ''))
	END AS cntry
FROM bronze_erp_loc_a101;

-- silver_erp_px_cat_g1v2
-- check for unwanted spaces 
SELECT * 
FROM silver_erp_px_cat_g1v2
WHERE id != TRIM(id) OR cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);
-- these are no unwanted spaces

-- Data Standardization & Consistency
SELECT DISTINCT cat FROM  silver_erp_px_cat_g1v2;

-- Insert Values into silver_erp_px_cat_g1v2
INSERT INTO silver_erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance)
SELECT 
	id, 
	cat, 
    subcat, 
    maintenance 
FROM bronze_erp_px_cat_g1v2;