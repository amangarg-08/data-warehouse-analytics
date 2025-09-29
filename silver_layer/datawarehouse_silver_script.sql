/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
This is Silver Layer Script
This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' tables from the 'bronze' tables.
	 - Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
      
Usage Example:
    CALL load_silver();
===============================================================================
*/



-- CREATING TABLES
CREATE TABLE silver_crm_cust_info (
	cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);

CREATE TABLE silver_crm_prd_info (
    prd_id       INT,
    cat_id       VARCHAR(50),
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt   DATE
);

CREATE TABLE silver_crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt DATE,
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);

CREATE TABLE silver_erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);

CREATE TABLE silver_erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);


CREATE TABLE silver_erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);



/*
===============================================================================
In the Silver Layer, we copy cleaned and transformed data from the Bronze tables.
The Bronze tables remain unchanged, as they serve as the raw data source.
All data cleansing and processing is applied during the transfer into the Silver tables.
(for step-by-step cleaning refer to steps file)
===============================================================================
*/




-- LOADING DATA INTO TABLES 
DROP PROCEDURE IF EXISTS load_silver ;
DELIMITER $$
CREATE PROCEDURE load_silver()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    SET batch_start_time = NOW();
    
    -- LOADING CRM TABLES
    
    -- LOADING silver_crm_cust_info
     SET start_time = NOW();
     TRUNCATE TABLE silver_crm_cust_info;

	-- INSERTING DATA INTO silver_crm_cust_info 
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
     
	SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS silver_crm_cust_info_load_duration;


	-- LOADING silver_crm_prd_info
	SET start_time = NOW();
	TRUNCATE TABLE silver_crm_prd_info;

	-- INSERTING DATA INTO silver_crm_prd_info 
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
    
    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS silver_crm_prd_info_load_duration;
    
    
    -- LOADING silver_crm_sales_details
	SET start_time = NOW();
	TRUNCATE TABLE silver_crm_sales_details;

	-- INSERTING DATA INTO silver_crm_sales_details
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
			WHEN sls_price <= 0 OR sls_price IS NULL  THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price
	FROM bronze_crm_sales_details;

    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS silver_crm_sales_details_load_duration;
 
 
     -- LOADING silver_erp_cust_az12
	SET start_time = NOW();
	TRUNCATE TABLE silver_erp_cust_az12;

	-- INSERTING DATA INTO silver_erp_cust_az12
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

	SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS silver_erp_cust_az12_load_duration;


     -- LOADING silver_erp_loc_a101
	SET start_time = NOW();
	TRUNCATE TABLE silver_erp_loc_a101;

	-- INSERTING DATA INTO silver_erp_loc_a101
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

	SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS silver_erp_loc_a101_load_duration;


     -- LOADING silver_erp_px_cat_g1v2
	SET start_time = NOW();
	TRUNCATE TABLE silver_erp_px_cat_g1v2;


-- Inserting Data into silver_erp_px_cat_g1v2
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
    
    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS silver_erp_px_cat_g1v2_load_duration;
    
    
  -- Batch End
    SET batch_end_time = NOW();
    -- Loading Silver Layer is Completed
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS silver_tables_total_load_duration;
 
END $$

DELIMITER ;

CALL load_silver();
     
