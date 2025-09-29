/*
===============================================================================
DDL Script: Create Bronze Tables
Stored Procedure: Load Bronze Layer (Source -> datawarehouse)
===============================================================================
Script Purpose:
    This script creates tables in the 'datawarehouse' database.
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze_load_bronze;
===============================================================================
*/



USE mysql ;

-- Creates a new database named 'DataWarehouse'

CREATE DATABASE datawarehouse;

USE datawarehouse;


-- CREATING TABLES
CREATE TABLE bronze_crm_cust_info (
	cst_id              INT,
    cst_key             VARCHAR(50),
    cst_firstname       VARCHAR(50),
    cst_lastname        VARCHAR(50),
    cst_marital_status  VARCHAR(50),
    cst_gndr            VARCHAR(50),
    cst_create_date     DATE
);


CREATE TABLE bronze_crm_prd_info (
    prd_id       INT,
    prd_key      VARCHAR(50),
    prd_nm       VARCHAR(50),
    prd_cost     INT,
    prd_line     VARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt   DATETIME
);


CREATE TABLE bronze_crm_sales_details (
    sls_ord_num  VARCHAR(50),
    sls_prd_key  VARCHAR(50),
    sls_cust_id  INT,
    sls_order_dt INT,
    sls_ship_dt  INT,
    sls_due_dt   INT,
    sls_sales    INT,
    sls_quantity INT,
    sls_price    INT
);


CREATE TABLE bronze_erp_loc_a101 (
    cid    VARCHAR(50),
    cntry  VARCHAR(50)
);


CREATE TABLE bronze_erp_cust_az12 (
    cid    VARCHAR(50),
    bdate  DATE,
    gen    VARCHAR(50)
);


CREATE TABLE bronze_erp_px_cat_g1v2 (
    id           VARCHAR(50),
    cat          VARCHAR(50),
    subcat       VARCHAR(50),
    maintenance  VARCHAR(50)
);




-- LOADING DATA INTO TABLES 

DROP PROCEDURE IF EXISTS load_bronze ;

DELIMITER $$
CREATE PROCEDURE load_bronze()
BEGIN
    DECLARE start_time DATETIME;
    DECLARE end_time DATETIME;
    DECLARE batch_start_time DATETIME;
    DECLARE batch_end_time DATETIME;

    SET batch_start_time = NOW();


    -- CRM TABLES
    
    -- CRM_CUST_INFO
    SET start_time = NOW();
    TRUNCATE TABLE bronze_crm_cust_info;
    
    -- Inserting Data Into: bronze_crm_cust_info
    LOAD DATA LOCAL INFILE 'C:/Users/Lenovo/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
    INTO TABLE bronze_crm_cust_info
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS bronze_crm_cust_info_load_duration;


    -- CRM_PRD_INFO
    SET start_time = NOW();
    TRUNCATE TABLE bronze_crm_prd_info;
    
    -- Inserting Data Into: bronze_crm_prd_info
    LOAD DATA LOCAL INFILE 'C:/Users/Lenovo/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
    INTO TABLE bronze_crm_prd_info
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS bronze_crm_prd_info_load_duration;


    -- CRM_SALES_DETAILS
    SET start_time = NOW();
    TRUNCATE TABLE bronze_crm_sales_details;
    
    -- Inserting Data Into: bronze_crm_sales_details
    LOAD DATA LOCAL INFILE 'C:/Users/Lenovo/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
    INTO TABLE bronze_crm_sales_details
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS bronze_crm_sales_details_load_duration;




    -- ERP TABLES

    -- ERP_LOC_A101
    SET start_time = NOW();
    TRUNCATE TABLE bronze_erp_loc_a101;
    
    -- Inserting Data Into: bronze_erp_loc_a101
    LOAD DATA LOCAL INFILE 'C:/Users/Lenovo/Downloads/sql-data-warehouse-project/datasets/source_erp/loc_a101.csv'
    INTO TABLE bronze_erp_loc_a101
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS bronze_erp_loc_a101_load_duration;


    -- ERP_CUST_AZ12
    SET start_time = NOW();
    TRUNCATE TABLE bronze_erp_cust_az12;
    
    -- Inserting Data Into: bronze_erp_cust_az12 

    LOAD DATA LOCAL INFILE 'C:/Users/Lenovo/Downloads/sql-data-warehouse-project/datasets/source_erp/cust_az12.csv'
    INTO TABLE bronze_erp_cust_az12
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS bronze_erp_cust_az12_load_duration;


    -- ERP_PX_CAT_G1V2
    SET start_time = NOW();
    TRUNCATE TABLE bronze_erp_px_cat_g1v2;
    -- Inserting Data Into: bronze_erp_px_cat_g1v2

    LOAD DATA LOCAL INFILE 'C:/Users/Lenovo/Downloads/sql-data-warehouse-project/datasets/source_erp/px_cat_g1v2.csv'
    INTO TABLE bronze_erp_px_cat_g1v2
    FIELDS TERMINATED BY ',' 
    IGNORE 1 ROWS;

    SET end_time = NOW();
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, start_time, end_time), ' seconds') AS bronze_erp_px_cat_g1v2_load_duration;


    -- Batch End
    SET batch_end_time = NOW();
    -- Loading Bronze Layer is Completed
    SELECT CONCAT(TIMESTAMPDIFF(SECOND, batch_start_time, batch_end_time), ' seconds') AS bronze_tables_total_load_duration;

END $$

DELIMITER ;

CALL load_bronze();

