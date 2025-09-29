/* 
==============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
    
    ** In the Gold layer, we’re not storing raw/cleaned data anymore. 
    We’re only shaping data into a business-friendly format. 
    That’s why views are often used.
===============================================================================
*/


-- Create Dimension: gold_dim_customers
CREATE VIEW gold_dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    cl.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(cb.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    cb.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS cb
ON ci.cst_key = cb.cid
LEFT JOIN silver_erp_loc_a101 AS cl
ON ci.cst_key = cl.cid;


-- Create Dimension: gold_dim_products
CREATE VIEW gold_dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data


-- Create Fact Table: gold_fact_sales
CREATE VIEW gold_fact_sales AS 
SELECT 
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver_crm_sales_details AS sd
LEFT JOIN gold_dim_products AS pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold_dim_customers AS cu
ON sd.sls_cust_id = cu.customer_id;

SELECT * FROM gold_dim_customers;
SELECT * FROM gold_dim_products;
SELECT * FROM gold_fact_sales;



