/*
=========================================================
-- Steps for creating Dimension: gold_dim_customers
=========================================================
*/



-- lets join all customer tables
SELECT 
	ci.cst_id, 
    ci.cst_key, 
    ci.cst_firstname, 
    ci.cst_lastname, 
    ci.cst_marital_status, 
    ci.cst_gndr, 
    ci.cst_create_date,
    cb.bdate,
    cb.gen,
    cl.cntry
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS cb
ON ci.cst_key = cb.cid
LEFT JOIN silver_erp_loc_a101 AS cl
ON ci.cst_key = cl.cid;

-- Now we will check for duplicate data
SELECT cst_id, COUNT(*) FROM
	(SELECT 
		ci.cst_id, 
		ci.cst_key, 
		ci.cst_firstname, 
		ci.cst_lastname, 
		ci.cst_marital_status, 
		ci.cst_gndr, 
		ci.cst_create_date,
		cb.bdate,
		cb.gen,
		cl.cntry
	FROM silver_crm_cust_info AS ci
	LEFT JOIN silver_erp_cust_az12 AS cb
	ON ci.cst_key = cb.cid
	LEFT JOIN silver_erp_loc_a101 AS cl
	ON ci.cst_key = cl.cid) as t1
GROUP BY cst_id
HAVING COUNT(*) > 1;
-- there is no duplicate data it means we have joined tables correctly

-- As we can see there are two similar columns cst_gndr and gen lets check them out
SELECT 
	DISTINCT
    ci.cst_gndr,
    cb.gen
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS cb
ON ci.cst_key = cb.cid
LEFT JOIN silver_erp_loc_a101 AS cl
ON ci.cst_key = cl.cid;
-- we can see there are few errors (some rows do not match) 
-- CRM is the primary source for gender
SELECT 
    CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(cb.gen, 'n/a')
	END AS gender
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS cb
ON ci.cst_key = cb.cid
LEFT JOIN silver_erp_loc_a101 AS cl
ON ci.cst_key = cl.cid;
-- Now we can add this in table

SELECT 
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    cl.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
	CASE 
		WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(cb.gen, 'n/a')
    END                                AS gender,
    cb.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS cb
ON ci.cst_key = cb.cid
LEFT JOIN silver_erp_loc_a101 AS cl
ON ci.cst_key = cl.cid;

-- lets final nice names of each columns and also add a surrogate key
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
    ci.cst_id                          AS customer_id,
    ci.cst_key                         AS customer_number,
    ci.cst_firstname                   AS first_name,
    ci.cst_lastname                    AS last_name,
    la.cntry                           AS country,
    ci.cst_marital_status              AS marital_status,
    CASE 
        WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
        ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
    END                                AS gender,
    ca.bdate                           AS birthdate,
    ci.cst_create_date                 AS create_date
FROM silver_crm_cust_info AS ci
LEFT JOIN silver_erp_cust_az12 AS cb
ON ci.cst_key = cb.cid
LEFT JOIN silver_erp_loc_a101 AS cl
ON ci.cst_key = cl.cid;


-- Steps for creating Dimension: gold_dim_products
-- lets join all products tables
SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pn.prd_end_dt,
	pc.cat,
    pc.subcat,
    pc.maintenance
FROM silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id;

-- Now we will check for duplicate data
SELECT prd_id, COUNT(*) FROM
	(SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pn.prd_end_dt,
	pc.cat,
    pc.subcat,
    pc.maintenance
FROM silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id) as t1
GROUP BY prd_id
HAVING COUNT(*) > 1;
-- there is no duplicate data it means we have joined tables correctly

-- we don't want historical data
SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pn.cat_id,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
	pc.cat,
    pc.subcat,
    pc.maintenance
FROM silver_crm_prd_info pn
LEFT JOIN silver_erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data

-- lets give better names to columns, adjust the columns sequence and add a surrogate key
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

-- Creating Fact Table: gold_fact_sales
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

