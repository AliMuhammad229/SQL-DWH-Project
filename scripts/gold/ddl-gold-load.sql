CREATE OR ALTER VIEW Gold.dim_customer AS
	(SELECT
			ROW_NUMBER() OVER(ORDER BY cst_id) As customer_key,
			ci.cst_id AS customer_id,
			ci.cst_key AS customer_number,
			ci.cst_firstname AS first_name,
			ci.cst_lastname AS lasst_name,
			la.CNTRY AS country,
			ci.cst_marital_status AS marital_status,
			CASE 
				WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
				ELSE COALESCE(ca.GEN, 'n/a')
			END AS gender,
			ca.BDATE AS birth_date,
			cst_create_date AS create_date
	FROM Silver.crm_cust_info ci
	LEFT JOIN Silver.erp_CUST_AZ12 ca
	ON ci.cst_key = ca.CID
	LEFT JOIN Silver.erp_LOC_A101 la
	ON ci.cst_key = la.CID
);

SELECT * FROM Gold.dim_customer;

CREATE OR ALTER VIEW Gold.dim_products AS (
	SELECT
		ROW_NUMBER() OVER(ORDER BY pii.cat_id,pii.prd_start_dt) AS product_key,
		pii.prd_id AS product_id, 
		pii.cat_id AS category_id,
		pii.prd_key As product_number,
		pc.CAT AS product_category,
		pc.SUBCAT AS product_subcategory,
		pc.maintenance AS product_maintenance,
		pii.prd_cost As product_cost,
		pii.prd_line AS product_line,
		pii.prd_start_dt AS product_start_date
	FROM Silver.crm_prd_info AS pii
	LEFT JOIN Silver.erp_PX_CAT_G1V2 AS pc
	ON pii.cat_id = pc.ID
	WHERE pii.prd_end_dt IS NULL --Filter out historical data
);


SELECT * FROM Gold.dim_products;


CREATE OR ALTER VIEW Gold.fact_sales AS
(SELECT 
	sd.sls_ord_num AS order_number,
	prd.product_key,
	cust.customer_key,
	sd.sls_order_dt As order_date,
	sd.sls_ship_dt As ship_date,
	sd.sls_due_dt As due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity As sales_quantity,
	sd.sls_price AS sales_price
FROM Silver.crm_sales_details AS sd
LEFT JOIN Gold.dim_products AS prd
ON sd.sls_prd_key = prd.product_number
LEFT JOIN Gold.dim_customer AS cust
On sd.sls_cust_id = cust.customer_id);

SELECT * FROM Gold.fact_sales;
