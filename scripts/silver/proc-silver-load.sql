EXEC Silver.load_silver;

---Tranformed Table Customer_Info:
CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_time_bronze DATETIME, @end_time_bronze DATETIME
	BEGIN TRY
		PRINT '==============================================';
		PRINT 'LOADING SILVER LAYER';
		PRINT '==============================================';


		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @start_time_bronze = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_cust_info';
		TRUNCATE TABLE Silver.crm_cust_info;
		PRINT '>> Inserting Data: Silver.crm_cust_info';
		INSERT INTO Silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT 
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				 WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				 WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				 ELSE 'n/a'
			END AS cst_marital_status,
			CASE 
				 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				 WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				 ELSE 'n/a'
			END AS cst_gndr,
			cst_create_date
		FROM 
		(
			SELECT 
			*,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS Flag
			FROM 
			Bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) as Remove_Duplicates
		WHERE Remove_Duplicates.Flag = 1; -- select the most recent records
		
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_prd_info';
		TRUNCATE TABLE Silver.crm_prd_info;
		PRINT '>> Inserting Data: Silver.crm_prd_info';
		INSERT INTO Silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)
	
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
			TRIM(prd_nm) AS prd_nm,
			COALESCE(prd_cost, 0) AS prd_cost,
			CASE UPPER(TRIM(prd_line)) 
				 WHEN 'R' THEN 'Roads'
				 WHEN 'M' THEN 'Montains'
				 WHEN 'S' THEN 'Other Sales'
				 WHEN 'T' THEN 'Touring'
				 ELSE 'n/a'
			END AS prd_line,
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
				AS DATE
			) AS prd_end_dt
		FROM 
			Bronze.crm_prd_info;


	
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.crm_sales_details';
		TRUNCATE TABLE Silver.crm_sales_details;
		PRINT '>> Inserting Data: Silver.crm_sales_details';
		INSERT INTO Silver.crm_sales_details(
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT 
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE 
				WHEN sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			END AS sls_order_dt,
			CASE 
				WHEN sls_ship_dt = 0 or LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			END AS sls_ship_dt,
			CASE 
				WHEN sls_due_dt = 0 or LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			END AS sls_due_dt,
			CASE
				WHEN sls_sales <= 0 OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price)
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE 
				WHEN sls_price <= 0 OR sls_price IS NULL THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price


		FROM 
			Bronze.crm_sales_details;

		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------';
		
		
		
		
		
		
		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_CUST_AZ12';
		TRUNCATE TABLE Silver.erp_CUST_AZ12;
		PRINT '>> Inserting Data: Silver.erp_CUST_AZ12';
		INSERT INTO Silver.erp_CUST_AZ12(
			CID,
			BDATE,
			GEN
		)

		SELECT
			CASE
				WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
				ELSE CID
			END AS CID,
			CASE 
				WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE,
			CASE
				WHEN UPPER(TRIM(GEN)) IN('M', 'MALE') THEN 'Male'
				WHEN UPPER(TRIM(GEN)) IN('F', 'FEMALE') THEN 'Female'
				ELSE 'n/a'
			END AS GEN
		FROM 
			Bronze.erp_CUST_AZ12;


		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------';



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_LOC_A101';
		TRUNCATE TABLE Silver.erp_LOC_A101;
		PRINT '>> Inserting Data: Silver.erp_LOC_A101';
		INSERT INTO Silver.erp_LOC_A101(
			CID,
			CNTRY
		)
		SELECT
			REPLACE(CID, '-', '') AS CID,
			CASE 
				WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
				WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
				WHEN TRIM(CNTRY) IS NULL OR TRIM(CNTRY) = '' THEN 'n/a'
				ELSE TRIM(CNTRY)
			END AS CNTRY
		FROM 
			Bronze.erp_LOC_A101;

		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------';



		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: Silver.erp_PX_CAT_G1V2';
		TRUNCATE TABLE Silver.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Data: Silver.erp_PX_CAT_G1V2';
		INSERT INTO Silver.erp_PX_CAT_G1V2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)

		SELECT
			*
		FROM 
			Bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();
		PRINT '>> LOADING DURATION: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '--------------------------';
		SET @end_time_bronze = GETDATE();
		PRINT '>> LOADING DURATION OF SILVER LAYER: ' + CAST(DATEDIFF(second, @start_time_bronze, @end_time_bronze) AS NVARCHAR) + ' seconds'
	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT '==============================================';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================';
	END CATCH

END
