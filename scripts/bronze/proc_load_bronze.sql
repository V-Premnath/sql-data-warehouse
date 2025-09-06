/*
=========================================================
Stored procedure : Load Source Data into Bronze Layer   
=========================================================

PURPOSE:
	this stored procedure loads data from external csv files into bronze layer /schema tables.
	- truncates all the tables one by one and then populates with data.
	uses 'BULK LOAD' command to load whole files into tables.

PARAMETERS: 
	No input parameters are taken and DOES NOT return any value, other than implicit integer return.

EXAMPLE USAGE: 
	EXEC bronze.load_bronze;

*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 
	DECLARE 
		@start_time DATETIME,
		@end_time DATETIME,
		@bronze_start_time DATETIME,
		@bronze_end_time DATETIME; 
	BEGIN TRY
		PRINT '=============================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '=============================================';
		PRINT '';
		PRINT '>> -----------------------------';
		PRINT '>> LOADING CRM TABLES';
		PRINT '>> -----------------------------';
		SET @bronze_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
			FROM 'C:\Users\vprem\Desktop\Projects\sql_project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION FOR TABLE: bronze.crm_cust_info: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' sec';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
			FROM 'C:\Users\vprem\Desktop\Projects\sql_project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION FOR TABLE: bronze.crm_prd_info: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' sec';
		PRINT '>> -----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.crm_sales_details ';
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details ';
		BULK INSERT bronze.crm_sales_details
			FROM 'C:\Users\vprem\Desktop\Projects\sql_project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
			WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR  = ',',
				TABLOCK
			);
		SET @end_time = GETDATE();
		PRINT 'LOAD DURATION FOR TABLE: bronze.crm_sales_details: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' sec';
		PRINT '>> -----------------------------';


		PRINT '';
		PRINT '>> -----------------------------';
		PRINT 'LOADING ERP TABLES';
		PRINT '>> -----------------------------';
		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\vprem\Desktop\Projects\sql_project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION FOR TABLE: bronze.erp_cust_az12: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' sec';
		PRINT '>> -----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\vprem\Desktop\Projects\sql_project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION FOR TABLE: bronze.erp_loc_a101: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' sec';
		PRINT '>> -----------------------------';

		SET @start_time = GETDATE();
		PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\vprem\Desktop\Projects\sql_project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR  = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> LOAD DURATION FOR TABLE: bronze.erp_px_cat_g1v2: ' + CAST(DATEDIFF(second, @start_time,@end_time) AS NVARCHAR) + ' sec';
		PRINT '>> -----------------------------';
		PRINT '';
		
		SET @bronze_end_time = GETDATE();
		PRINT '=============================================';
		PRINT 'LOADING COMPLETED FOR BRONZE LAYER';
		PRINT '>> LOAD DURATION FOR BRONZE LAYER: ' + CAST(DATEDIFF(second, @bronze_start_time,@bronze_end_time) AS NVARCHAR) + ' sec';
		PRINT '=============================================';
		PRINT '';

	END TRY
	BEGIN CATCH
		PRINT '=============================================';
		PRINT 'ERROR CAUGHT WHILE LOADING DATA';
		PRINT '=============================================';
		PRINT 'ERROR MESSAGE: ' + ERROR_MESSAGE();
		PRINT 'ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR STATE: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'ERROR LINE:' + CAST(ERROR_LINE() AS NVARCHAR); 
		PRINT 'ERROR SEVERITY:' + CAST(ERROR_SEVERITY() AS NVARCHAR); 
	END CATCH
END
