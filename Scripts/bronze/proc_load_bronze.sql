/*
==================================================================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================================================================================
Script Purpose:
	This stored procedure loads data into the 'bronze' schema from external CSV files.
	It performs the following actions:
	- Truncates the bronze tabless before loading data.
	- Uses the BULK INSERT command to load data from CSV files to bronze tables.

	Parameters:
		None.
		This stored procedure does not accept any parameters or return any values.

	Usage Example:
		EXEC bronze.load_bronze;
===================================================================================================================
*/


Create or alter procedure bronze.load_bronze as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME;
	SET @start_time = GETDATE();
BEGIN TRY
	PRINT '===============================================================================';
	PRINT 'Loading Bronze Layer';
	PRINT '===============================================================================';


	PRINT '-------------------------------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '-------------------------------------------------------------------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table; bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;

	PRINT '>> Inseting Data Into: bronze.crm_cust_info';
	BULK INSERT bronze.crm_cust_info
	from 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';


	SET @start_time = GETDATE();
	PRINT '>> Truncating Table; bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;

	PRINT '>> Inseting Data Into: bronze.crm_sales_details';
	BULK INSERT bronze.crm_sales_details
	from 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table; bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;

	PRINT '>> Inseting Data Into: bronze.crm_prd_info';
	BULK INSERT bronze.crm_prd_info
	from 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';


	PRINT '-------------------------------------------------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '-------------------------------------------------------------------------------';
	SET @start_time = GETDATE();
	PRINT '>> Truncating Table; bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;

	PRINT '>> Inseting Data Into: bronze.erp_loc_a101';
	BULK INSERT bronze.erp_loc_a101
	from 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';


	SET @start_time = GETDATE();
	PRINT '>> Truncating Table; bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;

	PRINT '>> Inseting Data Into: bronze.erp_cust_az12';
	BULK INSERT bronze.erp_cust_az12
	from 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

	SET @start_time = GETDATE();
	PRINT '>> Truncating Table; bronze.erp_px_cat_glv2';
	TRUNCATE TABLE bronze.erp_px_cat_glv2;

	PRINT '>> Inseting Data Into: bronze.erp_px_cat_glv2';
	BULK INSERT bronze.erp_px_cat_glv2
	from 'C:\Users\USER\Downloads\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

	END TRY
	BEGIN CATCH
	PRINT '===============================================================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '===============================================================================';
	END CATCH
	SET @end_time = GETDATE();
		PRINT '>> Load Duration of WholeBatch: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';
END
