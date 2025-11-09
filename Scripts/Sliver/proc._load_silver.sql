/*
===================================================================================
Stored Procedure: Load silver Layer(bronze > silver)
====================================================================================
Script Purpose: 
	This stored procedure performs the ETL (Extract, Transform, Load) process to populate
	the 'silver' schema tables from the 'bronze' schema.

Actions Perfomred:
	- Truncates Silver Tables.
	- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
	None.
	This stored procedure does not accept any parameters or return any values.

Usage Example:
	EXEC Silver.load_silver;
=====================================================================================
*/



Create or Alter PROCEDURE silver.load_silver as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time Datetime;
Begin TRY
	SET @batch_start_time = GETDATE();
	PRINT '===============================================================================';
	PRINT 'Loading Bronze Layer';
	PRINT '===============================================================================';


	PRINT '-------------------------------------------------------------------------------';
	PRINT 'Loading CRM Tables';
	PRINT '-------------------------------------------------------------------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.crm_cust_info';
TRUNCATE TABLe silver.crm_cust_info;
PRINT '>> Inserting Data INTO: silver.crm_cust_info';
INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_martial_status,
    cst_gndr,
    cst_create_date
  )

  Select
    cst_id,
    cst_key,
    TRIM(cst_firstname) as cst_firstname,
    TRIM(cst_lastname) as cst_lastname,
    Case 
        when UPPER(TRIM(cst_martial_status)) = 'S' THEN 'Single'
        when UPPER(TRIM(cst_martial_status)) = 'M' THEN 'Married'
        ELSE 'n/a'
        END as cst_martial_status,
    Case 
        when UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        when UPPER(TRIM(cst_gndr)) = 'M' Then 'Male'
        else 'n/a'
        END as cst_gndr,
        cst_create_date

    FROM (
        SELECT
        *,
        ROW_NUMBER() OVER(Partition by cst_id Order by cst_create_date DESC) as flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id is NOT NULL
        )t
        where flag_last = 1
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';


SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.crm_prd_info';
TRUNCATE TABLe silver.crm_prd_info;
PRINT '>> Inserting Data INTO: silver.crm_prd_info';
INSERT INTO silver.crm_prd_info (
 prd_id,
 cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
)
 select 
 prd_id,
 Replace(SUBSTRING(prd_key, 1, 5), '-', '_') as category_id,
 SUBSTRING(prd_key, 7, len(prd_key)) as prd_key,
 prd_nm,
 ISNULL(prd_cost, 0) as prd_cost,
 case 
	when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
	when UPPER(TRIM(prd_line)) = 'R' then 'Road'
	when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
	when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
	Else 'N/A'
	END prd_line,
cast(prd_start_dt as date) as prd_start_dt,
Cast(Dateadd(DAY, -1, LEad(prd_start_dt) over(partition by prd_key order by prd_start_dt)) as date) as prd_end_dt
from bronze.crm_prd_info
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.crm_sales_details';
TRUNCATE TABLe silver.crm_sales_details;
PRINT '>> Inserting Data INTO: silver.crm_sales_details';

INSERT INTO silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key ,
	sls_cust_id,
	sls_order_dt ,
	sls_ship_dt ,
	sls_due_dt ,
	sls_sales ,
	sls_quantity ,
	sls_price)



SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 
OR
LEN(sls_order_dt) != 8 THEN NULL
	else CAST(CAST(sls_order_dt as VARCHAR) as DATE)
	END as sls_order_dt,

	CASE WHEN sls_ship_dt = 0 
OR
LEN(sls_ship_dt) != 8 THEN NULL
	else CAST(CAST(sls_ship_dt as VARCHAR) as DATE)
	END as sls_ship_dt,

CASE WHEN sls_due_dt = 0 
OR
LEN(sls_due_dt) != 8 THEN NULL
	else CAST(CAST(sls_due_dt as VARCHAR) as DATE)
	END as sls_due_dt,

Case 
	when sls_sales is NULL 
  OR
  sls_sales <= 0
  OR
  sls_sales != sls_quantity * ABS(sls_price)
	then sls_quantity * ABS(sls_price)
	ELSE sls_sales
END as sls_sales,
sls_quantity,
Case
	when sls_price is NULL 
	OR 
	sls_price <= 0
	THEN 
	sls_sales / NULLIF(sls_quantity, 0)
	ELSE sls_price
END as sls_price
FROM bronze.crm_sales_details
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.erp_cust_az12';
TRUNCATE TABLe silver.erp_cust_az12;
PRINT '>> Inserting Data INTO: silver.erp_cust_az12';

Insert into silver.erp_cust_az12 (cid, bdate, gen)


select 
case when cid like 'NAS%' THen SUBSTRING(cid, 4, LEN(cid))
	else cid
	end cid,

Case when bdate > GETDATE() THEN NULL
	ELSE bdate
	END bdate,

 case when UPPER(trim(gen)) IN ('F', 'Female') then 'Female'
	  when upper(trim(gen)) in ('M', 'Male') then 'Male'
	  ELSE 'n/a'
	  END as gen
from bronze.erp_cust_az12
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.erp_loc_a101';
TRUNCATE TABLe silver.erp_loc_a101;
PRINT '>> Inserting Data INTO: silver.erp_loc_a101';

INSERT INTO silver.erp_loc_a101
(cid, cntry)
select
REPLACE(cid, '-', '') cid,
case when TRIM(cntry) = 'DE' Then 'Germany'
	when trim(cntry) in ('US', 'USA') then 'United States'
	when trim(cntry) = '' OR cntry is NULL then 'n/a'
	ELSE trim(cntry)
	END as cntry
FROM bronze.erp_loc_a101
ORDER BY cntry
SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

SET @start_time = GETDATE();
PRINT '>> Truncating Table: silver.erp_px_cat_glv2';
TRUNCATE TABLe silver.erp_px_cat_glv2;
PRINT '>> Inserting Data INTO: silver.erp_px_cat_glv2';

INSERT INTO silver.erp_px_cat_glv2
(id, cat, subcat, maintenance)

select 
id,
cat,
subcat,
maintenance
from bronze.erp_px_cat_glv2;

SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '>> -----------------------';

SET @batch_end_time = GETDATE();
		PRINT '=========================================================================='
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'SECONDS';
		PRINT '==========================================================================';

END TRY
BEGIN CATCH
	PRINT '===============================================================================';
	PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST (ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '===============================================================================';
	END CATCH
	END
