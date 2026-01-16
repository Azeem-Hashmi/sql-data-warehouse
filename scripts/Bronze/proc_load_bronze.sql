/*
=================================================

Stored Procedure: Load Bronze Layer (Source -> Bronze Layer)

PURPOSE:

This scripts load source data from external csv files into bronze schema. It will first truncate the tables exist in bronze layer
and then bulk insert the data into relevant tables accordingly.

This stored procedure has no input parameter.

To execure this procedure, use command mentioned below:

EXEC bronze.load_bronze;

=================================================

*/


CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @etl_start_time DATETIME, @etl_end_time DATETIME;
	SET @etl_start_time = GETDATE();
	DECLARE @start_time DATETIME, @end_time DATETIME;
	BEGIN TRY
		PRINT'=============================================================';
		PRINT'Loading Bronze Layer';
		PRINT'=============================================================';

		PRINT'-------------------------------------------------------------';
		PRINT'Loading CRM Tables';
		PRINT'-------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '--------------------------------'
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'E:\Coding-Practice\Data-Engineering-Resources\SQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'E:\Coding-Practice\Data-Engineering-Resources\SQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'E:\Coding-Practice\Data-Engineering-Resources\SQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'E:\Coding-Practice\Data-Engineering-Resources\SQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '--------------------------------'
		
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'E:\Coding-Practice\Data-Engineering-Resources\SQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '--------------------------------'

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'E:\Coding-Practice\Data-Engineering-Resources\SQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds'
		PRINT '--------------------------------'
	END TRY
	BEGIN CATCH
		PRINT'=============================================================';
		PRINT'Error occured during loading bronze layer';
		PRINT'Error Message: ' + ERROR_MESSAGE();
		PRINT'Error Message: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT'Error Message: ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT'=============================================================';
	END CATCH
	SET @etl_end_time = GETDATE();
	PRINT'=============================================================';
	PRINT'Bronze Layer has been loaded successfully.';
	PRINT 'Total Time Duration to Load Bronze Layer: ' + CAST(DATEDIFF(second, @etl_start_time, @etl_end_time) AS NVARCHAR) + ' seconds'
	PRINT'=============================================================';
END;