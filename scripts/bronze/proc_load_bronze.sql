/*This code lets you create a procedure to bulk load data from external csv files
It truncates the tables first and then it bulk loads the data
Usage example - EXEC bronze.load_bronze; */

/*As this is a long query for loading tables, we can create a procedure to do it in one go
by using CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
and then the query
END;*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;
	
	BEGIN TRY
/* We are using bulk insert to insert the whole table from the source files instead of filling it row by row
WATCH AND LEARN
If you load the file twice by mistake, it will show 2 times the rows and hence we truncate the taable first and then load the data*/
	SET @batch_start = GETDATE();
		PRINT '==================================================================================================';
		PRINT 'Loading tables for Bronze Layer';
		PRINT '==================================================================================================';
		PRINT '--------------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '--------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;
		BULK INSERT bronze.crm_cust_info
		From 'C:\Users\PAankhuri\Downloads\Data Warehouse project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		/*Now that the data is inserted we will check the quality of inserted data, 
		see if there is data in all columns, columns are correct, the data is in correct column
		eg if the first name is in the second name column
		SELECT * FROM bronze.crm_cust_info - to see the whole table and do quality check

		Please make sure to add .csv in the path*/
		SET @end_time = GETDATE();
		PRINT '>> Load time for cust info = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		BULK INSERT bronze.crm_prd_info
		From 'C:\Users\PAankhuri\Downloads\Data Warehouse project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time for prod info = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		BULK INSERT bronze.crm_sales_details
		From 'C:\Users\PAankhuri\Downloads\Data Warehouse project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time for sales detail = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

		PRINT '--------------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '--------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		BULK INSERT bronze.erp_cust_az12
		From 'C:\Users\PAankhuri\Downloads\Data Warehouse project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time for cust az12= ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		BULK INSERT bronze.erp_loc_a101
		From 'C:\Users\PAankhuri\Downloads\Data Warehouse project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time for loc a101 = ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		BULK INSERT bronze.erp_px_cat_g1v2
		From 'C:\Users\PAankhuri\Downloads\Data Warehouse project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load time for px cat g1v2= ' + CAST (DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		SET @batch_end = GETDATE();
	
	PRINT 'Total time taken to load bronze layer = ' + CAST ( DATEDIFF(second, @batch_start, @batch_end) AS NVARCHAR) + 'seconds';
	END TRY
	
	BEGIN CATCH
		PRINT 'Error occured during loading of bronze layer';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH

END;

/*As we have created a procedure above, here we will execute it*/
EXEC bronze.load_bronze;

