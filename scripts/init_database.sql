/*This is an sql code to create a new database named Datawarehouse 
and before creating it, it checks if it exists and delete the previous one. 
It creats schemas named Gold, Silver, Bronze.
Warning - It deletes the Database named DataWarehouse 
so be sure you have backup before executing this code*/

Use master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
  BEGIN 
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE Datawarehouse;
END;

GO

CREATE DATABASE Datawarehouse;
GO

Use Datawarehouse;

CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
