/*
========================================================================================================
Create Database and Schemas
========================================================================================================
Script Purpose: 
	This script creates a new database named 'Datawarehouse' after checking if it already exist.
	If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
	within  the database: 'bronze', 'silver', 'gold'

Warning:
	Running this script will drop the entire 'DataWarehouse' database if it exist.
	All data in the database will be permenantly deleted. Proceed with caution and
	ensure you have proper backups before running this script.

*/



-- Drop and recreate a 'Datawarehouse' database.

IF EXIST( select 1 from sys.databases where name = 'DataWarehouse')
Begin
	Alter Database DataWarehouse set SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse

END;
GO

--CREATE the 'Datawarehouse' database

Create DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

--create schema

Create SCHEMA bronze;
Go

Create SCHEMA silver;
GO

Create SCHEMA gold;
GO
