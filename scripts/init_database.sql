/*

-----------------------------------------------------------------------------------

PURPOSE:

This file is containing the scripts for creating the datawarehouse database 
and the other relevant schemas of tables
This scripts creates a new database named DataWarehouse after checking if it 
already exist or not. If it exist then it will drop it first and than recreate 
it. Additionally, it will create three schema named bronze, silver and gold which
are indicating three layers of our data warehouse

-----------------------------------------------------------------------------------

WARNING:

Running this script will drop the entire database if it exists.
All data in the database will be permenantly deleted.

-----------------------------------------------------------------------------------

*/


USE MASTER;
GO

-- checking if the database already exist or not, if exist than drop it

IF EXISTS (SELECT 1 from sys.databases where name = 'DataWarehouse')
BEGIN
	ALTER Database DataWarehouse 
	SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse
END;
GO


-- Creating data warehouse

Create Database DataWarehouse;
GO

-- creating schemas

USE DataWarehouse;
GO
Create Schema bronze;
GO
Create Schema silver;
GO
Create Schema gold;
GO