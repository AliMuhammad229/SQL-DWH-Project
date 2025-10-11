/*
========================================
INTORDUCTION:
========================================


This files create a database and schema based on Medallion Architecture.


========================================
WARNINGS:
========================================


Make sure, before you going ahead, you have your backup because it checks whether the database has already been created. It will permanently remove that database and create another one.

*/

USE master;

GO

--Drop and recreate DWH 'DataWarehouse' DB
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWarehouse;
END;

GO

--CREATE DATABASE DataWarehouse
CREATE DATABASE DataWarehouse;

USE DataWarehouse;


--CREATE SCHEMA
GO
CREATE SCHEMA Bronze;
GO
CREATE SCHEMA Silver;
GO
CREATE SCHEMA Gold;
GO
