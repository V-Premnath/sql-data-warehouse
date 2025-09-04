/*
======================================
Create Database and Schemas
======================================
	SCRIPT
	PURPOSE :	Creates a new Database 'DataWarehouse' after checking if oone exists with the same name, and if one exisats,
				    drops it and recreates it along with creating the schemas 'bronze', 'silver' and 'gold'. 	
				
	WARNING:	Running this script drops the existing 'DataWarehouse' databsae and creates a new one, all the data is lost
				    Handle with care while using this script.

          =>  This script is intended for execution via sqlcmd or SSMS. 
          =>  'GO' statements are used to separate batches.
*/

use master;
GO

IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN 
	ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE; 
	DROP DATABASE DataWarehouse;
END;
GO


-- Database Creation 
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

GO
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
