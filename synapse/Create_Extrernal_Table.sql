-- create credentials 
-- master key
-- https://learn.microsoft.com/en-us/sql/t-sql/statements/create-master-key-transact-sql?view=sql-server-ver17
CREATE MASTER KEY  ENCRYPTION BY PASSWORD = '...';

-- credentials
-- https://learn.microsoft.com/en-us/sql/t-sql/statements/create-database-scoped-credential-transact-sql?view=sql-server-ver17
CREATE DATABASE SCOPED CREDENTIAL cred_vp
WITH IDENTITY = 'Managed Identity';


-- create external datasource silver
CREATE EXTERNAL DATA SOURCE source_silver
WITH 
(
    LOCATION = 'https://awstoragedatalakevp.blob.core.windows.net/silver',
    CREDENTIAL = cred_vp
);

-- create external datasource gold
CREATE EXTERNAL DATA SOURCE source_gold
WITH 
(
    LOCATION = 'https://awstoragedatalakevp.blob.core.windows.net/gold',
    CREDENTIAL = cred_vp
);

-- create external file format
-- https://learn.microsoft.com/en-us/sql/t-sql/statements/create-external-file-format-transact-sql?view=sql-server-ver17&tabs=parquet
CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

-- create external table extsales
CREATE EXTERNAL TABLE gold.extsales
WITH(
    LOCATION = 'extsales',
    DATA_SOURCE = source_gold,
    FILE_FORMAT = format_parquet
) AS
SELECT * FROM gold.sales;


SELECT * FROM gold.extsales;
