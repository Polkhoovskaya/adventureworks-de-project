---------------------
-- create Lake House
---------------------

-- create view calendar
CREATE OR ALTER VIEW gold.calendar
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_calendar/',
        FORMAT = 'PARQUET'
    ) as Q1;

Go

-- create view customers
CREATE OR ALTER VIEW gold.customers
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_customers/',
        FORMAT = 'PARQUET'
    ) as Q2;


Go

-- create view product_categories
CREATE OR ALTER VIEW gold.product_categories
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_product_categories/',
        FORMAT = 'PARQUET'
    ) as Q3;

Go


-- create view product_subcategories
CREATE OR ALTER VIEW gold.product_subcategories
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_product_subcategories/',
        FORMAT = 'PARQUET'
    ) as Q4;

Go


-- create view products
CREATE OR ALTER VIEW gold.products
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_products/',
        FORMAT = 'PARQUET'
    ) as Q5;

Go

-- create view returns
CREATE OR ALTER VIEW gold.returns
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_returns/',
        FORMAT = 'PARQUET'
    ) as Q6;

Go

-- create view sales
CREATE OR ALTER VIEW gold.sales
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_sales/',
        FORMAT = 'PARQUET'
    ) as Q7;

Go

-- create view territories
CREATE OR ALTER VIEW gold.territories
AS 
SELECT *
FROM 
    OPENROWSET
    (
        BULK 'https://awstoragedatalakevp.blob.core.windows.net/silver/AdventureWorks_territories/',
        FORMAT = 'PARQUET'
    ) as Q8;

Go