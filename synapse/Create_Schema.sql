-- Create schema if it doesn't exist
IF NOT EXISTS (
    SELECT 1
    FROM sys.schemas
    WHERE name = 'gold'
)
    EXEC('CREATE SCHEMA gold');