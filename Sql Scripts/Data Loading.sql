-- Creating an external stage for transaction data in S3
CREATE OR REPLACE STAGE transaction_demo.transactions_schema.transaction_ext_stage
    url='s3://streaming-data-snowflake/transaction_data'
    credentials=(aws_key_id='' aws_secret_key='');

-- Creating a file format for CSV, enforcing strict rules
CREATE OR REPLACE FILE FORMAT csv_format_strict
  TYPE = 'CSV'
  FIELD_DELIMITER = ','  
  SKIP_HEADER = 1  
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'  
  ESCAPE_UNENCLOSED_FIELD = NONE 
  NULL_IF = ('');  

-- Testing the loading of data from the external stage to a temporary table
COPY INTO transaction_raw_temp
FROM @transaction_ext_stage
FILE_FORMAT = (FORMAT_NAME = csv_format_strict);

-- Count the number of rows loaded into 'transaction_raw_temp' to verify the load
SELECT COUNT(*)
FROM transaction_raw_temp;

-- Show all available stages in the current schema
SHOW STAGES;

-- List the files available in the external stage to verify content
LIST @transaction_ext_stage;

-- Creating a pipe for automatic data ingestion from the S3 bucket to Snowflake
CREATE OR REPLACE PIPE transaction_s3_pipe
  auto_ingest = true  -- Enables automatic ingestion from the stage
  AS
  COPY INTO transaction_raw  -- Copy data into the 'transaction_raw' table
  FROM @transaction_ext_stage
  FILE_FORMAT = (FORMAT_NAME = csv_format_strict);

-- Show all available pipes in the schema
SHOW PIPES;

-- Check the status of the pipe to ensure it's functioning correctly
SELECT SYSTEM$PIPE_STATUS('transaction_s3_pipe');

-- Count the rows in the 'transaction_raw' table to verify ingestion
SELECT COUNT(*)
FROM transaction_raw;
