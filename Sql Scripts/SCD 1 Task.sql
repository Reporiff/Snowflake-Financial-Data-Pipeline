-- Create or replace the procedure 'pdr_transaction_scd1' to perform the SCD Type 1 operation
CREATE OR REPLACE PROCEDURE pdr_transaction_scd1()
RETURNS STRING NOT NULL
LANGUAGE JAVASCRIPT
AS
$$
    var cmd = `
        -- Perform the MERGE operation to update existing records and insert new ones
        MERGE INTO transaction_data t
        USING transaction_raw r
        ON t.transaction_id = r.transaction_id
        -- Update existing records when any field differs between 'transaction_data' and 'transaction_raw'
        WHEN MATCHED AND (
                          t.timestamp <> r.timestamp OR
                          t.customer_id <> r.customer_id OR
                          t.account_id <> r.account_id OR
                          t.transaction_type <> r.transaction_type OR
                          t.transaction_amount <> r.transaction_amount OR
                          t.currency <> r.currency OR
                          t.merchant_name <> r.merchant_name OR
                          t.merchant_category <> r.merchant_category OR
                          t.transaction_status <> r.transaction_status OR
                          t.fraud_flag <> r.fraud_flag OR
                          t.payment_method <> r.payment_method OR
                          t.location <> r.location OR
                          t.foreign_transaction <> r.foreign_transaction OR
                          t.transaction_fee <> r.transaction_fee
                          ) THEN
            -- Set new values for the fields that are different
            UPDATE SET t.timestamp = r.timestamp,
                       t.customer_id = r.customer_id,
                       t.account_id = r.account_id,
                       t.transaction_type = r.transaction_type,
                       t.transaction_amount = r.transaction_amount,
                       t.currency = r.currency,
                       t.merchant_name = r.merchant_name,
                       t.merchant_category = r.merchant_category,
                       t.transaction_status = r.transaction_status,
                       t.fraud_flag = r.fraud_flag,
                       t.payment_method = r.payment_method,
                       t.location = r.location,
                       t.foreign_transaction = r.foreign_transaction,
                       t.transaction_fee = r.transaction_fee
        -- Insert new records when there is no match
        WHEN NOT MATCHED THEN
            INSERT (transaction_id, timestamp, customer_id, account_id, transaction_type, transaction_amount, currency, merchant_name, merchant_category, transaction_status, fraud_flag, payment_method, location, foreign_transaction, transaction_fee)
            VALUES (r.transaction_id, r.timestamp, r.customer_id, r.account_id, r.transaction_type, r.transaction_amount, r.currency, r.merchant_name, r.merchant_category, r.transaction_status, r.fraud_flag, r.payment_method, r.location, r.foreign_transaction, r.transaction_fee);
    `;

    var sql = snowflake.createStatement({sqlText: cmd});
    
    // Execute the MERGE query
    sql.execute();
    
    // Truncate the raw table 'transaction_raw' after the merge to clean up
    var cmd1 = "TRUNCATE TABLE transaction_raw;";
    var sql1 = snowflake.createStatement({sqlText: cmd1});
    sql1.execute();
    
    return 'Merge completed and transaction_raw table truncated successfully.';
$$;

-- Call the procedure to execute the logic defined above
CALL pdr_transaction_scd1();

-- Create a task that runs the procedure every minute
CREATE OR REPLACE TASK transaction_scd1_task
  WAREHOUSE = COMPUTE_WH 
  SCHEDULE = '1 minute'  -- Runs every minute for testing
AS
  CALL pdr_transaction_scd1();

-- Suspend/ Resume the task 
ALTER TASK transaction_scd1_task
  SUSPEND; 

-- Check the status of the created task 'transaction_scd1_task'

SHOW TASKS LIKE 'transaction_scd1_task';
