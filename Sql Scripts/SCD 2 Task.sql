-- Display all available streams in the current database
SHOW STREAMS;

-- Create or replace a view 'v_transaction_change_data' to capture changes in 'transaction_table_changes'
-- This view labels the type of DML operation (INSERT, DELETE, or UPDATE) and adds metadata fields for start and end times
CREATE OR REPLACE VIEW v_transaction_change_data AS
SELECT *,
       CASE
           WHEN METADATA$ACTION = 'INSERT' THEN 'I'  -- Marking insertions with 'I'
           WHEN METADATA$ACTION = 'DELETE' THEN 'D'  -- Marking deletions with 'D'
           ELSE 'U'  -- Marking updates with 'U'
       END AS dml_type,
       CURRENT_TIMESTAMP() AS start_time,  -- Add current timestamp as the start time
       NULL AS end_time,  -- End time will remain NULL until the record is no longer current
       TRUE AS is_current  -- Mark the record as current (TRUE)
FROM transaction_table_changes;

-- Create a scheduled task 'transaction_scd2_task' that runs every minute
-- This task performs a SCD Type 2 merge from the change data view into the 'transaction_history' table
CREATE OR REPLACE TASK transaction_scd2_task
  WAREHOUSE = COMPUTE_WH  -- Define the warehouse to execute the task
  SCHEDULE = '1 minute'  -- Set the task to run every minute
  ERROR_ON_NONDETERMINISTIC_MERGE = FALSE  -- Allows non-deterministic merge (multiple matching rows allowed)
AS
  -- Perform the MERGE operation into 'transaction_history'
  MERGE INTO transaction_history th
  USING v_transaction_change_data tcd
    ON th.transaction_id = tcd.transaction_id  -- Match on transaction_id
       AND th.start_time = tcd.start_time  -- Match on start_time (for uniqueness)
  
  -- Update existing records for an update operation (U)
  WHEN MATCHED AND tcd.dml_type = 'U' THEN 
    UPDATE SET th.end_time = tcd.end_time, th.is_current = FALSE, th.dml_type = 'U'  -- Mark as non-current and update the end_time

  -- Update existing records for a delete operation (D)
  WHEN MATCHED AND tcd.dml_type = 'D' THEN 
    UPDATE SET th.end_time = tcd.end_time, th.is_current = FALSE, th.dml_type = 'D'  -- Mark as non-current and update the end_time

  -- Insert new records for an insert operation (I)
  WHEN NOT MATCHED AND tcd.dml_type = 'I' THEN 
    INSERT (transaction_id, timestamp, customer_id, account_id, transaction_type, transaction_amount, 
            currency, merchant_name, merchant_category, transaction_status, fraud_flag, payment_method, 
            location, foreign_transaction, transaction_fee, start_time, end_time, is_current, dml_type)
    VALUES (tcd.transaction_id, tcd.timestamp, tcd.customer_id, tcd.account_id, tcd.transaction_type, 
            tcd.transaction_amount, tcd.currency, tcd.merchant_name, tcd.merchant_category, tcd.transaction_status, 
            tcd.fraud_flag, tcd.payment_method, tcd.location, tcd.foreign_transaction, tcd.transaction_fee, 
            tcd.start_time, tcd.end_time, tcd.is_current, tcd.dml_type);

-- Suspend the task if needed (optional, useful for testing or pausing execution)
ALTER TASK transaction_scd2_task SUSPEND; -- Task is suspended, use RESUME to activate it

-- Show the available streams with names like 'transaction_table_changes'
SHOW STREAMS LIKE 'transaction_table_changes';
