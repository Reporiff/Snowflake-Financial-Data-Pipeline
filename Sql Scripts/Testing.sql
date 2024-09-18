-- Removing the data by truncating the necessary tables
TRUNCATE TABLE transaction_data;
TRUNCATE TABLE transaction_history;
TRUNCATE TABLE transaction_raw;

-- Dropping the stream if it exists (cleanup before testing)
DROP STREAM IF EXISTS transaction_table_changes;

-- Check if transaction_data has been emptied (verification)
SELECT COUNT(*) 
FROM transaction_data;

-- Verify specific data in transaction_data table (for debugging specific transactions)
SELECT *
FROM transaction_data
WHERE transaction_id = '593bb10d-aef7-4d93-aab3-e88e979e6d84';

-- Check if transaction_raw has been emptied (verification)
SELECT COUNT(*) 
FROM transaction_raw;

-- Verify if transaction_history table has been emptied (verification)
SELECT COUNT(*) 
FROM transaction_history;

-- Verify specific data in transaction_history table (for debugging specific transactions)
SELECT * 
FROM transaction_history
WHERE transaction_id = '593bb10d-aef7-4d93-aab3-e88e979e6d84';

-- Testing SCD2 by updating a transaction's status in transaction_data
-- The update should trigger the change data capture
UPDATE transaction_data
SET 
    transaction_status = 'completed'  -- Changing status for SCD2 testing
WHERE transaction_id = '593bb10d-aef7-4d93-aab3-e88e979e6d84';

- Delete a specific record from the transaction_data table
DELETE FROM transaction_data
WHERE transaction_id = '593bb10d-aef7-4d93-aab3-e88e979e6d84';

-- Verify that the deletion has been captured in the transaction_table_changes stream
SELECT *
FROM transaction_table_changes
WHERE transaction_id = '593bb10d-aef7-4d93-aab3-e88e979e6d84';

-- Verify the deletion in the transaction_history table
SELECT *
FROM transaction_history
WHERE transaction_id = '593bb10d-aef7-4d93-aab3-e88e979e6d84';

-- Check for records marked as not current in the transaction_history table
SELECT *
FROM transaction_history
WHERE is_current = FALSE
AND transaction_id = '593bb10d-aef7-4d93-aab3-e88e979e6d84';

-- Verify the transaction_table_changes stream
SHOW STREAMS LIKE 'transaction_table_changes';

-- Check if the change was captured in the transaction_table_changes stream
SELECT *
FROM transaction_table_changes;




