-- Create the database if it doesn't exist already
create database if not exists transaction_demo;

-- Switch to the 'transaction_demo' database
use database transaction_demo;

-- Create the schema 'transactions_schema' if it doesn't exist
create schema if not exists transactions_schema;

-- Switch to the 'transactions_schema' schema
use schema transactions_schema;

-- Display the existing tables in the current schema
show tables;

-- Create or replace the 'transaction_data' table, which holds the main transaction data
create or replace table transaction_data (
    transaction_id varchar, 
    timestamp timestamp_ntz, 
    customer_id varchar, 
    account_id varchar, 
    transaction_type varchar, 
    transaction_amount number(10, 2), 
    currency varchar, 
    merchant_name varchar, 
    merchant_category varchar, 
    transaction_status varchar, 
    fraud_flag boolean, 
    payment_method varchar, 
    location varchar, 
    foreign_transaction boolean, 
    transaction_fee number(10, 2), 
    update_timestamp timestamp_ntz default current_timestamp()
);

-- Add a unique constraint on 'transaction_id' to prevent duplicates
ALTER TABLE transaction_data
ADD UNIQUE (transaction_id);

-- Create or replace the 'transaction_history' table to store the historical records of transactions
create or replace table transaction_history (
    transaction_id varchar, 
    timestamp timestamp_ntz, 
    customer_id varchar, 
    account_id varchar, 
    transaction_type varchar, 
    transaction_amount number(10, 2), 
    currency varchar, 
    merchant_name varchar, 
    merchant_category varchar, 
    transaction_status varchar, 
    fraud_flag boolean, 
    payment_method varchar, 
    location varchar, 
    foreign_transaction boolean, 
    transaction_fee number(10, 2), 
    start_time timestamp_ntz default current_timestamp(), 
    end_time timestamp_ntz default current_timestamp(), 
    is_current boolean
);

-- Add a column 'dml_type' to track the type of operation (Insert, Update, Delete) in 'transaction_history'
ALTER TABLE transaction_history 
ADD COLUMN dml_type varchar(1);

-- Create or replace the 'transaction_raw' table to hold the raw transaction data before processing
create or replace table transaction_raw (
    transaction_id varchar, 
    timestamp timestamp_ntz, 
    customer_id varchar, 
    account_id varchar, 
    transaction_type varchar, 
    transaction_amount number(10, 2),
    currency varchar, 
    merchant_name varchar, 
    merchant_category varchar, 
    transaction_status varchar, 
    fraud_flag boolean, 
    payment_method varchar, 
    location varchar, 
    foreign_transaction boolean, 
    transaction_fee number(10, 2)
);

-- Recreate 'transaction_raw_temp' table for testing purposes, structure similar to 'transaction_raw'
CREATE OR REPLACE TABLE transaction_raw_temp (
    transaction_id varchar, 
    timestamp timestamp_ntz, 
    customer_id varchar, 
    account_id varchar, 
    transaction_type varchar, 
    transaction_amount number(10, 2),
    currency varchar, 
    merchant_name varchar, 
    merchant_category varchar, 
    transaction_status varchar, 
    fraud_flag boolean,  
    payment_method varchar, 
    location varchar, 
    foreign_transaction boolean,  
    transaction_fee number(10, 2)
);

-- Create a stream to track changes in the 'transaction_data' table
create or replace stream transaction_table_changes on table transaction_data;
