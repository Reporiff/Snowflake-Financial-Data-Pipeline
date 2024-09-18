# Project Overview

This project demonstrates the creation of a data pipeline using AWS EC2, Docker, Apache NiFi, Python, and Snowflake. The pipeline simulates transaction data using the **Faker** library and ingests it into Snowflake's data warehouse for processing and analysis. The pipeline also implements **SCD Type 1** and **SCD Type 2** mechanisms to maintain data history in a `transaction_history` table while keeping the most recent data in `transaction_data`.

## Technologies Used:
- **AWS EC2**: Used as the compute instance to host Docker and process data.
- **Docker**: Containerized Apache NiFi & Python for data generation and movement.
- **Apache NiFi**: Used to build a flow for listing, fetching, and moving data to an S3 bucket.
- **Faker (Python)**: Generated fake transaction data in Jupyter notebooks.
- **AWS S3**: Data lake storage for raw transaction data.
- **Snowflake**: Created tables, staged data, and managed the data pipeline using Snowpipe and tasks.
- **Snowpipe**: Automatically ingests data from S3 into the staging layer in Snowflake.

## Architecture Diagram:
[Architecture Diagram](https://github.com/Reporiff/Snowflake-Streaming-Data-Pipeline/blob/main/Architecture%20Diagram.png)

## Workflow

1. **AWS EC2 Setup**:
    - Launched an EC2 instance and connected via SSH through the terminal.
    - Installed Docker and set up containers for Apache NiFi & Python.
    - Configured inbound security rules to allow access to Docker container ports.

2. **Apache NiFi Setup**:
    - Configured NiFi to create a data flow that listed, fetched, and uploaded the dataset (generated using Python and Faker) to the S3 bucket.

3. **Snowflake Staging Layer**:
    - Created a **staging layer** in Snowflake to temporarily hold data ingested from the S3 bucket using Snowpipe.
    - This layer ensured raw data was properly validated and formatted before being moved to the final destination tables.
    - Snowpipe automatically ingested new data from S3 into the **`transaction_raw`** table in the staging layer.

4. **Orchestration and Task Creation**:
    - Orchestrated the data flow using Snowflake tasks and stored procedures.
    - Created tasks to automatically move data from the staging layer (`transaction_raw`) to the final destination (`transaction_data`).
    - Deleted old data from the raw staging table once it was moved to the destination.

5. **SCD 1 & SCD 2 Implementation**:
    - **SCD Type 1**: Updates the existing data in `transaction_data` when new data is inserted and deletes the old data from `transaction_raw`.
    - **SCD Type 2**: Tracks the history of changes in the `transaction_history` table by maintaining the previous values and marking the current record as `is_current = TRUE`.

## Challenges Encountered

1. **Column Mismatch in Snowflake**: One of the major challenges faced was the misalignment of columns during data ingestion into the Snowflake `transaction_raw` table. When the dataset was being loaded from the S3 bucket via Snowpipe, Snowflake failed to map the columns in the dataset correctly, leading to errors. This was due to inconsistent file formatting in the CSV files stored in the S3 bucket and a mismatch between the data and the schema defined in Snowflake. The issue was resolved by creating strict file format rules for Snowflake’s ingestion process, enforcing a CSV format that matched the exact column structure expected by Snowflake, ensuring smooth ingestion into the `transaction_raw` table.

2. **Duplicate Data in `transaction_data`**: A significant issue was the presence of duplicate data in the `transaction_data` table because the `transaction_id` column was not set as the primary key during the initial table creation. This allowed multiple records with the same `transaction_id` to be inserted, leading to duplicate entries and affecting analysis accuracy. The solution involved modifying the schema by adding a **UNIQUE constraint** on the `transaction_id` column to prevent future duplicates, and implementing a clean-up procedure to remove any existing duplicate records by retaining only the latest entry based on the `timestamp` field.

3. **Snowpipe Not Ingesting Data Properly**: Snowpipe was initially not ingesting data from the S3 bucket into Snowflake’s staging layer as expected. The issue was traced to a misconfigured **SQS (Simple Queue Service) queue** address, which prevented Snowpipe from being notified when new files were uploaded to the S3 bucket. After identifying the incorrect SQS queue setup, the queue address was reconfigured correctly, allowing Snowpipe to monitor the S3 bucket for new files and trigger automatic data ingestion into Snowflake’s staging layer (`transaction_raw` table).

## Files in Repository

- **docker-compose folder**: Contains the `docker-compose` file used to configure and run Docker containers.
- **dataset folder**: Includes the `fake_dataset.ipynb` (Jupyter notebook for generating fake transaction data using Faker) and two CSV transaction data files.
- **sql scripts folder**: Contains the SQL scripts for creating tables, loading data, running SCD1 and SCD2 tasks, and testing the pipeline.

## Conclusion

This project simulates the ingestion and processing of transaction data using a robust pipeline. The pipeline efficiently handles large datasets using Snowflake and ensures data consistency and history tracking through the use of **SCD Type 1** and **SCD Type 2** techniques.
