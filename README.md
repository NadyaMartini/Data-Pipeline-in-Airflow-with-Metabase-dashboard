# Data-Pipeline-in-Airflow-with-Metabase-dashboard
This project focuses on data extraction, transformation, and loading (ETL) processes in an Apache Airflow environment. It consists of two separate DAGs: ETL_stg and ETL_dwh.
Technologies and Tools: 
Apache Airflow,
AWS S3,
Vertica,
Metabase.

Here's an overview of each DAG:

ETL_stg DAG:

This DAG extracts data from AWS S3 and loads them into DataFrames for transformations and filtering. It removes transactions with account numbers equal to 0. The cleaned data is saved back to separate CSV files for each batch.
The cleaned CSV files are loaded into the Vertica database, specifically into the STV202307035__STAGING.transactions table.
This process is executed in a loop for each of the 10 batches. This DAG also handles the loading of currency data from 'currencies_history.csv' into the STV202307035__STAGING.currencies table in Vertica.

ETL_dwh DAG:
This dag populates global_metrics_temp table in vertica with aggregated data. This table is directly linked to Metabase dashboard. 

Please note that mock data was used for testing purposes.


