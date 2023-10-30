from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
import vertica_python
from io import BytesIO
from airflow.hooks.base_hook import BaseHook



default_args = {
    'owner': 'Nadia',
    'schedule_interval': '@daily',  
    'start_date': datetime(2023, 10, 16),
    'catchup': False,
    'on_failure_callback': True
}

dag = DAG('ETL_dwh', default_args=default_args)

aws_access_key_id = BaseHook.get_connection('aws_access_key_id')
aws_secret_access_key = BaseHook.get_connection('aws_secret_access_key_id')
endpoint_url = 'https://storage.yandexcloud.net'
bucket_name = 'final-project'

vertica_conn_pass = BaseHook.get_connection('password_vertica')
vertica_conn_user = BaseHook.get_connection('user_vertica')
conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': vertica_conn_user,       
             'password': vertica_conn_pass,
             'database': 'dwh',
            'autocommit': True}

 
def insert_data_into_global_metrics():
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TEMPORARY TABLE STV202307035__DWH.global_metrics_temp (
                date_update DATE,
                currency_from INT, 
                amount_total NUMERIC(18, 2), 
                cnt_transactions INT, 
                avg_transactions_per_account NUMERIC, 
                cnt_accounts_make_transactions INT
            ); """ )

        cur.execute ( 
            """ 
            INSERT INTO  STV202307035__DWH.global_metrics_temp (date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
            WITH avg_per_account AS (
                SELECT 
                    CAST(transaction_dt AS DATE) AS date_update,
                    currency_code,
                    account_number_from,
                    AVG(amount) AS avg_amount_per_account
                FROM STV202307035__STAGING.transactions
                GROUP BY account_number_from, CAST(transaction_dt AS DATE), currency_code
            )
            SELECT 
                CAST(tr.transaction_dt AS DATE) AS date_update, 
                tr.currency_code AS currency_from, 
                SUM(tr.amount * cu.currency_with_div) AS amount_total,
                COUNT(tr.operation_id) AS cnt_transactions, 
                AVG(a.avg_amount_per_account) AS avg_transactions_per_account,
                COUNT(DISTINCT tr.account_number_from) AS cnt_accounts_make_transactions
            FROM STV202307035__STAGING.transactions AS tr    
            LEFT JOIN STV202307035__STAGING.currencies AS cu 
                ON tr.currency_code = cu.currency_code
                AND CAST(tr.transaction_dt AS DATE) = cu.date_update
            LEFT JOIN avg_per_account AS a 
                ON tr.account_number_from = a.account_number_from
                AND CAST(tr.transaction_dt AS DATE) = a.date_update
            WHERE cu.currency_code_with = '470'
            GROUP BY CAST(tr.transaction_dt AS DATE), tr.currency_code;
            """ ) 

        cur.execute ( """ 
            COPY STV202307035__DWH.global_metrics (
                date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions
            ) FROM global_metrics_temp;
            """
            )
        cur.close()
    

insert_data_into_global_metrics = PythonOperator(
    task_id='insert_data_into_global_metrics',
    python_callable=insert_data_into_global_metrics,
    dag=dag
)


insert_data_into_global_metrics 