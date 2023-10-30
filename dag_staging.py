from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
import vertica_python
from io import BytesIO



default_args = {
    'owner': 'Nadia',
    'schedule_interval': '@daily',  
    'start_date': datetime(2023, 10, 16),
    'catchup': False,
    'on_failure_callback': True
}

dag = DAG('ETL_stg', default_args=default_args)



aws_access_key_id = BaseHook.get_connection('aws_access_key_id')
aws_secret_access_key = BaseHook.get_connection('aws_secret_access_key_id')

vertica_conn_pass = BaseHook.get_connection('password_vertica')
vertica_conn_user = BaseHook.get_connection('user_vertica')


endpoint_url = 'https://storage.yandexcloud.net'
bucket_name = 'final-project'

conn_info = {'host': '51.250.75.20', 
             'port': '5433',
             'user': vertica_conn_user,       
             'password': vertica_conn_pass,
             'database': 'dwh',
            'autocommit': True}



def s3_get(file_name):
    try:
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=endpoint_url)
        response = s3.get_object(Bucket=bucket_name, Key=file_name)
        data = response['Body'].read()
        df = pd.read_csv(BytesIO(data), encoding='utf-8')
        return df
    except Exception as e:
        print("An error:", str(e))

def s3_get_and_insert_transactions_batches():
    for i in range(1,11):
        df_transaction = s3_get(f'transactions_batch_{i}.csv')
        df_transaction = df_transaction[(df_transaction['account_number_from'] != 0)  & (df_transaction['account_number_to'] != 0)]
        df_transaction.to_csv(f'transactions_batch_{i}.csv', index=False, header=False, sep=',')

        with vertica_python.connect(**conn_info) as conn:
            cur = conn.cursor()
            cur.execute(
                f"""
                COPY STV202307035__STAGING.transactions (
                operation_id,
                account_number_from,
                account_number_to,
                currency_code,
                country,
                status,
                transaction_type,
                amount,
                transaction_dt)
                FROM LOCAL 'transactions_batch_{i}.csv' 
                DELIMITER ','
                REJECTED DATA AS TABLE STV202307035__STAGING.transactions_rej
                REJECTMAX 2; 
                """
                )
            cur.close()

def s3_get_and_insert_currencies():
    df_currencies = s3_get("currencies_history.csv")
    df_currencies.to_csv('df_currencies.csv', index=False, header=False, sep=',')
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            COPY STV202307035__STAGING.currencies (currency_code, currency_code_with, date_update, currency_with_div)
            FROM LOCAL 'df_currencies.csv' DELIMITER ','
            REJECTED DATA AS TABLE STV202307035__STAGING.currencies_rej
            REJECTMAX 2; 
            """
        )
        cur.close()


s3_get_and_insert_transactions_batches = PythonOperator(
    task_id='s3_get_and_insert_transactions_batches',
    python_callable=s3_get_and_insert_transactions_batches,
    dag=dag
)

s3_get_and_insert_currencies = PythonOperator(
    task_id='s3_get_and_insert_currencies',
    python_callable=s3_get_and_insert_currencies,
    dag=dag
)


s3_get_and_insert_transactions_batches >> s3_get_and_insert_currencies