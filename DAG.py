from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pandas as pd
import io
import json
from datetime import datetime

# Conversion rates dictionary
conversion_rates = {
    "India": 1.0,
    "Japan": 0.65,
    "Norway": 8.5,
    "Sri Lanka": 1.8,
    "HongKong": 10.93,
    "Oman": 200.0,
    "Germany": 90.0,
    "Qatar": 22.0
}

# GCS buckets and files
JAPAN_BUCKET = "global-sales-data"
JAPAN_FILE = "japan_sales.csv"

SRI_LANKA_BUCKET = "global-sales-data_2"
SRI_LANKA_FILE = "sri_lanka_sales.json"

HONGKONG_BUCKET = "global-sales-data"
HONGKONG_FILE = "hong_kong_sales.xlsx"

def fetch_india_data(**context):
    mssql = MsSqlHook(
        mssql_conn_id=None,
        host='35.225.137.135',
        login='sqlserver',
        password='12345',
        schema='Schema 1',
        port=1433
    )
    query = "SELECT PId, Pname, PCategory, QtySold, Price, Amount FROM dbo.india_sales_data"
    df = mssql.get_pandas_df(query)
    df['Country'] = 'India'
    context['ti'].xcom_push(key='india_data', value=df.to_json())

def fetch_japan_data(**context):
    gcs = GCSHook()
    data = gcs.download(JAPAN_BUCKET, JAPAN_FILE)
    df = pd.read_csv(io.BytesIO(data))
    df['Country'] = 'Japan'
    context['ti'].xcom_push(key='japan_data', value=df.to_json())

def fetch_norway_data(**context):
    pg = PostgresHook(
        postgres_conn_id=None,
        host='172.27.48.2',
        login='admin',
        password='admin',
        schema='norway_schema',
        port=5432
    )
    query = "SELECT PId, Pname, PCategory, QtySold, Price, Amount FROM norway_sales_data"
    df = pg.get_pandas_df(query)
    df['Country'] = 'Norway'
    context['ti'].xcom_push(key='norway_data', value=df.to_json())

def fetch_srilanka_data(**context):
    gcs = GCSHook()
    data = gcs.download(SRI_LANKA_BUCKET, SRI_LANKA_FILE)
    records = json.loads(data)
    df = pd.json_normalize(records)
    df['Country'] = 'Sri Lanka'
    context['ti'].xcom_push(key='srilanka_data', value=df.to_json())

def fetch_hongkong_data(**context):
    gcs = GCSHook()
    data = gcs.download(HONGKONG_BUCKET, HONGKONG_FILE)
    df = pd.read_excel(io.BytesIO(data))
    df['Country'] = 'HongKong'
    context['ti'].xcom_push(key='hongkong_data', value=df.to_json())

def fetch_mysql_data(country, host, login, password, schema, table, ti):
    mysql = MySqlHook(
        mysql_conn_id=None,
        host=host,
        login=login,
        password=password,
        schema=schema,
        port=3306
    )
    query = f"SELECT PId, Pname, PCategory, QtySold, Price, Amount FROM {table}"
    df = mysql.get_pandas_df(query)
    df['Country'] = country
    ti.xcom_push(key=f'{country.lower()}_data', value=df.to_json())

def merge_clean_transform(**context):
    countries = ['india', 'japan', 'norway', 'srilanka', 'hongkong', 'oman', 'germany', 'qatar']
    dfs = []
    for c in countries:
        json_str = context['ti'].xcom_pull(key=f'{c}_data')
        df = pd.read_json(json_str)
        dfs.append(df)
    full_df = pd.concat(dfs, ignore_index=True)
    full_df = full_df.dropna()
    
    def convert(row):
        rate = conversion_rates.get(row['Country'], 1)
        inr_val = row['Amount'] * rate
        tax_val = inr_val * 0.05
        return pd.Series([inr_val, tax_val])
    
    full_df[['inr_conversion', 'tax5%']] = full_df.apply(convert, axis=1)
    full_df['inr_conversion'] = full_df['inr_conversion'].round(2)
    full_df['tax5%'] = full_df['tax5%'].round(2)

    csv_buffer = io.StringIO()
    full_df.to_csv(csv_buffer, index=False)
    context['ti'].xcom_push(key='final_clean_data', value=csv_buffer.getvalue())

def load_to_bigquery(**context):
    csv_str = context['ti'].xcom_pull(key='final_clean_data')
    df = pd.read_csv(io.StringIO(csv_str))

    bq_hook = BigQueryHook()
    dataset_id = "global_sales_dataset"
    table_id = "output"

    bq_hook.insert_rows_from_dataframe(
        dataset_id=dataset_id,
        table_id=table_id,
        dataframe=df,
        write_disposition='WRITE_TRUNCATE'
    )

from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='sales_data_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id='fetch_india_data',
        python_callable=fetch_india_data
    )
    t2 = PythonOperator(
        task_id='fetch_japan_data',
        python_callable=fetch_japan_data
    )
    t3 = PythonOperator(
        task_id='fetch_norway_data',
        python_callable=fetch_norway_data
    )
    t4 = PythonOperator(
        task_id='fetch_srilanka_data',
        python_callable=fetch_srilanka_data
    )
    t5 = PythonOperator(
        task_id='fetch_hongkong_data',
        python_callable=fetch_hongkong_data
    )
    t6 = PythonOperator(
        task_id='fetch_oman_data',
        python_callable=fetch_mysql_data,
        op_kwargs={
            'country':'Oman',
            'host':'35.222.124.157',
            'login':'admin',
            'password':'admin',
            'schema':'Oman_sales',
            'table':'Oman_sales_data',
            'ti': '{{ ti }}'
        }
    )
    t7 = PythonOperator(
        task_id='fetch_germany_data',
        python_callable=fetch_mysql_data,
        op_kwargs={
            'country':'Germany',
            'host':'35.222.124.157',
            'login':'admin',
            'password':'admin',
            'schema':'germany_sales',
            'table':'germany_sales_data',
            'ti': '{{ ti }}'
        }
    )
    t8 = PythonOperator(
        task_id='fetch_qatar_data',
        python_callable=fetch_mysql_data,
        op_kwargs={
            'country':'Qatar',
            'host':'35.222.124.157',
            'login':'admin',
            'password':'admin',
            'schema':'globalsales1',
            'table':'qatar_sales_data',
            'ti': '{{ ti }}'
        }
    )

    t9 = PythonOperator(
        task_id='merge_clean_transform',
        python_callable=merge_clean_transform
    )

    t10 = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )

    [t1, t2, t3, t4, t5, t6, t7, t8] >> t9 >> t10
