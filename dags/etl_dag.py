from datetime import datetime, timedelta

from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from vnstock import *
import numpy as np

# def convert_to_timestamp(my_date):
#     my_datetime = datetime.combine(my_date, datetime.min.time())
#     ict = pytz.timezone('Asia/Ho_Chi_Minh')
#     my_datetime = ict.localize(my_datetime)
#     timestamp = (my_datetime - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
#     return int(timestamp)

def insert():
    engine = create_engine('postgresql://airflow:airflow@172.18.0.3:5432/stock_db')

    # Insert dữ liệu vào bảng d1
    current_date = datetime.now().date().strftime('%Y-%m-%d')
    stock_data = stock_historical_data(symbol="TCH", start_date="2020-01-01", end_date=current_date, resolution="1D", type="stock", beautify=True, decor=False, source='DNSE')
    # stock_data['time']= stock_data['time'].apply(convert_to_timestamp)
    stock_data.to_sql('d1', con=engine, if_exists='append', index=False)
    return True
    
default_args = {
    'owner': "DuongNam",
    "retries":5,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    start_date=datetime(2023, 12, 24),
    schedule_interval="@daily",
    tags=["etl_dag"]
) as dag:
    task1 = PostgresOperator(
        task_id = "create_stock_1D_table",
        postgres_conn_id = "postgres_localhost",
        sql = '''
            CREATE TABLE d1 (
            time timestamp NOT NULL,
            open numeric NOT NULL,
            high numeric NOT NULL,
            low numeric NOT NULL,
            close numeric NOT NULL,
            volume bigint NOT NULL,
            ticker varchar(10) NOT NULL
            );
        '''
    )
    task2 = PythonOperator(
        task_id='insert_data_1D',
        python_callable=insert
    )
    task1 >> task2