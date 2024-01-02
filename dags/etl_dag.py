from datetime import datetime, timedelta

from airflow import DAG 
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
from vnstock import *
import numpy as np

def insert():
    engine = create_engine('postgresql://airflow:airflow@172.18.0.3:5432/stock_db')
    current_date = datetime.now().date().strftime('%Y-%m-%d')
    
    # Lấy giá trị của biến first_run từ Airflow Variable
    first_run = Variable.get("first_run", default_var="false")
    
    # Kiểm tra xem đã chèn dữ liệu lần đầu tiên hay chưa
    if first_run == "false":
        # Lần đầu tiên
        start_date = "2020-01-01"
        end_date = current_date
        Variable.set("first_run", "true")
    else:
        # Chỉ chèn dữ liệu của ngày tiếp theo
        start_date = current_date
        end_date = current_date
    
    stock_data = stock_historical_data(symbol="TCH", start_date=start_date, end_date=end_date, resolution="1D", type="stock", beautify=True, decor=False, source='DNSE')
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
    schedule_interval="0 16 * * *",
    tags=["etl_dag"]
) as dag:
    task1 = PostgresOperator(
        task_id="create_stock_1D_table",
        postgres_conn_id="postgres_localhost",
        sql='''
            CREATE TABLE IF NOT EXISTS d1 (
            time timestamp NOT NULL,
            open numeric NOT NULL,
            high numeric NOT NULL,
            low numeric NOT NULL,
            close numeric NOT NULL,
            volume bigint NOT NULL,
            ticker varchar(10) NOT NULL,
            CONSTRAINT pk_ticker_time PRIMARY KEY (ticker, time)
        );
        '''
    )
    task2 = PythonOperator(
        task_id='insert_data_1D',
        python_callable=insert,
        provide_context=True
    )
    task1 >> task2
