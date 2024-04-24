from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from sqlalchemy import create_engine
from vnstock import *
import numpy as np

import scripts.company
import scripts.m1
import scripts.m15
import scripts.m30
import scripts.h1
import scripts.d1
from scripts.insert_data import *

default_args = {"owner": "DuongNam", "retries": 5, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="etl_dag",
    default_args=default_args,
    description='Data pipeline to process Stock data',
    start_date=datetime(2024, 4, 23),
    schedule_interval='@daily',
    tags=["etl_dag"],
) as dag:

    create_company_table = PostgresOperator(
        task_id="create_company_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS company_info (
                companyname VARCHAR(255),
                ticker VARCHAR(10) PRIMARY KEY
            );
        """,
    )
    create_m1_table = PostgresOperator(
        task_id="create_m1_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS m1 (
                        time timestamp NOT NULL,
                        open numeric NOT NULL,
                        high numeric NOT NULL,
                        low numeric NOT NULL,
                        close numeric NOT NULL,
                        volume bigint NOT NULL,
                        ticker varchar(10) NOT NULL,
                        CONSTRAINT pk_m1 PRIMARY KEY (ticker, time)
                    );
        """,
    )
    create_m15_table = PostgresOperator(
        task_id="create_m15_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS m15 (
                        time timestamp NOT NULL,
                        open numeric NOT NULL,
                        high numeric NOT NULL,
                        low numeric NOT NULL,
                        close numeric NOT NULL,
                        volume bigint NOT NULL,
                        ticker varchar(10) NOT NULL,
                        CONSTRAINT pk_m15 PRIMARY KEY (ticker, time)
                    );
        """,
    )
    create_m30_table = PostgresOperator(
        task_id="create_m30_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS m30 (
                time timestamp NOT NULL,
                open numeric NOT NULL,
                high numeric NOT NULL,
                low numeric NOT NULL,
                close numeric NOT NULL,
                volume bigint NOT NULL,
                ticker varchar(10) NOT NULL,
                CONSTRAINT pk_m30 PRIMARY KEY (ticker, time)
            );
        """,
    )
    create_h1_table = PostgresOperator(
        task_id="create_h1_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS h1 (
                time timestamp NOT NULL,
                open numeric NOT NULL,
                high numeric NOT NULL,
                low numeric NOT NULL,
                close numeric NOT NULL,
                volume bigint NOT NULL,
                ticker varchar(10) NOT NULL,
                CONSTRAINT pk_h1 PRIMARY KEY (ticker, time)
            );
        """,
    )
    create_d1_table = PostgresOperator(
        task_id="create_d1_table",
        postgres_conn_id="postgres_localhost",
        sql="""
            CREATE TABLE IF NOT EXISTS d1 (
                time timestamp NOT NULL,
                open numeric NOT NULL,
                high numeric NOT NULL,
                low numeric NOT NULL,
                close numeric NOT NULL,
                volume bigint NOT NULL,
                ticker varchar(10) NOT NULL,
                CONSTRAINT pk_d1 PRIMARY KEY (ticker, time)
            );
        """,
    )

    insert_data_company = PythonOperator(
        task_id="insert_data_company",
        python_callable=insert_company,
        provide_context=True,
    )

    insert_data_m1 = PythonOperator(
        task_id="insert_data_m1", python_callable=insert_m1, provide_context=True
    )

    insert_data_m15 = PythonOperator(
        task_id="insert_data_m15", python_callable=insert_m15, provide_context=True
    )

    insert_data_m30 = PythonOperator(
        task_id="insert_data_m30", python_callable=insert_m30, provide_context=True
    )

    insert_data_h1 = PythonOperator(
        task_id="insert_data_h1", python_callable=insert_h1, provide_context=True
    )

    insert_data_d1 = PythonOperator(
        task_id="insert_data_d1", python_callable=insert_d1, provide_context=True
    )

    create_company_table >> insert_data_company
    create_m1_table >> insert_data_m1 
    create_m15_table >> insert_data_m15 
    create_m30_table >> insert_data_m30 
    create_h1_table >> insert_data_h1 
    create_d1_table >> insert_data_d1
