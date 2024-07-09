from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import csv
import pandas as pd
import psycopg2
import sqlalchemy
from sqlalchemy import create_engine, MetaData, Table, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.engine.base import Connection
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.inspection import inspect
from sqlalchemy.sql.expression import Insert
import time
import pandas as pd

default_args = {
    "owner": "AMaltsev",
    "start_date" : datetime (2024, 2, 25),
    "catchup":False
    #,"retries" : 1
}

def log_event(event, table_name):
    # Получение SQLAlchemy engine
    postgres_hook = PostgresHook("postgres_log")
    engine = postgres_hook.get_sqlalchemy_engine()
    # Создание соединения
    with engine.begin() as conn:
        conn.execute("""
            INSERT INTO logs.table_logs (dag_id, task_id, event, execution_date)
            VALUES (%s, %s, %s, %s);
        """, ('POINT', table_name, event, datetime.now()))

def insert_data(table_name):
    # Логирование начала загрузки
    log_event('start', table_name)

    # Пауза на 5 секунд
    time.sleep(5)

    # Чтение данных из CSV
    df = pd.read_csv(f"/opt/airflow/files/{table_name}.csv", delimiter=";", encoding="UTF-8", infer_datetime_format=True)

    # Приведение имен столбцов к нижнему регистру
    df.columns = [column.lower() for column in df.columns]
    # Удаление кавычек из имен столбцов, если это необходимо
    df.columns = [column.replace('"', '') for column in df.columns]

    # Преобразование столбцов в формат datetime
    for column in df.columns:
        if df[column].dtype == 'object':
            try:
                df[column] = pd.to_datetime(df[column], format='mixed')
            except ValueError:
                # Если преобразование не удалось, значит столбец не содержит даты
                continue

    # Получение SQLAlchemy engine
    postgres_hook = PostgresHook("postgres_db")
    engine = postgres_hook.get_sqlalchemy_engine()

   # Проверка существования временной таблицы и её удаление, если она есть
    inspector = inspect(engine)
    temp_table_name = f"{table_name}_temp"
    with engine.begin() as conn:
        if inspector.has_table(temp_table_name, schema='ds'):
            conn.execute(f'DROP TABLE IF EXISTS ds.{temp_table_name}')

    # Загрузка данных таблицы
    df.to_sql(temp_table_name, engine, schema='ds', if_exists='append', index=False)

    with engine.connect() as conn:
        # Получение первичных ключей основной таблицы
        primary_keys_query = f'''
                                SELECT STRING_AGG(a.attname, ',')
                                FROM pg_index i JOIN pg_attribute a
                                ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                                WHERE i.indrelid ='ds.{table_name}'::regclass AND i.indisprimary
                                '''
        primary_keys_result = conn.execute(primary_keys_query)
        primary_keys = primary_keys_result.fetchone()[0]

        # Определение столбцов для обновления
        update_columns = ", ".join([f'"{column}" = EXCLUDED."{column}"' for column in df.columns])

        # Обновление основной таблицы данными из временной таблицы
        conflict_action = f'''ON CONFLICT ({primary_keys}) DO UPDATE SET {update_columns}'''
        insert_query = f'''
                        INSERT INTO ds.{table_name} 
                        SELECT * FROM ds.{temp_table_name} 
                        {conflict_action}
                        '''
        conn.execute(insert_query)

        # Удаление временной таблицы
        conn.execute(f'DROP TABLE ds.{temp_table_name}')

    # Логирование окончания загрузки
    log_event('end', table_name)

with DAG('TASK1_1',
    default_args=default_args,
    description="Загрузка данных",
    schedule_interval = timedelta(days=1),
    start_date = days_ago(1)) as dag:
    
    start = DummyOperator(
        task_id="start"
    )
    
    task_pstgrs_crt_sch_ds = PostgresOperator(
        task_id = 'task_cr_sch_ds', 
        postgres_conn_id = 'postgres_db',
        sql = """CREATE SCHEMA IF NOT EXISTS ds;"""
    )

    task_pstgrs_crt_tbl_ds = PostgresOperator(
        task_id = 'task_cr_tbl_ds', 
        postgres_conn_id = 'postgres_db',
        sql = """   
                    DO $$
                    DECLARE
                    table_exist int;
                    BEGIN
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ds' AND table_name = 'ft_balance_f' 
                    INTO table_exist;
                    IF table_exist IS NULL 
                        THEN 
                            create table ds.ft_balance_f(
                            nm int not null,
                            on_date date not null,
                            account_rk int8 not null,
                            currency_rk int8,
                            balance_out float,
                            primary key(account_rk, on_date)
                            );
                        ELSE  RAISE NOTICE 'ft_balance_f уже существует';
		            END IF;
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ds' AND table_name = 'ft_posting_f' 
                    INTO table_exist;
                    IF table_exist IS NULL 
                        THEN
                            create table ds.ft_posting_f(
                            nm int not  null,
                            oper_date date not null,
                            credit_account_rk int8 not null,
                            debet_account_rk int8 not null,
                            credit_amount float,
                            debet_amount float,
                            primary key(nm, oper_date, credit_account_rk, debet_account_rk)
                            );
                    ELSE  RAISE NOTICE 'ft_posting_f уже существует';
		            END IF;
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ds' AND table_name = 'md_account_d' 
                    INTO table_exist;
                    IF table_exist IS NULL 
                        THEN
                            create table ds.md_account_d(
                            nm int not  null,
                            data_actual_date date not null,
                            data_actual_end_date date not null,
                            account_rk int8 not null,
                            account_number varchar(20) not null,
                            char_type varchar(1) not null,
                            currency_rk int not null,
                            currency_code varchar(3) not null,
                            primary key(data_actual_date, account_rk)
                            );
                    ELSE RAISE NOTICE 'md_account_d уже существует';
		            END IF;
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ds' AND table_name = 'md_currency_d' 
                    INTO table_exist;
                    IF table_exist IS NULL 
                        THEN
                            create table ds.md_currency_d(
                            nm int not  null,
                            currency_rk int not null,
                            data_actual_date date not null,
                            data_actual_end_date date,
                            currency_code varchar(3),
                            code_iso_char varchar(3),
                            primary key(currency_rk, data_actual_date)
                            );
                    ELSE RAISE NOTICE 'md_account_d уже существует';
		            END IF;
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ds' AND table_name = 'md_exchange_rate_d' 
                    INTO table_exist;
                    IF table_exist IS NULL 
                        THEN
                            create table ds.md_exchange_rate_d(
                            nm int not  null,
                            data_actual_date date not null,
                            data_actual_end_date date,
                            currency_rk int not null,
                            reduced_cource float,
                            code_iso_num varchar(3),
                            primary key(nm,data_actual_date, currency_rk)
                            );
                    ELSE RAISE NOTICE 'md_exchange_rate_d уже существует';
		            END IF;
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = 'ds' AND table_name = 'md_ledger_account_s' 
                    INTO table_exist;
                    IF table_exist IS NULL 
                        THEN
                            create table ds.md_ledger_account_s(
                            nm int not  null,   
                            chapter char(1),
                            chapter_name varchar(16),
                            section_number integer,
                            section_name varchar(22),
                            subsection_name varchar(21),
                            ledger1_account integer,
                            ledger1_account_name varchar(47),
                            ledger_account integer not null,
                            ledger_account_name varchar(153),
                            characteristic char(1),
                            is_resident integer,
                            is_reserve integer,
                            is_reserved integer,
                            is_loan integer,
                            is_reserved_assets integer,
                            is_overdue integer,
                            is_interest integer,
                            pair_account varchar,
                            start_date date not null,
                            end_date date,
                            is_rub_only integer,
                            min_term varchar(1),
                            min_term_measure varchar(1),
                            max_term varchar(1),
                            max_term_measure varchar(1),
                            ledger_acc_full_name_translit varchar(1),
                            is_revaluation varchar(1),
                            is_correct varchar(1),
                            primary key(ledger_account, start_date)
                    );
                    ELSE RAISE NOTICE 'md_exchange_rate_d уже существует';
		            END IF;
                    END$$;
                    """
    )
                   
    ft_balance_f = PythonOperator(
        task_id="ft_balance_f",
        python_callable=insert_data, 
        op_kwargs={
                "table_name": "ft_balance_f",
                'conn_id':'posgres_db'
                },
        dag=dag
    )

    ft_posting_f = PythonOperator(
        task_id="ft_posting_f",
        python_callable=insert_data, 
        op_kwargs={
                "table_name": "ft_posting_f",
                'conn_id':'posgres_db'
                } ,       
        dag=dag
    )

    md_account_d = PythonOperator(
        task_id="md_account_d",
        python_callable=insert_data, 
        op_kwargs={
                "table_name": "md_account_d",
                'conn_id':'posgres_db'
                } ,       
        dag=dag
    ) 

    md_currency_d = PythonOperator(
        task_id="md_currency_d",
        python_callable=insert_data, 
        op_kwargs={"table_name": "md_currency_d",
                   'conn_id':'posgres_db'} ,       
        dag=dag
    )

    md_exchange_rate_d = PythonOperator(
        task_id="md_exchange_rate_d",
        python_callable=insert_data, 
        op_kwargs={"table_name": "md_exchange_rate_d",
                   'conn_id':'posgres_db'} ,       
        dag=dag
    )

    md_ledger_account_s = PythonOperator(
        task_id="md_ledger_account_s",
        python_callable=insert_data, 
        op_kwargs={"table_name": "md_ledger_account_s",
                   'conn_id':'posgres_db'} ,       
        dag=dag
    )

    task_pstgrs_crt_sch_logs = PostgresOperator(
        task_id = 'task_cr_sch_logs', 
        postgres_conn_id = 'postgres_log',
        sql = """CREATE SCHEMA IF NOT EXISTS logs;"""
    )

    task_pstgrs_crt_tbl_logs = PostgresOperator(
        task_id = 'task_cr_tbl_logs', 
        postgres_conn_id = 'postgres_log',
        sql = """drop table if exists logs.table_logs;
                    create table logs.table_logs(
                    id SERIAL PRIMARY KEY,
                    dag_id VARCHAR(255),
                    task_id VARCHAR(255),
                    event VARCHAR(50),
                    execution_date TIMESTAMP
                    );
                    """
    )

    end = DummyOperator(
    task_id="end"
    )

    (
    start 
    >> task_pstgrs_crt_sch_ds >> task_pstgrs_crt_sch_logs
    >> task_pstgrs_crt_tbl_ds >> task_pstgrs_crt_tbl_logs
    >> [ft_balance_f, ft_posting_f, md_account_d, md_currency_d, md_exchange_rate_d, md_ledger_account_s]
    >> end
    )