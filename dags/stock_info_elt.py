import os
from datetime import datetime, timedelta, date
import yfinance as yf

import polars as pl
import psycopg2
from psycopg2 import extras
from cuallee import Check, CheckLevel
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


URI = "postgresql://airflow:airflow@postgres/airflow"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

db_connection = psycopg2.connect(
    database = "airflow",
    user = "airflow",
    password = "airflow",
    host = "postgres",
    port = 5432
)



def get_hourly_prices(symbol):
    start_date = date.today() - timedelta(days=1)
    end_date = date.today() 
    data = yf.download(symbol, start=start_date, end=end_date, interval='60m')
    data.reset_index(inplace=True)
    data['symbol'] = symbol
    all_data = []
    for _, row in data.iterrows():
        all_data.append((row['Datetime'], symbol, row['Open'], row['High'], row['Low'], row['Close'], row['Volume']))
    return all_data[-1]


def insert_data(data):
    conn = db_connection
    cursor = conn.cursor()
    extras.execute_values(
        cursor, f"""
            INSERT INTO finances.stock_data VALUES %s
            ON CONFLICT (timestamp, symbol) DO NOTHING;
        """, [
            tuple(data)
        ]
    )
  
    conn.commit()

with DAG(
    'stock_info_elt',
    description='A simple DAG to fetch data \
    from AlphVantage API and write to a file',
    start_date=datetime(2024, 1, 1),
    schedule_interval=timedelta(hours=1),
) as dag:
    tickers = ["AAPL", "SPY", "NVDA", "META", "IMAE.AS", "VWCE.DE"]

    @task
    def fetch_stock_info(tickers):
        logger.info("Starting the Stock info thing")
        for symbol in tickers:
            data = get_hourly_prices(symbol)
            insert_data(data)
        logger.info("Finishing the Stock info thing")

    def check_completeness(pl_df, column_name):
        check = Check(CheckLevel.ERROR, "Completeness")
        validation_results_df = check.is_complete(column_name).validate(pl_df)
        return validation_results_df["status"].to_list()

    @task.branch
    def check_data_quality(validation_results):
        if "FAIL" not in validation_results:
            return ['generate_dashboard']
        return ['stop_pipeline']

    check_data_quality_instance = check_data_quality(
        check_completeness(pl.read_database(query="select symbol from finances.stock_data", connection=db_connection), "symbol")
    )

    stop_pipeline = DummyOperator(task_id='stop_pipeline')

    markdown_path = f'{os.getenv("AIRFLOW_HOME")}/visualization/'
    q_cmd = (
        f'cd {markdown_path} && quarto render {markdown_path}/dashboard.qmd'
    )
    gen_dashboard = BashOperator(
        task_id="generate_dashboard", bash_command=q_cmd
    )

    (
        fetch_stock_info(tickers)
        >> check_data_quality_instance
        >> gen_dashboard
    )
    check_data_quality_instance >> stop_pipeline
