from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

# garantir import do src
sys.path.append('/opt/airflow')

from src.extract.extract_mtg import extract_mtg
from src.extract.extract_prices import extract_prices
from src.extract.extract_exchange import extract_exchange

default_args = {
    'owner': 'tiago',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='bronze_mtg_pipeline',
    default_args=default_args,
    description='Pipeline Bronze MTG (incremental)',
    schedule_interval='*/30 * * * *',  # ⏰ 30 minutos
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    task_mtg = PythonOperator(
        task_id='extract_mtg_cards',
        python_callable=extract_mtg
    )

    task_prices = PythonOperator(
        task_id='extract_prices',
        python_callable=extract_prices
    )

    task_exchange = PythonOperator(
        task_id='extract_exchange',
        python_callable=extract_exchange
    )

    # execução paralela (mais profissional)
    [task_mtg, task_prices, task_exchange]
