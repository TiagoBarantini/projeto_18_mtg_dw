from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 🔥 garante import do src
sys.path.append(os.path.abspath("/opt/airflow"))

import pandas as pd
from src.utils.db import get_connection
from src.transform.transform_silver import transform_silver  # sua função existente

default_args = {
    'owner': 'tiago',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def load_silver_postgres():
    """
    Função que lê o Silver parquet e envia para PostgreSQL
    """
    conn = get_connection()
    silver_path = '/opt/airflow/data/silver/silver_cards.parquet'

    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {silver_path}")

    df = pd.read_parquet(silver_path)

    df.to_sql('silver_cards', conn, if_exists='replace', index=False)
    print("Silver carregado no PostgreSQL ✅")

with DAG(
    dag_id='silver_mtg_pipeline',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['silver', 'dw', 'mtg']
) as dag:

    transform_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver
    )

    load_task = PythonOperator(
        task_id='load_silver',
        python_callable=load_silver_postgres
    )

    # Ordem de execução
    transform_task >> load_task
