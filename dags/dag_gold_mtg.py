from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# 🔥 garante import do src
sys.path.append(os.path.abspath("/opt/airflow"))

import pandas as pd
from src.utils.db import get_connection
from src.transform.transform_gold import transform_gold  # sua função existente

default_args = {
    'owner': 'tiago',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

def load_gold_postgres():
    """
    Função que lê o Gold parquet e envia para PostgreSQL
    """
    conn = get_connection()
    gold_path = '/opt/airflow/data/gold/fato_cartas.parquet'

    if not os.path.exists(gold_path):
        raise FileNotFoundError(f"Arquivo não encontrado: {gold_path}")

    df = pd.read_parquet(gold_path)

    df.to_sql('fato_cartas', conn, if_exists='replace', index=False)
    print("Gold carregado no PostgreSQL ✅")

with DAG(
    dag_id='gold_mtg_pipeline',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['gold', 'dw', 'mtg']
) as dag:

    transform_task = PythonOperator(
        task_id='transform_gold',
        python_callable=transform_gold
    )

    load_task = PythonOperator(
        task_id='load_gold',
        python_callable=load_gold_postgres
    )

    # Ordem de execução
    transform_task >> load_task
