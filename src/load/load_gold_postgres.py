import pandas as pd
from src.utils.db import get_connection

def load_gold():
    # Caminhos dos arquivos parquet do GOLD
    files = {
        'dim_carta': '/opt/airflow/data/gold/dim_carta.parquet',
        'dim_tempo': '/opt/airflow/data/gold/dim_tempo.parquet',
        'dim_cambio': '/opt/airflow/data/gold/dim_cambio.parquet',
        'fato_cartas': '/opt/airflow/data/gold/fato_cartas.parquet'
    }

    engine = get_connection()

    for table_name, path in files.items():
        df = pd.read_parquet(path)
        with engine.begin() as conn:  # garante commit automático
            df.to_sql(table_name, conn, if_exists='replace', index=False)

    print("Load para PostgreSQL concluído ✅")
