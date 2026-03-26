import sys
import os
import pandas as pd
from src.utils.db import get_connection

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

def transform_gold():
    conn = get_connection()

    # LER SILVER
    silver_path = '/opt/airflow/data/silver/silver_cards.parquet'
    if not os.path.exists(silver_path):
        raise FileNotFoundError(f"{silver_path} não encontrado")
    
    df = pd.read_parquet(silver_path)

    # REMOVER COLUNAS DUPLICADAS
    df = df.loc[:, ~df.columns.duplicated()]

    # TRATAMENTOS
    df['date'] = pd.to_datetime(df['collected_at']).dt.date
    df['power'] = pd.to_numeric(df['power'], errors='coerce').fillna(0)
    df['toughness'] = pd.to_numeric(df['toughness'], errors='coerce').fillna(0)

    # Peso por raridade
    rarity_weight = {'Common': 1, 'Uncommon': 2, 'Rare': 3, 'Mythic': 5}
    df['rarity_weight'] = df['rarity'].map(rarity_weight).fillna(1)

    # DIM_CARTA
    dim_carta = df[['card_id', 'name_x', 'type', 'rarity', 'setname']].drop_duplicates().reset_index(drop=True)
    dim_carta['carta_id'] = dim_carta.index + 1

    # DIM_TEMPO
    dim_tempo = df[['date']].drop_duplicates().reset_index(drop=True)
    dim_tempo['data_id'] = dim_tempo.index + 1
    dim_tempo['ano'] = pd.to_datetime(dim_tempo['date']).dt.year
    dim_tempo['mes'] = pd.to_datetime(dim_tempo['date']).dt.month
    dim_tempo['dia'] = pd.to_datetime(dim_tempo['date']).dt.day

    # DIM_CAMBIO
    dim_cambio = df[['date', 'usd_brl']].drop_duplicates()
    dim_cambio = dim_cambio.merge(dim_tempo, on='date', how='left')[['data_id', 'usd_brl']]

    # FACT
    fact = df.merge(dim_carta[['card_id', 'carta_id']], on='card_id')
    fact = fact.merge(dim_tempo[['date', 'data_id']], on='date')
    fact['preco_brl'] = fact['price_usd'] * fact['usd_brl']
    fact['power_score'] = (fact['power'] + fact['toughness']) * fact['rarity_weight']

    fact = fact[['carta_id','data_id','price_usd','preco_brl','power','toughness','power_score']]
    fact.columns = ['carta_id','data_id','preco_usd','preco_brl','power','toughness','power_score']

    # LOAD no Postgres
    dim_carta.to_sql('dim_carta', conn, if_exists='replace', index=False)
    dim_tempo.to_sql('dim_tempo', conn, if_exists='replace', index=False)
    dim_cambio.to_sql('dim_cambio', conn, if_exists='replace', index=False)
    fact.to_sql('fato_cartas', conn, if_exists='replace', index=False)

    print("Gold transform e load concluído ✅")
