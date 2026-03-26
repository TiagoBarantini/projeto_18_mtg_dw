import pandas as pd
import os
from datetime import datetime

BRONZE_MTG = "data/bronze/mtg_cards.parquet"
BRONZE_PRICES = "data/bronze/prices.parquet"
BRONZE_EXCHANGE = "data/bronze/exchange.parquet"

SILVER_PATH = "data/silver/silver_cards.parquet"


def transform_silver():

    # LOAD
    df_mtg = pd.read_parquet(BRONZE_MTG)
    df_prices = pd.read_parquet(BRONZE_PRICES)
    df_exchange = pd.read_parquet(BRONZE_EXCHANGE)

    # PADRONIZAÇÃO
    df_mtg.columns = df_mtg.columns.str.lower()
    df_prices.columns = df_prices.columns.str.lower()

    df_mtg = df_mtg.rename(columns={"id": "card_id"})
    df_prices = df_prices.rename(columns={"id": "card_id"})
    
    # LIMPEZA
    df_mtg = df_mtg.drop_duplicates(subset=["card_id"])
    df_prices = df_prices.drop_duplicates(subset=["card_id"])

    df_prices["price_usd"] = pd.to_numeric(df_prices["price_usd"], errors="coerce")
    
    # JOIN
    df = df_mtg.merge(df_prices, on="card_id", how="left")

    # CÂMBIO (MANTER COLUNA)
    latest_rate = df_exchange.sort_values("data", ascending=False).iloc[0]["usd_brl"]

    df["usd_brl"] = latest_rate

    # DATA (IMPORTANTE PRO GOLD)
    df["collected_at"] = datetime.now()

    # NUMÉRICOS
    df["power"] = pd.to_numeric(df["power"], errors="coerce").fillna(0)
    df["toughness"] = pd.to_numeric(df["toughness"], errors="coerce").fillna(0)

    # INCREMENTAL
    if os.path.exists(SILVER_PATH):
        df_old = pd.read_parquet(SILVER_PATH)
        df = pd.concat([df_old, df], ignore_index=True)
        df = df.drop_duplicates(subset=["card_id"], keep="last")

    # SAVE
    df.to_parquet(SILVER_PATH, index=False)

    print("Silver pronto para o GOLD 🚀")


if __name__ == "__main__":
    transform_silver()
