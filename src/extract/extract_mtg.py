import requests
import pandas as pd
import os
from datetime import datetime

BRONZE_PATH = "data/bronze/mtg_cards.parquet"

def extract_mtg():

    url = "https://api.magicthegathering.io/v1/cards"
    response = requests.get(url)
    data = response.json()["cards"]

    df = pd.DataFrame(data)[[
        "id", "name", "type", "rarity", "setName", "power", "toughness"
    ]]

    df["collected_at"] = datetime.now()

    # INCREMENTAL
    if os.path.exists(BRONZE_PATH):
        df_old = pd.read_parquet(BRONZE_PATH)
        df = df[~df["id"].isin(df_old["id"])]

        df = pd.concat([df_old, df], ignore_index=True)

    df.to_parquet(BRONZE_PATH, index=False)
    print("MTG Bronze atualizado")

if __name__ == "__main__":
    extract_mtg()
