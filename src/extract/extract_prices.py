import requests
import pandas as pd
import os
from datetime import datetime

BRONZE_PATH = "data/bronze/prices.parquet"

def extract_prices():

    url = "https://api.scryfall.com/cards/search?q=game:paper"
    response = requests.get(url)
    data = response.json()["data"]

    records = []
    for card in data:
        records.append({
            "id": card.get("id"),
            "name": card.get("name"),
            "price_usd": card.get("prices", {}).get("usd"),
        })

    df = pd.DataFrame(records)
    df["collected_at"] = datetime.now()

    df = df.dropna(subset=["price_usd"])

    # incremental
    if os.path.exists(BRONZE_PATH):
        df_old = pd.read_parquet(BRONZE_PATH)
        df = pd.concat([df_old, df], ignore_index=True)

    df.to_parquet(BRONZE_PATH, index=False)
    print("Prices Bronze atualizado")

if __name__ == "__main__":
    extract_prices()
