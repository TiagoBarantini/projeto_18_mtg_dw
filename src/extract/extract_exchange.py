import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# carregar .env
load_dotenv()

BRONZE_PATH = "data/bronze/exchange.parquet"

def extract_exchange():

    API_KEY = os.getenv("EXCHANGE_API_KEY")

    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/USD"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Erro na API: {response.status_code}")

    data = response.json()

    usd_brl = data["conversion_rates"]["BRL"]

    df = pd.DataFrame([{
        "data": datetime.now(),
        "usd_brl": usd_brl
    }])

    # incremental (mantém histórico)
    if os.path.exists(BRONZE_PATH):
        df_old = pd.read_parquet(BRONZE_PATH)
        df = pd.concat([df_old, df], ignore_index=True)

    df.to_parquet(BRONZE_PATH, index=False)

    print(f"Câmbio atualizado: USD→BRL = {usd_brl}")


if __name__ == "__main__":
    extract_exchange()
