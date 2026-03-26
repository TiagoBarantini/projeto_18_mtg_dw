from sqlalchemy import create_engine
import os

def get_connection():
    """
    Retorna a conexão SQLAlchemy Engine para o PostgreSQL
    """
    user = os.getenv('POSTGRES_USER')
    password = os.getenv('POSTGRES_PASSWORD')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    db = os.getenv('POSTGRES_DB')

    url = f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}'
    engine = create_engine(url)
    return engine
