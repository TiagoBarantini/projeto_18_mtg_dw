# Projeto 18 - Magic The Gathering Data Warehouse

Este projeto é uma pipeline de dados que extrai informações da API de Magic: The Gathering, transforma e carrega em um Data Warehouse PostgreSQL, criando tabelas **bronze**, **silver** e **gold**.

## Estrutura do projeto

```
├── dags
│   ├── dag_bronze_mtg.py
│   ├── dag_silver_mtg.py
│   └── dag_gold_mtg.py
├── data
│   ├── bronze
│   ├── silver
│   └── gold
├── docker
│   └── docker-compose.yml
├── src
│   ├── extract
│   ├── transform
│   └── load
├── .env
├── Dockerfile
├── requirements.txt
└── README.md
```

## Funcionalidade das DAGs

- **Bronze:** Extrai dados da API e salva em arquivos `.parquet` na camada bronze.  
- **Silver:** Transforma os dados da camada bronze e gera arquivos `.parquet` prontos para análises intermediárias.  
- **Gold:** Carrega os dados do silver para tabelas do Data Warehouse e aplica cálculos e métricas finais.  

> Agora as DAGs **não dependem uma da outra**, podendo rodar de forma independente.

## Como executar

1. Crie o arquivo `.env` na raiz do projeto com as credenciais do PostgreSQL:

```
POSTGRES_USER=postgres
POSTGRES_PASSWORD=
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=mtg_dw
```

2. Suba o ambiente Docker:

```bash
cd docker
docker compose up -d
```

3. Acesse a interface do Airflow:

```
http://localhost:8080
```

4. Ative as DAGs no Airflow e execute manualmente ou aguarde o schedule.

## Tecnologias usadas

- Python 3.12  
- Airflow  
- Docker & Docker Compose  
- PostgreSQL  
- Pandas  

## Autor

**Tiago Barantini** – Engenheiro de Dados
