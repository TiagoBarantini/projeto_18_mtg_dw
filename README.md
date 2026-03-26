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

## Passo a passo para subir no GitHub

1. Abra o terminal na pasta raiz do projeto:

```bash
cd ~/projeto_18_mtg_dw
```

2. Inicialize o Git (se ainda não estiver inicializado):

```bash
git init
```

3. Adicione a origem remota via SSH:

```bash
git remote add origin git@github.com:TiagoBarantini/projeto_18_mtg_dw.git
```

4. Adicione todos os arquivos para commit:

```bash
git add .
```

5. Faça o commit com uma mensagem descritiva:

```bash
git commit -m "Projeto 18 MTG DW: dags Bronze, Silver, Gold e scripts de transformação"
```

6. Envie para o GitHub (branch principal `main`):

```bash
git branch -M main
git push -u origin main
```

7. Para atualizações futuras:

```bash
git add .
git commit -m "Mensagem de atualização"
git push
```
