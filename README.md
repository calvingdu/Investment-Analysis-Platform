# Investment Analysis Platform

## Overview

This is a data engineering/science focused project that analyzes the impact of major News Headlines on various investment instruments, including cryptocurrencies (Bitcoin, Ethereum), stock prices, and commodities (bonds, gold). The project aims to determine the type of and impact of news that lead to increased volatility or significant price changes in these investment instruments.

## Project Structure

1. **Data Sources**:
   - **Cryptocurrency Prices**: Bitcoin and Ethereum data from a Crypto API.
   - **Stock Prices**: Various stock prices from a Stock API.
   - **News Articles**: Data from a News API.

2. **Tech Stack**:
   - **Apache Airflow**: Orchestrates data pipelines and schedules ETL processes.
   - **Python**: For scripting, data ingestion, and transformation.
   - **Apache Spark**: For processing data from the News API.
   - **PostgreSQL**: Database for storing raw (Bronze layer), cleaned (Silver layer), and aggregated (Gold layer) data.
   - **Docker**: Containerization of all services to ensure consistency across environments.
   - **Hugging Face**:

3. **Workflow Structure**:
   - **Data Ingestion DAGs**: Separate Airflow DAGs for each API to perform ETL processes. Generally done with only Python.
   - **Data Aggregation DAG**: Aggregates and feature engineers data from the Silver layer, storing results in the Gold layer. This is usually done with Spark.

4. **Data Pipeline Architecture**:
   - **Bronze Layer**: Raw data storage in PostgreSQL.
   - **Silver Layer**: Cleaned and enriched data in PostgreSQL.
   - **Gold Layer**: Aggregated and feature-engineered data in PostgreSQL.

5. **Data Tables**:


## Setup Instructions

### Prerequisites
1. Make an .env file with API keys (Crypto API, Stock API, News API)
2. Build Docker Containers
- make build
3. Start the Containers
- make up
4. Review DAG UI on localhost
