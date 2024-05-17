# Investment Analysis Platform

## Overview

The Investment Analysis Platform is a data science project that analyzes the impact of major geopolitical events in the United States, China, and Russia on various investment instruments, including cryptocurrencies (Bitcoin, Ethereum), stock prices, and commodities (bonds, gold). The project aims to determine if events like elections and wars lead to increased volatility or significant price changes in these investment instruments.

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

3. **Data Pipeline Architecture**:
   - **Bronze Layer**: Raw data storage in PostgreSQL.
   - **Silver Layer**: Cleaned and enriched data in PostgreSQL.
   - **Gold Layer**: Aggregated and feature-engineered data in PostgreSQL.

4. **Workflow Structure**:
   - **Data Ingestion DAGs**: Separate Airflow DAGs for each API.
   - **Data Processing DAGs**: Python processes data from four APIs; Spark processes data from the News API.
   - **Data Aggregation DAG**: Aggregates and feature engineers data from the Silver layer, storing results in the Gold layer.

## Setup Instructions

### Prerequisites

- Docker and Docker Compose
- Python 3.11+
- API keys for the various data sources (Crypto API, Stock API, BEA API, Reddit API, News API)
