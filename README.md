# Brazilian Ecommerce ETL Data Pipeline Project

![image](https://github.com/user-attachments/assets/9b43f4e0-ca4b-475f-be08-8fe9a994c2fb)


## Introduction
"Brazilian E-commerce ETL data pipeline" project focuses on building an ETL process to handle and analyze e-commerce data from Brazil. The project involves extracting data from MySQL, processing and transforming it through various stages, and visualizing it using Superset. 
1. Data Extraction: Extract data from MySQL and store it as assets in the bronze layer.
2. Data Transformation: Transform data using Pandas.
3. Data Loading: Load transformed data into PostgreSQL.
4. Data Visualization: Connect Power BI with PostgreSQL and create simple charts to visualize data.


## Setup and Execution

Create env and .env file in your project directory and add your environment variables.

Download the corresponding data into the ingest_data/data/ directory

Start the MinIO, MySQL, and PostgreSQL containers. During this process, raw data is also uploaded to MySQL

``` bash
  docker-compose -f docker-compose-storage.yml build
  docker-compose -f docker-compose-storage.yml up -d
```
Start Dagster container and materialize all assets through the Dagster UI.
``` bash
  docker-compose -f docker-compose-dagster.yml build
  docker-compose -f docker-compose-dagster.yml up -d
```
Open your Power BI Desktop, connect to the PostgreSQL databases and create dashboard.

![image](https://github.com/user-attachments/assets/8b180003-fe41-4ccd-97cd-58c5441607ae)


## Tech Stack

**Data Processing:** Python

**Database and Data Storage:** MySQL, PostgreSQL, MinIO

**Ochestration:** Dagster

**Visualization:** Power BI

**Containerization:** Docker


