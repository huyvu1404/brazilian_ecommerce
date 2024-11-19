# Brazilian Ecommerce ETL Data Pipeline Project

![image](https://github.com/user-attachments/assets/9b43f4e0-ca4b-475f-be08-8fe9a994c2fb)


## Introduction
"Brazilian E-commerce ETL data pipeline" project focuses on building an ETL process to handle and analyze e-commerce data from Brazil. The project involves extracting data from MySQL, processing and transforming it through various stages, and visualizing it using Superset. 
1. Data Extraction: Extract data from MySQL and store it as assets in the bronze layer.
2. Data Transformation: Transform data using Pandas.
3. Data Loading: Load transformed data into PostgreSQL.
4. Data Visualization: Connect Superset to PostgreSQL and create simple charts to visualize data.


## Deployment

 First, start MinIO, MySQL, and PostgreSQL containers. In this process, raw data are also uploaded into MySQL.

``` bash
  docker-compose -f docker-compose-storage.yml up -d
```
Start Dagster container and materialize all assets in Dagster UI.
``` bash
  docker-compose -f docker-compose-dagster.yml up -d
```
Start Superset container. Go to Superset UI, connect to PostgreSQL databases, and create some charts.
``` bash
  docker-compose -f docker-compose-superset.yml up -d
```


## Tech Stack

**Data Processing:** Python

**Database and Data Storage:** MySQL, PostgreSQL, MinIO

**Ochestration:** Dagster

**Visualization:** Superset

**Containerization:** Docker


