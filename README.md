# Brazilian Ecommerce ETL Data Pipeline Project

## Introduction
The "Brazilian E-commerce ETL pipelines" project focuses on building a robust ETL (Extract, Transform, Load) process to handle and analyze e-commerce data from Brazil. The project involves extracting data from MySQL, processing and transforming it through various stages, and ultimately visualizing it using Superset. This pipeline ensures efficient data handling, storage, and analysis to derive meaningful insights from the e-commerce data.

## Main Tasks
- **Data Extraction**: Extract data from MySQL using Dagster and store it as assets in the bronze layer.
- **Data Storage**: Store data in Minio for backup and further processing.
- **Data Transformation**: Transform data from the bronze layer to the gold layer using Dagster.
- **Data Loading**: Load transformed data into PostgreSQL for analysis and visualization.
- **Data Visualization**: Connect Superset to PostgreSQL and create simple charts to visualize data.

## Languages:
- **Python**
## Packages:
- **Pandas**
- **SQLAlchemy & MySQL Connector & Psycopg2**
- **Minio**

## Databases:
- **MySQL**
- **PostgreSQL**
- **Superset**
## Object Storage:
- **Minio**
## Ochestrator:
- **Dagster**
## Visualization Tools:
- **Superset**
## Containerization Tools:
- **Docker**

