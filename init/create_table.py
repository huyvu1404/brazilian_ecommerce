# create_table.py
import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import pandas as pd
import os

mysql_config = {
    "host": "host.docker.internal",
    "port": 3307,
    "user": "root",
    "password": "root",
}

try:
    with  mysql.connector.connect(**mysql_config) as connection:
        try:
            cursor = connection.cursor()
            create_table_query = """ 
            DROP DATABASE IF EXISTS fde;
            CREATE DATABASE fde;
            USE fde;
            
            DROP TABLE IF EXISTS olist_geolocation_dataset;
            CREATE TABLE olist_geolocation_dataset (
                geolocation_zip_code_prefix int4,
                geolocation_lat float4,
                geolocation_lng float4,
                geolocation_city varchar(64),
                geolocation_state varchar(4)
            );

            DROP TABLE IF EXISTS olist_sellers_dataset;
            CREATE TABLE olist_sellers_dataset (
                seller_id varchar(32),
                seller_zip_code_prefix int4,
                seller_city varchar(64),
                seller_state varchar(4),
                PRIMARY KEY (seller_id)
            
            );

            DROP TABLE IF EXISTS olist_customers_dataset;
            CREATE TABLE olist_customers_dataset (
                customer_id varchar(32),
                customer_unique_id varchar(32),
                customer_zip_code_prefix int4,
                customer_city varchar(64),
                customer_state varchar(4),
                PRIMARY KEY (customer_id)
             
            );
    
            DROP TABLE IF EXISTS product_category_name_translation;
            CREATE TABLE product_category_name_translation (
                product_category_name varchar(64),
                product_category_name_english varchar(64)
            );
               
            DROP TABLE IF EXISTS olist_products_dataset;
            CREATE TABLE olist_products_dataset (
                product_id varchar(32),
                product_category_name varchar(64),
                product_name_lenght int4,
                product_description_lenght int4,
                product_photos_qty int4,
                product_weight_g int4,
                product_length_cm int4,
                product_height_cm int4,
                product_width_cm int4,
                PRIMARY KEY (product_id)
            );


            DROP TABLE IF EXISTS olist_orders_dataset;
            CREATE TABLE olist_orders_dataset (
                order_id varchar(32),
                customer_id varchar(32),
                order_status varchar(16),
                order_purchase_timestamp varchar(32),
                order_approved_at varchar(32),
                order_delivered_carrier_date varchar(32),
                order_delivered_customer_date varchar(32),
                order_estimated_delivery_date varchar(32),
                PRIMARY KEY(order_id),
                FOREIGN KEY (customer_id) REFERENCES olist_customers_dataset(customer_id)
            );

            DROP TABLE IF EXISTS olist_order_items_dataset;
            CREATE TABLE olist_order_items_dataset (
                order_id varchar(32),
                order_item_id int4,
                product_id varchar(32),
                seller_id varchar(32),
                shipping_limit_date varchar(32),
                price float4,
                freight_value float4,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (order_id, order_item_id, product_id, seller_id),
                FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id),
                FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id),
                FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset(seller_id)
            );

            DROP TABLE IF EXISTS olist_order_payments_dataset;
            CREATE TABLE olist_order_payments_dataset (
                order_id varchar(32),
                payment_sequential int4,
                payment_type varchar(16),
                payment_installments int4,
                payment_value float4,
                PRIMARY KEY (order_id, payment_sequential)
            );

            DROP TABLE IF EXISTS olist_order_reviews_dataset;
            CREATE TABLE olist_order_reviews_dataset (
                review_id varchar(32),
                order_id varchar(32),
                review_score int4,
                review_comment_title varchar(32),
                review_comment_message varchar(512),
                review_creation_date TIMESTAMP DEFAULT NOW(),
                review_answer_timestamp TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY(review_id, order_id),
                FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id)
            );"""
            cursor.execute(create_table_query)
            print('Created successfully.')
            
        except Exception as e:
            print(f'Error occured: {e}')
            
        finally: 
            cursor.close()
            
except Error as e:
    print(f'Error occured: {e}')

