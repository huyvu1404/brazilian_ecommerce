import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import pandas as pd
import os

mysql_config = {
    "host":os.getenv('MYSQL_HOST'),
    "port": os.getenv('MYSQL_PORT'),
    "database": os.getenv('MYSQL_DATABASE'),
    "user": os.getenv('MYSQL_USER'),
    "password": os.getenv('MYSQL_PASSWORD') 
}

names = ['brazil_state_name',
         'product_category_name_translation',
         'olist_sellers_dataset',
         'olist_products_dataset',
         'olist_customers_dataset',
         'olist_orders_dataset',
         'olist_geolocation_dataset',
         'olist_order_reviews_dataset',
         'olist_order_payments_dataset',
         'olist_order_items_dataset']

data_path = './data/'
try:
    engine = create_engine(f"mysql+mysqlconnector://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
    with engine.connect() as conn:
        for name in names:
            csv_file_name =  f"{name}.csv"
            table_name = name
            file_path = os.path.join(data_path, csv_file_name)
            print(file_path)
            
            df_iters = pd.read_csv(file_path, iterator=True, chunksize=100000)
            
            first_chunk = next(df_iters)
            first_chunk.to_sql(
                    table_name, 
                    con=conn, 
                    if_exists='replace', 
                    index=False
                )
            print(f"Create table {table_name} and load the first batch of {csv_file_name} sucessfully!")
            
            for df in df_iters:
                df.to_sql(
                    table_name, 
                    con=conn, 
                    if_exists='append', 
                    index=False
                )
            print(f"Entire {csv_file_name} loaded sucessfully!")
    
except Error as e:
    print("Error occured:",e)
                