import mysql.connector
from mysql.connector import Error
from sqlalchemy import create_engine
import pandas as pd
import os

mysql_config = {
    "host": "host.docker.internal",
    "port": 3307,
    "database": "fde",
    "user": "root",
    "password": "root"  
}

names = [          'product_category_name_translation',
                   'olist_sellers_dataset',
                   'olist_products_dataset',
                   'olist_customers_dataset',
                   'olist_orders_dataset',
                   'olist_geolocation_dataset',
                   'olist_order_reviews_dataset',
                   'olist_order_payments_dataset',
                   'olist_order_items_dataset',
                   ]
path = './data/'
try:
    with  mysql.connector.connect(**mysql_config) as connection:
    
        try:
            cursor = connection.cursor()
            cursor.execute("""SET GLOBAL max_allowed_packet=1073741824;""")
        
        except Exception as e:
            print("Error occured:",e)
            
        finally:
            cursor.close()
except Error as e:
    print("Error occured:",e)
try:
    engine = create_engine(f"mysql+mysqlconnector://{mysql_config['user']}:{mysql_config['password']}@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}")
    with engine.connect() as conn:

            for name in names:
                csv_file_name =  f"{name}.csv"
                table_name = name
                csv_file_path = os.path.join(path, csv_file_name)
                print(csv_file_path)
                df = pd.read_csv(csv_file_path)
                df.to_sql(
                    table_name, 
                    con=conn, 
                    if_exists='append', 
                    index=False
                )
            print("Data loaded sucessfully!")
    
except Error as e:
    print("Error occured:",e)
                