import psycopg2
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

class PostgreSQLIOManager(IOManager):
    def __init__(self, config):
        self._config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        try:
            engine = create_engine(f'postgresql://{self._config["user"]}:{self._config["password"]}@{self._config["host"]}:{self._config["port"]}/{self._config["database"]}')
            with engine.connect() as conn:
                
                obj.to_sql(
                    name = context.asset_key.path[-1], 
                    con = conn, 
                    index = False,
                    if_exists = 'replace'
                )
            
        except Exception as e:
            print("Error occured:",e)
            
    def load_input(self, context: InputContext) -> pd.DataFrame:
        try:
            with psycopg2.connect(**self._config) as connection:
                try:
                    cursor = connection.cursor()
            
                    query = f'SELECT * FROM {context.asset_key.path[-1]}'
                    cursor.execute(query)
            
                    rows = cursor.fetchall()
            
                    # get columns name
                    columns = [i[0] for i in cursor.description]
            
                    # create pandas dataframe
                    df = pd.DataFrame(rows, columns = columns)
            
                    return df
                finally:
                    cursor.close()
        except Exception as e:
            print("Error occurred:", e)
        
        
