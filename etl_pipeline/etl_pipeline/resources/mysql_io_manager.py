import pandas as pd
from mysql.connector import connect

class MySQLIOManager:
    def __init__(self, config):
        # config for connecting to MySQL database
        self._config = config
    def extract_data(self, sql: str) -> pd.DataFrame:
        try:
            with connect(**self._config) as connection:
                try:
                    cursor = connection.cursor()
                    
                    cursor.execute(sql)
            
                    rows = cursor.fetchall()
            
                    columns = [i[0] for i in cursor.description]
            
                    df = pd.DataFrame(rows, columns = columns)
            
                    return df
                finally:
                    cursor.close()
                    
        except Exception as e:
            print("Error occured:",e)
            return pd.DataFrame()