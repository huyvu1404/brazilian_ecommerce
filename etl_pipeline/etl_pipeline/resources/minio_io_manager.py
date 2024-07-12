import pandas as pd
import os
from dagster import IOManager, OutputContext, InputContext
from minio import Minio
from minio.error import S3Error

class MinIOIOManager(IOManager):
    
    def __init__(self, config):
        self._config = config
        self._client = Minio(
            self._config["endpoint_url"],
            access_key = self._config["aws_access_key_id"],
            secret_key = self._config["aws_secret_access_key"],
            secure=False
        )
        
    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        
        bucket_name = self._config["bucket"]
        file_name = context.asset_key.path[-1]
        
        object_name = f"{file_name}.csv"
        data_directory = os.path.join(os.getcwd(), 'data')

        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        
        local_path = os.path.join(
                data_directory, 
                object_name
        )
        context.log.info(f'object_name: {context.asset_key}')        
        try:

            obj.to_csv(local_path, index=False)
            
            self._client.fput_object(
                bucket_name, 
                object_name, 
                local_path
            )
            
        except S3Error as e:
            context.log.error(f"Error uploading to MinIO: {e}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        
        bucket_name = self._config['bucket']
        file_name = context.asset_key.path[-1]

        object_name = f"{file_name}.csv"
        data_directory = os.path.join(os.getcwd(), 'data')

        if not os.path.exists(data_directory):
            os.makedirs(data_directory)
        object_name = f"{file_name}.csv"
        
        local_path = os.path.join(
                data_directory, 
                object_name
        ) 
        try:
           
            self._client.fget_object(
                bucket_name, 
                object_name, 
                local_path)
            
            return pd.read_csv(local_path)
        
        except S3Error as e:
            context.log.error(f"Error downloading from MinIO: {e}")
            return pd.DataFrame()