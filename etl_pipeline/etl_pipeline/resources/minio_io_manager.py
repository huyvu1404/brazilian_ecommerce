import pandas as pd
import os
from io import BytesIO, StringIO
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
        csv_bytes = obj.to_csv(index=False).encode('utf-8')
        csv_buffer = BytesIO(csv_bytes)
        context.log.info(f'file_length: {len(csv_bytes)}')        
        try:
            self._client.put_object(
                bucket_name, 
                object_name, 
                data=csv_buffer,
                length=len(csv_bytes),
                content_type='application/csv'
            )
            
        except S3Error as e:
            context.log.error(f"Error uploading to MinIO: {e}")

    def load_input(self, context: InputContext) -> pd.DataFrame:
        
        bucket_name = self._config['bucket']
        file_name = context.asset_key.path[-1]

        object_name = f"{file_name}.csv"

        try:
           
            response = self._client.get_object(
                bucket_name, 
                object_name)
            data = response.data.decode("utf-8")
            data = StringIO(data)
            return pd.read_csv(data)
        
        except S3Error as e:
            context.log.error(f"Error downloading from MinIO: {e}")
            return pd.DataFrame()
        finally:
            response.close()
            response.release_conn()