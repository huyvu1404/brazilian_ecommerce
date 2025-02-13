import os
from dotenv import load_dotenv
from .minio_io_manager import MinIOIOManager
from .mysql_io_manager import MySQLIOManager
from .psql_io_manager import PostgreSQLIOManager

load_dotenv()

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port":  os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    }

MINIO_CONFIG = {
    "endpoint_url": "host.docker.internal:9000",
    "bucket": os.getenv("MINIO_BUCKET"),
    "aws_access_key_id": os.getenv("MINIO_ACCESS_KEY"),
    "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY"),
    }

POSTGRES_CONFIG = {
    "host":  os.getenv("POSTGRES_HOST"),
    "port":  os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    }

my_resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(POSTGRES_CONFIG),
}
