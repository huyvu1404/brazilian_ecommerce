import os
from .minio_io_manager import MinIOIOManager
from .mysql_io_manager import MySQLIOManager
from .psql_io_manager import PostgreSQLIOManager
from dotenv import load_dotenv
load_dotenv()
MYSQL_CONFIG = {
    "host":  os.getenv("MYSQL_HOST"),
    "port":  os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
    }

MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_HOST"),
    "bucket": os.getenv("MINIO_BUCKET"),
    "aws_access_key_id": os.getenv("MINIO_KEY"),
    "aws_secret_access_key": os.getenv("MINIO_ACCESS_KEY"),
    }

PSQL_CONFIG = {
    "host":  os.getenv("PSQL_HOST"),
    "port":  os.getenv("PSQL_PORT"),
    "database": os.getenv("PSQL_DATABASE"),
    "user": os.getenv("PSQL_USER"),
    "password": os.getenv("PSQL_PASSWORD"),
    }

my_resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
}

print(PSQL_CONFIG)