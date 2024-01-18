from .minio_io_manager import MinIOIOManager
from .mysql_io_manager import MySQLIOManager
from .psql_io_manager import PostgreSQLIOManager


MYSQL_CONFIG = {
    "host": "host.docker.internal",
    "port": 3307,
    "database": "fde",
    "user": "root",
    "password": "root",
    }

MINIO_CONFIG = {
    "endpoint_url": "host.docker.internal:9000",
    "bucket": "warehouse",
    "aws_access_key_id": "minio",
    "aws_secret_access_key": "minio123",
    }

PSQL_CONFIG = {
    "host": "host.docker.internal",
    "port": 8080,
    "database": "fde",
    "user": "postgres",
    "password": "root",
    }

my_resources = {
    "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
    "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
    "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
}
