from string import Template
from dagster import ConfigurableResource
from sqlescapy import sqlescape
from duckdb import DuckDBPyConnection

import duckdb
import os

class SQL:
    def __init__(self, query:str , **options):
        self.sql = query
        self.options = options

class DuckDBUtils(ConfigurableResource):
    """
        A personal resource used to help in duckdb sql statements executions.
    """

    def duckConn(self) -> DuckDBPyConnection:
        # conn = duckdb.connect(":memory:")
        conn = duckdb.connect("/tmp/duckdb.db")
        conn.install_extension('httpfs')
        conn.load_extension('httpfs')

        conn.query(f"""
            SET http_keep_alive=false;
            SET http_retries=5;
            SET http_retry_wait_ms=500;
            SET s3_url_style='path';
            SET s3_access_key_id='{os.environ['ACCESS_KEY']}';
            SET s3_secret_access_key='{os.environ['SECRET_ACCESS']}';
            SET s3_endpoint='{os.environ['ENDPOINT_MINIO']}';
            SET preserve_insertion_order = false;
            SET memory_limit = '10GB';
            SET threads TO 12;
        """)

        conn.query("CREATE SCHEMA IF NOT EXISTS bronze;")

        return conn

    def executeQuery(self, duckConn: DuckDBPyConnection , query:SQL):
        result = duckConn.query(self.sql_to_string(query))
        if result is None:
            return
        return result.df()

    def copy_to_minio(self, schema:str, table:str, minioPath:str ) -> SQL:
        return SQL(f"COPY $schema.$table TO '$minioPath' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', ROW_GROUP_SIZE 1000000)", schema=schema, table=table, minioPath=minioPath)

    def create_table_parquet(self, schema:str, table:str, downloadUrl:list) -> SQL:
        return SQL(f"CREATE TABLE IF NOT EXISTS $schema.$table AS SELECT * FROM read_parquet($downloadUrl)", schema=schema, table=table, downloadUrl=downloadUrl)

    def create_table_csv(self, schema:str, table:str, downloadUrl:list) -> SQL:
        return SQL(f"CREATE TABLE IF NOT EXISTS $schema.$table AS SELECT * FROM read_csv($downloadUrl, header=true)", schema=schema, table=table, downloadUrl=downloadUrl)

    def drop_table(self, schema:str, table:str) -> SQL:
        return SQL("DROP TABLE IF EXISTS $schema.$table", schema=schema, table=table,)

    def get_metadata(self, table:str) -> SQL:
        return SQL(f"SELECT * FROM duckdb_tables() WHERE table_name = '$table'", table=table)

    def sql_to_string(self, query:SQL) -> str:
        replacements = {}
        for key, value in query.options.items():
            if isinstance(value, SQL):
                replacements[key] = f"({self.sql_to_string(value)})"
            elif isinstance(value, str):
                replacements[key] = f"{sqlescape(value)}"
            elif isinstance(value, (int, float, bool)):
                replacements[key] = str(value)
            elif isinstance(value, list):
                replacements[key] = str(value)
            elif value is None:
                replacements[key] = "null"
            else:
                raise ValueError(f"Invalid type for {key}")
        return Template(query.sql).safe_substitute(replacements)
