from string import Template
from dagster import ConfigurableResource
from sqlescapy import sqlescape
import duckdb
import os

class SQL:
    def __init__(self, query:str , **options):
        self.sql = query
        self.options = options

class DuckDB(ConfigurableResource):

    def duckConn(self) -> duckdb.DuckDBPyConnection:
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
        return conn


    def executeQuery(self, duckConn: duckdb.DuckDBPyConnection , query:SQL):
        result = duckConn.query(self.sql_to_string(query))
        if result is None:
            return
        return result.fetchall()

    def count_data(self, table:str) -> SQL:
        return SQL(f"SELECT COUNT(*) FROM $table", table=table)

    def copy_to_minio(self, table:str, minioPath:str ) -> SQL:
        return SQL(f"COPY $table TO '$minioPath' (FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd', ROW_GROUP_SIZE 1000000)", table=table, minioPath=minioPath)

    def create_table(self, table:str, downloadUrl:list) -> SQL:
        return SQL(f"CREATE TABLE IF NOT EXISTS $table AS SELECT * FROM read_parquet($downloadUrl)", table=table, downloadUrl=downloadUrl)

    def drop_table(self, table:str) -> SQL:
        return f"DROP TABLE IF EXISTS {table}"

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
