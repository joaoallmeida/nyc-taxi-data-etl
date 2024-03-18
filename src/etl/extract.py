from bs4 import BeautifulSoup
from dataclasses import dataclass

import logging
import requests
import duckdb
import os

@dataclass
class Extract:
    bucket: str

    def __post_init__(self):
        self.URL_BASE = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
        self.ACCESS_KEY = os.environ['minio_access_key']
        self.SECRET_KEY = os.environ['minio_secret_key']
        self.ENDPOINT_URL = os.environ['minio_endpoint']

    def _duckdb_conn_(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        conn.install_extension('httpfs')
        conn.load_extension('httpfs')
        conn.query(f"SET http_keep_alive=false")
        conn.query(f"SET http_retries=5")
        conn.query(f"SET http_retry_wait_ms=500")
        conn.query(f"SET s3_url_style='path'")
        conn.query(f"SET s3_access_key_id='{self.ACCESS_KEY}'")
        conn.query(f"SET s3_secret_access_key='{self.SECRET_KEY}'")
        conn.query(f"SET s3_endpoint='{self.ENDPOINT_URL}'")
        return conn

    def _get_url_source_data_(self, source_name:str) -> list[dict]:
        try:
            response = requests.get(self.URL_BASE)
            soup =  BeautifulSoup(response.text, 'html.parser')
            records = [link.get('href').strip() for table in soup.find_all('table') for link in table.find_all('a') if link.get('title').replace(' ','-').lower() == source_name]
        except Exception as e:
            raise e

        return records

    def get_raw_data(self, source:str, year_range:dict) -> None:

        logging.info(f'Getting data from the source --> {source}')

        url_list = self._get_url_source_data_(source)
        conn = self._duckdb_conn_()

        try:
            for url in url_list:
                year = int(url.split('_')[-1].split('-')[0])
                file_name = url.split('/')[-1].replace('-','_')
                tb_name = file_name.split('.')[0]

                if year in [year for year in range(year_range["from"], year_range["to"] + 1)]:
                    try:
                        logging.info(f'Uploading data file >> {file_name}')

                        conn.query(f"CREATE TABLE {tb_name} AS SELECT * FROM read_parquet('{url}')")
                        conn.query(f"COPY {tb_name} TO 's3://{self.bucket}/{source}/{year}/{file_name}' ( FORMAT PARQUET, OVERWRITE_OR_IGNORE true, COMPRESSION 'zstd' )")
                        conn.query(f'DROP TABLE IF EXISTS {tb_name}')

                        logging.info(f'Upload of data file >> {file_name} << has completed.')
                        logging.info(f'Uploaded on >> s3://{self.bucket}/{source}/{year}/{file_name}')

                    except duckdb.HTTPException:
                        logging.warning(f'Failure to read parquet >> {file_name}')

        except Exception as e:
            logging.error(f'Falha ao realizar extracao dos dados --> {e}')
            raise e

        logging.info(f'Data capture from source >> {source} << has completed.')
        conn.close()