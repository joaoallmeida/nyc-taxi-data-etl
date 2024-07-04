from bs4 import BeautifulSoup
from dagster import ConfigurableIOManager
from tlc_pipeline.duckUtils import DuckDB
import requests


class IngestionIOManager(ConfigurableIOManager):
    bucket: str

    @property
    def duckUtils(self) -> DuckDB:
        return DuckDB()

    def __get_url_source_data__(self, source:str) -> list[dict]:
        try:
            response = requests.get('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')
            soup =  BeautifulSoup(response.text, 'html.parser')
            records = [link.get('href').strip() for table in soup.find_all('table') for link in table.find_all('a') if link.get('title').replace(' ','-').lower() == source]
        except Exception as e:
            raise e
        return records

    def handle_output(self, context, table:str):
        minioPath = f's3://{self.bucket}/dagster/raw/{table}/data.parquet'
        self.duckUtils.executeQuery( self.duckUtils.copy_to_minio(table, minioPath) )

    def load_input(self, context, table:str) -> str:
        return "SELECT * FROM read_parquet('$1')".format(self.__get_url_source_data__(table))
