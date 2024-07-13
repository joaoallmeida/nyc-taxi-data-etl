from dagster_dbt import DbtCliResource, DagsterDbtTranslator
from dagster import file_relative_path, AssetKey
from pathlib import Path
from bs4 import BeautifulSoup
from duckdb import DuckDBPyConnection
from .duckUtils import DuckDBUtils

import requests
import os
import pandas as pd


# DBT Resource
DBT_PROJECT_DIR = file_relative_path(__file__,"../../dbt_transformation")
DBT_RESOURCE = DbtCliResource(
    project_dir=DBT_PROJECT_DIR,
    target=os.environ['ENVIRONMENT']
)

DBT_MANIFEST =(DBT_RESOURCE.cli(["parse"], target_path=Path('target'), manifest={})
              .wait()
              .target_path.joinpath('manifest.json'))


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

    def get_group_name(cls, dbt_resource_props) -> str:
        return "Silver"


# Ingestion Resource
class IngestionResource:
    def __init__(self, duckConn: DuckDBPyConnection, duckdb: DuckDBUtils ) -> None:
        self.duckConn = duckConn
        self.duckUtils = duckdb

    def __get_url_source_data__(self, source_name:str) -> list[dict]:
        try:
            response = requests.get('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')
            soup =  BeautifulSoup(response.text, 'html.parser')

            if source_name != 'taxi-zones':
                records = [link.get('href').strip() for table in soup.find_all('table') for link in table.find_all('a') if link.get('title').replace(' ','-').lower() == source_name]
            else:
                records = [link.get('href').strip() for table in soup.find_all('ul') for link in table.find_all('a') if link.get('href').endswith('lookup.csv') ]

        except Exception as e:
            raise e
        return records

    def get_raw_csv_data(self, source:str):
        csvUrl = self.__get_url_source_data__(source)
        tbName = "raw_" + source.replace('-','_')

        self.duckUtils.executeQuery(self.duckConn, self.duckUtils.create_table_csv(schema="bronze" ,table=tbName, downloadUrl=csvUrl))
        meta = self.duckUtils.executeQuery(self.duckConn, self.duckUtils.get_metadata(table=tbName))

        return meta


    def get_raw_parquet_data(self, source:str, yearRange) -> pd.DataFrame:
        listUrl = self.__get_url_source_data__(source)
        parquetsUrl = list()
        tbName = "raw_" + source.replace('-','_')

        for url in listUrl:
            year = int(url.split('_')[-1].split('-')[0])
            if year in [year for year in range(yearRange.yearFrom, yearRange.yearTo + 1)]:
                parquetsUrl.append(url)

        self.duckUtils.executeQuery(self.duckConn, self.duckUtils.create_table_parquet(schema="bronze" ,table=tbName, downloadUrl=parquetsUrl))
        meta = self.duckUtils.executeQuery(self.duckConn, self.duckUtils.get_metadata(table=tbName))

        return meta
