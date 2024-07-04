from typing import List
from dagster_dbt import DbtCliResource, DagsterDbtTranslator
from dagster import file_relative_path, AssetKey
from dagster import AssetExecutionContext
from pathlib import Path
from bs4 import BeautifulSoup
import requests


# DBT Resource
dbtResource = DbtCliResource(
    project_dir=file_relative_path(__file__,'../dbt_transformation')
)

dbtManifest =(dbtResource.cli(["parse"], target_path=Path('target'), manifest={})
              .wait()
              .target_path.joinpath('manifest.json'))


class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    @classmethod
    def get_asset_key(cls, dbt_resource_props) -> AssetKey:
        return AssetKey(dbt_resource_props["name"])

    def get_group_name(cls, dbt_resource_props) -> str:
        return "refined"

# Ingestion Resource
class Ingestion:

    def __get_url_source_data__(self, source_name:str) -> list[dict]:
        try:
            response = requests.get('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page')
            soup =  BeautifulSoup(response.text, 'html.parser')
            records = [link.get('href').strip() for table in soup.find_all('table') for link in table.find_all('a') if link.get('title').replace(' ','-').lower() == source_name]
        except Exception as e:
            raise e
        return records

    def get_raw_data(self, source:str, year_range:dict) -> List:
        listUrl = self.__get_url_source_data__(source)
        parquets = list()
        
        for url in listUrl:
            year = int(url.split('_')[-1].split('-')[0])
            # fileName = url.split('/')[-1].replace('-','_')
            # tbName = fileName.split('.')[0]
            if year in [year for year in range(year_range["from"], year_range["to"] + 1)]:
                parquets.append(url)

        return parquets
