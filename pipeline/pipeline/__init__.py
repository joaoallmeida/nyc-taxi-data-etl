from dagster import Definitions, load_assets_from_package_module, define_asset_job

from . import assets
from .assets.raw_ingestion_checks import *
from .resources import DBT_RESOURCE
from .resources.duckUtils import DuckDBUtils

params = [
    {
        "asset":"raw_green_taxi_trip_records",
        "table":"raw_green_taxi_trip_records",
        "notNullCol": "vendorid",
        "numCols": 20,
        "expectedRows": 1100000
    },
    {
        "asset":"raw_yellow_taxi_trip_records",
        "table":"raw_yellow_taxi_trip_records",
        "notNullCol": "vendorid",
        "numCols": 19,
        "expectedRows": 60000000
    },
    {
        "asset":"raw_taxi_zones",
        "table":"raw_taxi_zones",
        "notNullCol": "locationid",
        "numCols": 4,
        "expectedRows": 265
    }
]


assets_check = []
for param in params:
    assets_check.append(check_non_nulls(param)) 
    assets_check.append(check_non_empty(param))
    assets_check.append(check_num_cols(param))
    assets_check.append(check_volume_data(param))


all_assets = load_assets_from_package_module(assets)

assets_job = define_asset_job(
    name="etl_job",
    description="TLC Trip Record Data Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts",
    selection=all_assets,
    config={
        "ops": {
            "raw_green_taxi_trip_records":{
                "config": {
                    "yearFrom": 2023,
                    "yearTo": 2024
                }
            },
            "raw_yellow_taxi_trip_records":{
                "config": {
                    "yearFrom": 2023,
                    "yearTo": 2024
                }
            }
        }
    }
)

defs = Definitions(
    assets=all_assets,
    asset_checks=assets_check,
    jobs=[assets_job],
    resources={
        "dbt": DBT_RESOURCE,
        "duckdb": DuckDBUtils()
    }
)
