from dagster import Definitions
from dagster import define_asset_job, load_assets_from_modules, file_relative_path

from tlc_pipeline import assets, resources
from tlc_pipeline.duckUtilsResource import DuckDB

all_assets = load_assets_from_modules([assets])

assets_job = define_asset_job(
    name="etl_job",
    description="TLC Trip Record Data Yellow and green taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts",
    selection=all_assets,
    config={
        "execution": {
            "config": {
                "multiprocess": {
                    "max_concurrent": 5,
                },
            }
        }
    }
)

defs = Definitions(
    assets=all_assets,
    jobs=[assets_job],
    resources={
        "dbt": resources.dbtResource,
        "duckdb": DuckDB()
    }
)
