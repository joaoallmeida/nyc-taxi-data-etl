from dagster import Definitions
from dagster import define_asset_job, load_assets_from_modules, file_relative_path

from tlc_pipeline import assets, resources
from tlc_pipeline.duckUtils import DuckDB

all_assets = load_assets_from_modules([assets])

assets_job = define_asset_job(
    name="etl_job",
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
