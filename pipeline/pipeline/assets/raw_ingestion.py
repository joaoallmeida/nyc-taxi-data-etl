from dagster import AssetExecutionContext, Config, Int, RetryPolicy, Backoff, asset, MetadataValue
from dagster_dbt import dbt_assets, DbtCliResource, DagsterDbtTranslatorSettings

from pipeline.resources.duckUtils import DuckDBUtils
from pipeline.resources import IngestionResource, DBT_MANIFEST, CustomDagsterDbtTranslator

import time
import os

retryProlicy = RetryPolicy( max_retries=5, delay=15, backoff=Backoff.EXPONENTIAL )

class AssetConfigParameter(Config):
    yearFrom: Int
    yearTo: Int


@asset(compute_kind='duckdb', group_name='Bronze', retry_policy=retryProlicy)
def raw_green_taxi_trip_records(context: AssetExecutionContext, duckdb: DuckDBUtils, config: AssetConfigParameter):

    startTime = time.time()
    source = "green-taxi-trip-records"

    context.log.info(f'Creating table: Green Trips')

    duckConn = duckdb.duckConn()
    metadata = IngestionResource(duckConn, duckdb).get_raw_parquet_data(source=source, yearRange=config)

    context.log.info(f'Upload data to datalake')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio( schema="bronze" ,table=metadata['table_name'][0]))
    context.log.info(f'Data upload has completed.')

    dataView = duckdb.executeQuery(duckConn, duckdb.select_table( schema="bronze" ,table=metadata['table_name'][0]))

    context.add_output_metadata( metadata={
             "Estimated Size": f"{metadata['estimated_size'][0]:,.0f}"
            ,"Schema": metadata['schema_name'][0]
            ,"Table": metadata['table_name'][0]
            ,"Columns": int(metadata['column_count'][0])
            ,"Execution Time": (time.time()-startTime)
            ,"Preview": MetadataValue.md(dataView.to_markdown())
    } )

    context.log.info(f'Table creation has completed')
    duckConn.close()

@asset(compute_kind='duckdb', group_name='Bronze', retry_policy=retryProlicy)
def raw_yellow_taxi_trip_records(context: AssetExecutionContext, duckdb: DuckDBUtils, config: AssetConfigParameter ):

    startTime = time.time()
    sourceName = "yellow-taxi-trip-records"

    context.log.info(f'Creating table: Yellow Trips')

    duckConn = duckdb.duckConn()
    metadata = IngestionResource(duckConn, duckdb).get_raw_parquet_data(source=sourceName, yearRange=config)

    context.log.info(f'Send data to datalake')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio(schema="bronze" ,table=metadata["table_name"][0]))
    context.log.info(f'Data upload has completed.')

    dataView = duckdb.executeQuery(duckConn, duckdb.select_table( schema="bronze" ,table=metadata["table_name"][0]))

    context.add_output_metadata( metadata={
             "Estimated Size": f"{metadata['estimated_size'][0]:,.0f}"
            ,"Schema": metadata['schema_name'][0]
            ,"Table": metadata['table_name'][0]
            ,"Columns": int(metadata['column_count'][0])
            ,"Execution Time": (time.time()-startTime)
            ,"Preview": MetadataValue.md(dataView.to_markdown())
    } )

    context.log.info(f'Table creation has completed')
    duckConn.close()


@asset(compute_kind='duckdb', group_name='Bronze', retry_policy=retryProlicy)
def raw_taxi_zones(context: AssetExecutionContext, duckdb: DuckDBUtils ):

    startTime = time.time()
    sourceName = 'taxi-zones'

    context.log.info(f'Creating table: Taxi Zones')

    duckConn = duckdb.duckConn()
    metadata = IngestionResource(duckConn, duckdb).get_raw_csv_data(source=sourceName)

    context.log.info(f'Send data to datalake')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio(schema="bronze" ,table=metadata['table_name'][0]))
    context.log.info(f'Data upload has completed.')

    dataView = duckdb.executeQuery(duckConn, duckdb.select_table( schema="bronze" ,table=metadata['table_name'][0]))

    context.add_output_metadata( metadata={
             "Estimated Size": f"{metadata['estimated_size'][0]:,.0f}"
            ,"Schema": metadata['schema_name'][0]
            ,"Table": metadata['table_name'][0]
            ,"Columns": int(metadata['column_count'][0])
            ,"Execution Time": (time.time()-startTime)
            ,"Preview": MetadataValue.md(dataView.to_markdown())
    } )

    context.log.info(f'Table creation has completed')
    duckConn.close()


@dbt_assets(manifest=DBT_MANIFEST, dagster_dbt_translator=CustomDagsterDbtTranslator(settings=DagsterDbtTranslatorSettings(enable_asset_checks=True)) )
def refined_trips_data(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()



