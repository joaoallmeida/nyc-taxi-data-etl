from dagster import AssetExecutionContext, RetryPolicy, Backoff
from dagster import asset
from dagster_dbt import dbt_assets, DbtCliResource

from tlc_pipeline.duckUtils import DuckDB
from tlc_pipeline.resources import Ingestion
from tlc_pipeline.resources import dbtManifest, CustomDagsterDbtTranslator

import time

retryProlicy = RetryPolicy( max_retries=5, delay=5, backoff=Backoff.EXPONENTIAL )

@asset(compute_kind='duckdb', group_name='Raw', retry_policy=retryProlicy)
def raw_green_taxi_trip_records(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    source = "green-taxi-trip-records"
    yearRange = {"from": 2023, "to": 2024}
    tbName = "raw_" + source.replace('-','_')
    minioPath = f's3://tlc/raw/{source}/data.parquet'

    context.log.info(f'Creating query from table: {tbName}')

    parquetsUrl = Ingestion().get_raw_data(source=source, year_range=yearRange)
    duckConn = duckdb.duckConn()
    duckdb.executeQuery(duckConn, duckdb.create_table(tbName, parquetsUrl))
    nrows = duckdb.executeQuery(duckConn, duckdb.count_data(tbName) )

    context.log.info(f'Upload data to datalake: {minioPath} ')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio(tbName, minioPath))
    context.log.info(f'Data upload has completed.')

    context.add_output_metadata( metadata={ "row_count": nrows[0][0], "execution_duration": (time.time() - startTime) } )

@asset(compute_kind='duckdb', group_name='Raw', retry_policy=retryProlicy)
def raw_yellow_taxi_trip_records(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    source = "yellow-taxi-trip-records"
    yearRange = {"from": 2023, "to": 2024}
    tbName = "raw_" + source.replace('-','_')
    minioPath = f's3://tlc/raw/{source}/data.parquet'

    context.log.info(f'Creating query from table: {tbName}')

    parquetsUrl = Ingestion().get_raw_data(source=source, year_range=yearRange)
    duckConn = duckdb.duckConn()
    duckdb.executeQuery(duckConn, duckdb.create_table(tbName, parquetsUrl))
    nrows = duckdb.executeQuery(duckConn, duckdb.count_data(tbName) )

    context.log.info(f'Send data to datalake: {minioPath} ')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio(tbName, minioPath))
    context.log.info(f'Data upload has completed.')

    context.add_output_metadata( metadata={ "row_count": nrows[0][0], "execution_duration": (time.time() - startTime) } )

@asset(compute_kind='duckdb', group_name='Raw', retry_policy=retryProlicy)
def raw_for_hire_vehicle_trip_records(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    source = "for-hire-vehicle-trip-records"
    yearRange = {"from": 2023, "to": 2024}
    tbName = "raw_" + source.replace('-','_')
    minioPath = f's3://tlc/raw/{source}/data.parquet'

    context.log.info(f'Creating query from table: {tbName}')

    parquetsUrl = Ingestion().get_raw_data(source=source, year_range=yearRange)
    duckConn = duckdb.duckConn()
    duckdb.executeQuery(duckConn, duckdb.create_table(tbName, parquetsUrl))
    nrows = duckdb.executeQuery(duckConn, duckdb.count_data(tbName) )

    context.log.info(f'Send data to datalake: {minioPath} ')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio(tbName, minioPath))
    context.log.info(f'Data upload has completed.')

    context.add_output_metadata( metadata={ "row_count": nrows[0][0], "execution_duration": (time.time() - startTime) } )

@dbt_assets(manifest=dbtManifest, dagster_dbt_translator=CustomDagsterDbtTranslator() )
def refined_trips_data(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
