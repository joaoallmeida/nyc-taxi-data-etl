from dagster import AssetExecutionContext, RetryPolicy, Backoff
from dagster import asset
from dagster_dbt import dbt_assets, DbtCliResource

from pipeline.duckUtilsResource import DuckDB
from pipeline.resources import Ingestion
from pipeline.resources import dbtManifest, CustomDagsterDbtTranslator

import time
import os

retryProlicy = RetryPolicy( max_retries=5, delay=5, backoff=Backoff.EXPONENTIAL )

@asset(compute_kind='duckdb', group_name='Bronze', retry_policy=retryProlicy)
def raw_green_taxi_trip_records(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    source = "green-taxi-trip-records"
    yearRange = {"from": 2023, "to": 2024}
    tbName = "raw_" + source.replace('-','_')
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{source}/data.parquet'

    context.log.info(f'Creating table: {tbName}')

    duckConn = duckdb.duckConn()
    metadata = Ingestion(duckConn, duckdb).get_raw_data(source=source, tbName=tbName, yearRange=yearRange)

    context.log.info(f'Upload data to datalake: {minioPath} ')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio( schema="bronze" ,table=tbName, minioPath=minioPath))
    context.log.info(f'Data upload has completed.')

    context.add_output_metadata( metadata={
             "Estimated Size": f"{metadata['estimated_size'][0]:,.0f}"
            ,"Schema": metadata['schema_name'][0]
            ,"Table": metadata['table_name'][0]
            ,"Columns": int(metadata['column_count'][0])
            ,"Execution Time": (time.time()-startTime)
    } )


@asset(compute_kind='duckdb', group_name='Bronze', retry_policy=retryProlicy)
def raw_yellow_taxi_trip_records(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    source = "yellow-taxi-trip-records"
    yearRange = {"from": 2023, "to": 2024}
    tbName = "raw_" + source.replace('-','_')
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{source}/data.parquet'

    context.log.info(f'Creating table: {tbName}')

    duckConn = duckdb.duckConn()
    metadata = Ingestion(duckConn, duckdb).get_raw_data(source=source, tbName=tbName, yearRange=yearRange)

    context.log.info(f'Send data to datalake: {minioPath} ')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio(schema="bronze" ,table=tbName, minioPath=minioPath))
    context.log.info(f'Data upload has completed.')

    context.add_output_metadata( metadata={
             "Estimated Size": f"{metadata['estimated_size'][0]:,.0f}"
            ,"Schema": metadata['schema_name'][0]
            ,"Table": metadata['table_name'][0]
            ,"Columns": int(metadata['column_count'][0])
            ,"Execution Time": (time.time()-startTime)
    } )


@asset(compute_kind='duckdb', group_name='Bronze', retry_policy=retryProlicy)
def raw_fhv_trip_records(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    source = "for-hire-vehicle-trip-records"
    yearRange = {"from": 2023, "to": 2024}
    tbName = "raw_fhv_trip_records"
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{source}/data.parquet'

    context.log.info(f'Creating table: {tbName}')

    duckConn = duckdb.duckConn()
    metadata = Ingestion(duckConn, duckdb).get_raw_data(source=source, tbName=tbName, yearRange=yearRange)

    context.log.info(f'Send data to datalake: {minioPath} ')
    duckdb.executeQuery(duckConn, duckdb.copy_to_minio(schema="bronze" ,table=tbName, minioPath=minioPath))
    context.log.info(f'Data upload has completed.')

    context.add_output_metadata( metadata={
             "Estimated Size": f"{metadata['estimated_size'][0]:,.0f}"
            ,"Schema": metadata['schema_name'][0]
            ,"Table": metadata['table_name'][0]
            ,"Columns": int(metadata['column_count'][0])
            ,"Execution Time": (time.time()-startTime)
    } )


@dbt_assets(manifest=dbtManifest, dagster_dbt_translator=CustomDagsterDbtTranslator() )
def refined_trips_data(context: AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()
