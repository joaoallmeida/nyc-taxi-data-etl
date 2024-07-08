from dagster import AssetExecutionContext, RetryPolicy, Backoff
from dagster import asset
from dagster_dbt import dbt_assets, DbtCliResource

from pipeline.duckUtilsResource import DuckDB, SQL
from pipeline.resources import Ingestion
from pipeline.resources import dbtManifest, CustomDagsterDbtTranslator

import time
import os

retryProlicy = RetryPolicy( max_retries=5, delay=5, backoff=Backoff.EXPONENTIAL )

@asset(compute_kind='duckdb', group_name='Bronze', retry_policy=retryProlicy)
def raw_payments(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    tbName = "raw_payments"
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{tbName}/data.parquet'

    context.log.info(f'Creating table: {tbName}')

    duckConn = duckdb.duckConn()

    duckdb.executeQuery(duckConn, SQL("""CREATE TABLE IF NOT EXISTS bronze.raw_payments AS
                                        SELECT *
                                        FROM (
                                            SELECT 1 AS ID, 'CREDIT CARD' as payment
                                            UNION
                                            SELECT 2 AS ID, 'CASH' as payment
                                            UNION
                                            SELECT 3 AS ID, 'NO CHARGE' as payment
                                            UNION
                                            SELECT 4 AS ID, 'DISPUTE' as payment
                                            UNION
                                            SELECT 5 AS ID, 'UNKNOWN' as payment
                                            UNION
                                            SELECT 6 AS ID, 'VOIDED TRIP' as payment
                                        ) """)
                        )

    metadata = duckdb.executeQuery(duckConn, duckdb.get_metadata(table=tbName))

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
def raw_rates(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    tbName = "raw_rates"
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{tbName}/data.parquet'

    context.log.info(f'Creating table: {tbName}')

    duckConn = duckdb.duckConn()

    duckdb.executeQuery(duckConn, SQL("""CREATE TABLE IF NOT EXISTS bronze.raw_rates AS
                                        SELECT *
                                        FROM (
                                            SELECT 1 AS ID, 'STANDARD RATE' AS rate
                                            UNION
                                            SELECT 2 AS ID, 'JFK' AS rate
                                            UNION
                                            SELECT 3 AS ID, 'NEWARK' AS rate
                                            UNION
                                            SELECT 4 AS ID, 'NASSAU OR WESTCHESTER' AS rate
                                            UNION
                                            SELECT 5 AS ID, 'NEGOTIATED FARE' AS rate
                                            UNION
                                            SELECT 6 AS ID, 'GROUP RIDE' AS rate
                                        ) """)
                        )

    metadata = duckdb.executeQuery(duckConn, duckdb.get_metadata(table=tbName))

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
def raw_trips(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    tbName = "raw_trips"
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{tbName}/data.parquet'

    context.log.info(f'Creating table: {tbName}')

    duckConn = duckdb.duckConn()

    duckdb.executeQuery(duckConn, SQL("""CREATE TABLE IF NOT EXISTS bronze.raw_trips AS
                                        SELECT *
                                        FROM (
                                            SELECT 1 AS ID, 'STREET-HAIL' AS trip
                                            UNION
                                            SELECT 2 AS ID, 'DISPATCH' AS trip
                                        )""")
                        )

    metadata = duckdb.executeQuery(duckConn, duckdb.get_metadata(table=tbName))

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
def raw_vendors(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    tbName = "raw_vendors"
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{tbName}/data.parquet'

    context.log.info(f'Creating table: {tbName}')

    duckConn = duckdb.duckConn()

    duckdb.executeQuery(duckConn, SQL("""CREATE TABLE IF NOT EXISTS bronze.raw_vendors AS
                                        SELECT *
                                        FROM (
                                            SELECT 1 AS ID, 'CREATIVE MOBILE TECHNOLOGIES' AS vendor
                                            UNION
                                            SELECT 2 AS ID, 'VERIFONE INC' AS vendor
                                        )""")
                        )

    metadata = duckdb.executeQuery(duckConn, duckdb.get_metadata(table=tbName))

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
def raw_green_taxi_trip_records(context: AssetExecutionContext, duckdb: DuckDB ):

    startTime = time.time()
    source = "green-taxi-trip-records"
    yearRange = {"from": 2023, "to": 2024}
    tbName = "raw_" + source.replace('-','_')
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{tbName}/data.parquet'

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
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{tbName}/data.parquet'

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
    minioPath = f'{os.environ['MINIO_PATH_BRONZE']}/{tbName}/data.parquet'

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
