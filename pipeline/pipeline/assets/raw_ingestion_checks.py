from dagster import AssetCheckResult
from dagster import asset_check
from typing import Dict

from pipeline.resources.duckUtils import SQL


def check_non_nulls(params: Dict):
    @asset_check(name='check_non_nulls', asset=params['asset'], required_resource_keys={'duckdb'})
    def _run(context):
        conn = context.resources.duckdb.duckConn(readOnly=True)
        count = context.resources.duckdb.executeQuery(conn, SQL('SELECT COUNT(*) FROM bronze.$table WHERE $col IS NULL', col=params['notNullCol'], table=params['table']))['count_star()'][0]
        return AssetCheckResult(passed=int(count) == 0, metadata={"Count Rows": int(count)})
    return _run


def check_non_empty(params: Dict):
    @asset_check(name='check_non_empty', asset=params['asset'], required_resource_keys={'duckdb'})
    def _run(context):
        conn = context.resources.duckdb.duckConn(readOnly=True)
        count = context.resources.duckdb.executeQuery(conn,SQL('SELECT COUNT(*) FROM bronze.$table', table=params['table']))['count_star()'][0]
        return AssetCheckResult(passed=int(count) > 0, metadata={"Count Rows": int(count)})
    return _run


def check_num_cols(params: Dict):
    @asset_check(name='check_num_cols', asset=params['asset'], required_resource_keys={'duckdb'})
    def _run(context):
        conn = context.resources.duckdb.duckConn(readOnly=True)
        count = context.resources.duckdb.executeQuery( conn,SQL("SELECT column_count FROM duckdb_tables() WHERE table_name = '$table'", table=params['table']))['column_count'][0]
        return AssetCheckResult(passed=int(count) == params['numCols'], metadata={"Count Rows": int(count)})
    return _run


def check_volume_data(params: Dict):
    @asset_check(name='check_volume_data', asset=params['asset'], required_resource_keys={'duckdb'})
    def _run(context):
        conn = context.resources.duckdb.duckConn(readOnly=True)
        count = context.resources.duckdb.executeQuery( conn,SQL("""SELECT CASE
                                                                            WHEN COUNT(*) BETWEEN 1 AND $expectedRows THEN COUNT(*)
                                                                            ELSE 0
                                                                          END as rows
                                                                FROM bronze.$table""", expectedRows=params['expectedRows'], table=params['table']))['rows'][0]
        return AssetCheckResult(passed=int(count) > 0, metadata={"Count Rows": int(count)})
    return _run

