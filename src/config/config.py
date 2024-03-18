import logging

def get_env_config(name:str, env:str='dev') -> logging.Logger:
    from dotenv import load_dotenv

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] >> %(message)s')
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if env == 'dev':
        load_dotenv()

    return logger


def get_etl_config():

    import polars as pl

    config = {
        "data_source": [
            {"source_name":"yellow-taxi-trip-records", "source_years": {"from": 2021, "to": 2023}  },
            {"source_name":"green-taxi-trip-records", "source_years": {"from": 2021, "to": 2023} },
            {"source_name":"for-hire-vehicle-trip-records", "source_years": {"from": 2021, "to": 2023} }
        ],
        "data_transform": {
            "yellow-taxi-trip-records" : {
                'cast_columns': {'RatecodeID':pl.Int64,'passenger_count':pl.Int64}
            },
            "green-taxi-trip-records": {
                'cast_columns': {'RatecodeID':pl.Int64,'passenger_count':pl.Int64,'ehail_fee':pl.Int64,'payment_type':pl.Int64 ,'trip_type': pl.Int64}
            },
            "for-hire-vehicle-trip-records" : {
                'cast_columns': {'DOlocationID':pl.Int64,'PUlocationID':pl.Int64}
            }
        }
    }

    return config
