from etl import extract
from config.config import get_env_config, get_etl_config
from threading import Thread


def main():

    logger = get_env_config('tlc-pipeline')

    etl_config = get_etl_config()
    extr = extract.Extract('tlc-data-raw')

    processes = [Thread(target=extr.get_raw_data, args=(source['source_name'], source['source_years'], )) for source in etl_config['data_source']]
    [p.start() for p in processes ]
    result = [p.join() for p in processes]

if __name__=='__main__':
    main()
