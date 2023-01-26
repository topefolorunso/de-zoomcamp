import argparse
import pandas as pd
from sqlalchemy import create_engine
from time import time


def main(params):
    file_name = params.file_name
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000, low_memory=False)

    t_start = time()
    df = next(df_iter)
    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.to_sql(name=table_name, con=engine, if_exists='replace') 
    t_end = time()
    print('inserted first chunk..., took %.3f seconds' % (t_end - t_start))

    t_start = time()
    try:
        while True:
            loop_start = time()
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')
            loop_end = time()
            print('inserted another chunk..., took %.3f seconds' % (loop_end - loop_start))

    except StopIteration:
        t_end = time()
        print('completed data ingestion..., took %.3f minutes' % ((t_end - t_start)/60))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--file_name', help='name of file we want to ingest')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of table where we will write data to')

    args = parser.parse_args()

    main(args)