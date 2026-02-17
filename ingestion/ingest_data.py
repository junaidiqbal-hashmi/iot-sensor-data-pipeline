
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


df = pd.read_csv("data/sensor_logs.txt", names=["reading_time", "pump_id", "sensor_type", "value"])



dtype = {
    "pump_id": "string",
    "sensor_type": "string",
    "value": "float64"
}

parse_dates = [
    "reading_time"
]

df = pd.read_csv("data/sensor_logs.txt", names=["reading_time", "pump_id", "sensor_type", "value"],
    dtype=dtype,
    parse_dates=parse_dates
)




engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')


print(pd.io.sql.get_schema(df, name='sensor_data', con=engine))



df.head(0).to_sql(name='sensor_data', con=engine, if_exists='replace')


def run():
    pg_user = 'root'
    pg_pass = 'root'
    pg_host = 'localhost'
    pg_port = 5432
    pg_db = 'sensor'
    chunksize = 100000
    target_table = 'sensor_data'


    df_iter = pd.read_csv(
        "data/sensor_logs.txt",
        names=["reading_time", "pump_id", "sensor_type", "value"],
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )
    first = True
    for df_chunk in tqdm(df_iter):
        if first:
            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists='replace'
            )
            first = False

        df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists='append'
        )



