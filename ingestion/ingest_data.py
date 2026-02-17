
import click
import pandas as pd
from sqlalchemy import create_engine
from tqdm.auto import tqdm


dtype = {
    "pump_id": "string",
    "sensor_type": "string",
    "value": "float64"
}

parse_dates = [
    "reading_time"
]

# engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default=5432, type=int, help='PostgreSQL port')
@click.option('--pg-db', default='sensor', help='PostgreSQL database name')
@click.option('--target-table', default='sensor_data', help='Target table name')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for reading CSV')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table):
    
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')
    df_iter = pd.read_csv(
        "/workspaces/iot-sensor-data-pipeline/data/sensor_logs.txt",
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
if __name__ == '__main__':
    run()


