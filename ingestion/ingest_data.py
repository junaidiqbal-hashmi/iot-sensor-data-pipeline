
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
@click.option('--input-file', required=True, help='Path to input sensor log file')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, chunksize, target_table, input_file):
    
    print("Starting Bronze ingestion...")
    print(f"Source file: {input_file}")

    engine = create_engine(
        f'postgresql+psycopg://{pg_user}:{pg_pass}@'
        f'{pg_host}:{pg_port}/{pg_db}'
    )

    total_rows = 0

    try:
        df_iter = pd.read_csv(
            input_file,
            names=["reading_time", "pump_id",
                   "sensor_type", "value"],
            dtype=dtype,
            parse_dates=parse_dates,
            iterator=True,
            chunksize=chunksize,
        )

        for df_chunk in tqdm(df_iter, desc="Loading chunks"):

            df_chunk.to_sql(
                name=target_table,
                con=engine,
                if_exists='append',
                index=False,
            )

            total_rows += len(df_chunk)

        print(f"Successfully loaded {total_rows:,} rows.")

    finally:
        engine.dispose()
        print("Database connection closed.")


if __name__ == '__main__':
    run()