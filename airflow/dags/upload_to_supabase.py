from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus


def upload_gold_to_supabase():

    # Local PostgreSQL
    local_hook = PostgresHook(postgres_conn_id="postgres_default")

    query = """
    SELECT *
    FROM gold_pump_health
    """

    df = local_hook.get_pandas_df(query)

    # Supabase connection
    supabase_hook = PostgresHook(postgres_conn_id="supabase_postgres")

    conn = supabase_hook.get_connection("supabase_postgres")

    password = quote_plus(conn.password)

    engine = create_engine(
    f"postgresql+psycopg2://"
    f"{conn.login}:{password}"
    f"@{conn.host}:{conn.port}/{conn.schema}"
    )

    df.to_sql(
        "gold_pump_health",
        engine,
        if_exists="replace",
        index=False
    )

    print(
        f"Uploaded {len(df)} rows to Supabase"
    )