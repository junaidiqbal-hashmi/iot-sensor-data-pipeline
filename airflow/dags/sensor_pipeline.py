from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from upload_to_supabase import upload_gold_to_supabase


with DAG(
    dag_id="sensor_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,     # Manual trigger
    catchup=False,
    tags=["iot", "sensor"],
    template_searchpath=["/opt/airflow/sql"],  # Location of SQL files
) as dag:

    silver_temperature = SQLExecuteQueryOperator(
        task_id="silver_temperature",
        conn_id="postgres_default",
        sql="silver_temperature.sql",
    )

    silver_vibration = SQLExecuteQueryOperator(
        task_id="silver_vibration",
        conn_id="postgres_default",
        sql="silver_vibration.sql",
    )

    silver_30min_agg = SQLExecuteQueryOperator(
        task_id="silver_30min_agg",
        conn_id="postgres_default",
        sql="silver_30min_agg.sql",
    )

    gold_pump_health = SQLExecuteQueryOperator(
        task_id="gold_pump_health",
        conn_id="postgres_default",
        sql="gold_pump_health.sql",
    )

    upload_gold = PythonOperator(
    task_id="upload_gold_to_supabase",
    python_callable=upload_gold_to_supabase,
    )

    (
        silver_temperature
        >> silver_vibration
        >> silver_30min_agg
        >> gold_pump_health
        >> upload_gold
    )