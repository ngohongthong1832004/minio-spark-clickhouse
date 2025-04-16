from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_etl_minio_to_clickhouse",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/3 * * * *",  # 👈 Mỗi 3 phút
    catchup=False,
) as dag:

    run_spark_etl = BashOperator(
        task_id="run_spark_job",
        bash_command="docker exec spark spark-submit --master local /app/app.py"
    )
