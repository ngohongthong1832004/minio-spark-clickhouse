from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="spark_etl_minio_to_clickhouse",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/10 * * * *",  # chạy mỗi 10 phút
    catchup=False,
    description="Run Spark ETL with MinIO and ClickHouse via BashOperator"
) as dag:

    # ✅ Test kết nối docker exec
    run_test = BashOperator(
        task_id="run_test",
        bash_command='docker exec spark echo "✅ docker exec working!"'
    )

    # ✅ Chạy fake_to_minio.py
    generate_fake_data = BashOperator(
        task_id="generate_fake_data",
        bash_command="""
        docker exec spark bash -c '
            set -e
            echo "🔥 Running fake_to_minio.py..."
            sleep 2
            python3 /app/fake_to_minio.py
            echo "✅ Done fake_to_minio.py"
        '
        """
    )

    # ✅ Chạy Spark ETL từ MinIO → ClickHouse
    run_spark_etl = BashOperator(
        task_id="run_spark_job",
        bash_command="""
        docker exec spark bash -c '
            set -e
            echo "🔥 Starting spark-submit..." > /tmp/debug_etl.log
            spark-submit \
                --master local[*] \
                --jars /app/jars/clickhouse-jdbc-all.jar \
                --driver-class-path /app/jars/clickhouse-jdbc-all.jar \
                --conf spark.executor.extraClassPath=/app/jars/clickhouse-jdbc-all.jar \
                /app/etl_minio_to_clickhouse.py >> /tmp/debug_etl.log 2>&1
            echo "✅ Finished spark-submit" >> /tmp/debug_etl.log
        '
        docker exec spark cat /tmp/debug_etl.log >> /proc/1/fd/1 2>&1
        """
    )

    # 👇 Thứ tự chạy
    run_test >> generate_fake_data >> run_spark_etl
