from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "kafka", "minio", "clickhouse"],
) as dag:

    kafka_to_minio = BashOperator(
        task_id="kafka_to_minio",
        bash_command=(
            "docker exec spark spark-submit "
            "--jars "
            "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.0.jar,"
            "/opt/spark/jars/kafka-clients-3.5.0.jar,"
            "/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.0.jar,"
            "/opt/spark/jars/commons-pool2-2.11.1.jar,"
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.526.jar "
            "/home/jovyan/scripts/kafka_to_minio_batch.py"
        ),
    )

    minio_to_clickhouse = BashOperator(
        task_id="minio_to_clickhouse",
        bash_command=(
            "docker exec spark spark-submit "
            "--jars "
            "/opt/spark/jars/hadoop-aws-3.3.4.jar,"
            "/opt/spark/jars/aws-java-sdk-bundle-1.12.526.jar "
            "/home/jovyan/scripts/minio_to_clickhouse.py"
        ),
    )

    kafka_to_minio >> minio_to_clickhouse