from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


with DAG(
    dag_id="sales_generator",
    start_date=datetime(2026, 1, 1),
    schedule="* * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["streaming", "generator", "kafka"],
) as dag:

    generate_sales = BashOperator(
        task_id="generate_sales",
        bash_command=(
            "docker exec spark "
            "python /home/jovyan/scripts/sales_producer_batch.py"
        ),
        do_xcom_push=False,
    )

    trigger_pipeline = BashOperator(
        task_id="trigger_sales_pipeline",
        bash_command="docker exec airflow-scheduler airflow dags trigger sales_pipeline",
        do_xcom_push=False,
    )

    generate_sales >> trigger_pipeline