from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

dag = DAG(
    'rfm_analysis',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

run_spark = BashOperator(
    task_id='run_job',
    bash_command='docker exec jupyterlab python /opt/workspace/spark.py',
    dag=dag
)

run_spark