from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id= 'dag_start_spark',
    start_date= datetime(2025, 12, 4),
    schedule_interval='@daily',
) as dag:

    run_spark = BashOperator(
        task_id='start_task',
        bash_command='docker exec spark-master spark-submit --master spark://spark-master:7077 /workspace/.ipynb_checkpoints/spark-checkpoint.ipynb'
    )

