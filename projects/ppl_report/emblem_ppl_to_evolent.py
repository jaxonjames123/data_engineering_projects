from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}


dag = DAG(
    "emblem_ppl_to_evolent",
    description="Moves Emblem PPL file from Emblem SFTP to Evolent SFTP",
    default_args=default_args,
    schedule_interval="00 8 * * *",
    start_date=datetime(2025, 5, 19),
    catchup=False,
)

t1 = BashOperator(
    task_id="emblem_ppl_to_evolent",
    bash_command="/home/etl/etl_home/scripts/emblem_ppl_to_evolent.sh ",
    retries=0,
    dag=dag,
)
