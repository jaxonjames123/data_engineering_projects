from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

import pendulum
local_tz = pendulum.timezone("America/New_York")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
}


dag = DAG('biotricity_claims_to_s3',
          description='Copy biotricity claims files to OptimusSCN/Claims bucket on s3',
          default_args=default_args,
          schedule="0 8 * * 5",
          start_date=datetime(2025, 2, 8, tzinfo=local_tz), catchup=False)


t1 = BashOperator(
    task_id='biotricity_claims_to_s3',
    bash_command="/home/etl/etl_home/scripts/biotricity_claims_to_s3.sh ",
    retries=0,
    dag=dag)
