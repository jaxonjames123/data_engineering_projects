from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


dag = DAG('email_mda_report_weekly',
          description='Emails MDA Report via Paubox every Monday',
          default_args=default_args,
          schedule_interval= "00 10 * * 1",
          start_date=datetime(2025, 5, 19), catchup=False)

t1 = BashOperator(
    task_id = 'email_mda_report_weekly',
    bash_command = "/home/etl/etl_home/scripts/send_mda_reports.sh ",
    retries = 0,
    dag = dag)
