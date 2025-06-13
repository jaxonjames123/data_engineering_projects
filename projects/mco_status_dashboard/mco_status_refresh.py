from datetime import datetime

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

local_tz = pendulum.timezone("America/New_York")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    # 'retry_delay': timedelta(minutes=3),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'adhoc':False,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'trigger_rule': u'all_success'
}


cmd = """
    cd /home/etl/etl_home/lib
    python get_mco_details.py """


dag = DAG(
    "mco_status_refresh",
    description="runs database queries for mco_status page for roster, claims, pharmacy claims, gic, and mco status every hour",
    default_args=default_args,
    schedule="0 0-23 * * *",
    start_date=datetime(2023, 1, 19, tzinfo=local_tz),
    catchup=False,
)


t1 = BashOperator(task_id="mco_status_refresh", bash_command=cmd, retries=1, dag=dag)
