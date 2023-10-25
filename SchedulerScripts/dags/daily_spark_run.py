import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta



# Default Dag declartion
default_args = {
    "owner": "STHZ",
    "start_date": pendulum.datetime(2023, 10, 23, tz="Asia/Rangoon"),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# Dags definition
with DAG (
    "spark_ELT_pipeline",
    default_args = default_args,
    schedule="@daily",
    tags = ['spark_ELT_pipeline']
) as dag:
    submit = BashOperator(task_id="submit_pyspark",
        bash_command='./pyspark_submit.sh'
    )
   