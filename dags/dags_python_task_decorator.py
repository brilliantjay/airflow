import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.decorators import task
from pprint import pprint

with DAG(
    dag_id="dags_python_task_decorator",
    schedule="0 2 * * 1",
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"),
    catchup=False  
) as dag:

    # [START howto_operator_python]
    @task(task_id="python_task_1")
    def print_context(some_input):
       print(some_input)

    python_task_1 = print_context('task_decorator 실행')