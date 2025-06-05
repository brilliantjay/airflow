import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.decorators  import task

with DAG(
    dag_id="dags_bash_python_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_push = BashOperator(
        task_id = "bash_push",
        bash_command = 'echo PUSH START {{ti.xcom_push(key="bash_pushed",value=200)}} && echo PUSH_COMPLETE'
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key='bash_pushed')
        return_value = ti.xcom_pull(task_ids='bash_push')
        print('status_value:'+str(status_value))
        print('return_value:'+return_value)


    bash_push >> python_pull_xcom()    