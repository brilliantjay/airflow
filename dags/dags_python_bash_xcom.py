import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
from airflow.operators.bash import BashOperator
from airflow import DAG
from airflow.decorators  import task

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'Good', 'data':[1,2,3], 'options_cnt':100}
        return result_dict
    
    bash_pull = BashOperator(
        task_id = 'bash_pull',
        env={
            'STATUS':'{{ti.xcom_pull(task_ids="python_push")["status"]}}',
            'DATA':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',
            'OPTIONS_CNT':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'            
        },
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )

    python_push_xcom() >> bash_pull