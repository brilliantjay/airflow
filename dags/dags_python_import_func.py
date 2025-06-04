import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
from airflow.operators.python import PythonOperator
from airflow import DAG
from common.common_func import get_sftp

with DAG(
    dag_id="dags_python_import_func",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    task_get_sftp = PythonOperator(task_id='task_get_sftp',python_callable=get_sftp)
    task_get_sftp