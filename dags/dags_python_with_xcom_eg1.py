import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
from airflow.operators.python import PythonOperator
from airflow import DAG
import datetime
from airflow.decorators  import task

with DAG(
    dag_id="dags_python_with_xcom_eg1",
    schedule="30 6 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    

  @task(task_id='python_xcom_push_task1')
  def xcom_push1(**kwargs):
    ti = kwargs["ti"]
    ti.xcom_push(key="resutl1",value="value_1")
    ti.xcom_push(key="result2", value=[1,2,3]) 

  @task(task_id='python_xcom_push_task2')
  def xcom_push2(**kwargs):    
      ti = kwargs['ti']
      ti.xcom_push(key="result1", value="value_2")
      ti.xcom_push(key="result2",value=[1,2,3,4])

  @task(task_id='python_xcom_pull_task')
  def xcom_pull(**kwargs):
     ti = kwargs['ti'] 
     value1 = ti.xcom_pull(key='result1')
     value2 = ti.xcom_pull(key='result2', task_ids='python_xcom_push_task1')   
     print(value1)
     print(value2)


  xcom_push1() >> xcom_push2() >> xcom_pull()    