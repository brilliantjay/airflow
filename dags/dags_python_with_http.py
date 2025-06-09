from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow import DAG
from airflow.decorators import task

import pendulum


with DAG(
    dag_id="dags_python_with_http",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:    

    tb_cycle_station_info = SimpleHttpOperator(
        task_id="insrt_postgres",
        http_conn_id = "openapi.seoul.go.kr",
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tpssRouteSectionTime/1/10/',
        method="GET",
        headers={'Content-Type':"application/json",'charset':'utf-8','Accept':'*/*'}       
    )  

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom.pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint
        pprint(json.loads(rslt))



    tb_cycle_station_info >> python_2