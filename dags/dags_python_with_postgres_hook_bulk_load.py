from airflow.operators.python import PythonOperator
from airflow import DAG

import pendulum
from hooks.custom_postgres_hook import CustomPostgresHook


with DAG(
    dag_id="dags_python_with_postgres_hook_bulk_load",
    schedule=None,
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    def insert_postgres(postgres_conn_id, tbl_nm, file_nm, **kwargs):
        custom_postgres_hook = CustomPostgresHook(postgres_conn_id = postgres_conn_id)
        custom_postgres_hook.bulk_load(table_name=tbl_nm,file_name=file_nm, delimiter=',', is_header=True, is_replace=True)


    insrt_postgres = PythonOperator(
        task_id='insrt_postgres',
        python_callable=insert_postgres,
        op_kwargs={'postgres_conn_id':'conn-db-postgres-custom',  'tbl_nm':"tvCorona19VaccinestatNew_bulk2",
                   'file_nm':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'})            
      

    insrt_postgres
