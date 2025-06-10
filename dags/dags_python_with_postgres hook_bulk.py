from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pendulum


with DAG(
    dag_id="dags_python_with_postgres_hook_bulk_load",
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    def insrt_postgres(postgres_conn_id, tbl_nm,file_nm, **kwargs):
       postgres_hook = PostgresHook(postgres_conn_id)
       postgres_hook.bulk_load(tbl_nm,file_nm) 
       

    insrt_postgres = PythonOperator(task_id="insrt_postgres", python_callable=insrt_postgres, op_kwargs={'postgres_conn_id':'conn-db-postgres-custom',
                   'tbl_nm':"tvCorona19VaccinestatNew_bulk1",
                   'file_nm':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash }}/tvCorona19VaccinestatNew.csv'})            

    insrt_postgres