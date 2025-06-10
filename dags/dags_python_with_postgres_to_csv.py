from airflow.operators.python import PythonOperator
from airflow import DAG
from operators.postgres_export_to_csv import PostgresCsvExportOperator

import pendulum


with DAG(
    dag_id="dags_python_with_postgres_to_csv",
    schedule="@daily",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:

    export_csv = PostgresCsvExportOperator(
        task_id="export_csv",
        postgres_conn_id = "conn-db-postgres-custom",
        sql = "select * from tvcorona19vaccinestatnew_bulk1",
        filepath = "/opt/airflow/files/tvcorona19vaccinestatnew_bulk1.csv"
    )           

    export_csv