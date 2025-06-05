from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow import DAG
from datetime import datetime

with DAG(
    dag_id='dags_mariadb_select_example',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    select_data_from_mariadb = SQLExecuteQueryOperator(
        task_id='select_data',
        mysql_conn_id='test', # Use the connection ID you created
        sql="SELECT * FROM rdb.rdb_user LIMIT 10;", # Your SQL query
        handler=lambda x: print(x) # You can define a handler to process the results
    )

    select_data_from_mariadb