import os
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator    


with DAG(
    dag_id="dags_mariadb_test2",
    schedule="@daily",
    start_date=datetime(2021, 10, 1),   
    catchup=False,
) as dag:
    
    get_rdb_user = SQLExecuteQueryOperator(
        task_id="get_rdb.rdb_user",
        conn_id="test",
        sql=r"""SELECT USER_ID, USER_PW, USER_NAME, USER_ROLE, IS_ADMIN, cast(REG_DT as char) FROM rdb.rdb_user;"""    
    )   


    get_rdb_user 