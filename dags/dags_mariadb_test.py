import os
from datetime import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator    


with DAG(
    dag_id="dags_mariadb_test",
    schedule="@daily",
    start_date=datetime(2021, 10, 1),   
    catchup=False,
) as dag:
    
    create_table_mariadb_task = SQLExecuteQueryOperator(
        task_id="create_country_table",
        conn_id="test",
        sql=r"""
        CREATE TABLE rdb.Country6 (
            country_id INT NOT NULL AUTO_INCREMENT,
            name VARCHAR(50),
            continent VARCHAR(50),
            PRIMARY KEY (country_id)
        );
        """   
    )

    populate_user_table = SQLExecuteQueryOperator(
        task_id="populate_user_table",
        conn_id="test",
        sql=r"""
                INSERT INTO rdb.rdb_user (USER_NAME, USER_ID, USER_ROLE, USER_PW,IS_ADMIN) VALUES ('test3', 'test1234568', 'ADMIN', 'welco123!','Y');               
                """,
    )
    get_rdb_user = SQLExecuteQueryOperator(
        task_id="get_rdb.rdb_user",
        conn_id="test",
        sql=r"""SELECT USER_ID, USER_PW, USER_NAME, USER_ROLE, IS_ADMIN FROM rdb.rdb_user;"""    
    )
   
    get_rdb_one_user = SQLExecuteQueryOperator(
        task_id="get_rdb_one_user",
        conn_id="test",
        sql=r"""SELECT USER_ID, USER_PW, USER_NAME, USER_ROLE, IS_ADMIN FROM rdb.rdb_user where {{ params.column }}='{{ params.value }}';""",
        params={"column": "USER_ID", "value": "test12345"},
    )
    
    create_table_mariadb_task >> populate_user_table >> get_rdb_user >> get_rdb_one_user
    