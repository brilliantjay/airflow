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
        CREATE TABLE rdb.Country (
            country_id INT NOT NULL IDENTITY(1,1) PRIMARY KEY,
            name VARCHAR(50),
            continent VARCHAR(50)
        );
        """   
    )

    populate_user_table = SQLExecuteQueryOperator(
        task_id="populate_user_table",
        conn_id="test",
        sql=r"""
                INSERT INTO rdb.rdb_user (username, description)
                (USER_NAME, USER_ID, USER_ROLE, USER_PW,IS_ADMIN) VALUES ('test', 'test12345', 'ADMIN', 'welco123!','Y');               
                """,
    )
    get_all_countries = SQLExecuteQueryOperator(
        task_id="get_all_countries",
        conn_id="test",
        sql=r"""SELECT * FROM Country;""",
    )
   
    get_countries_from_continent = SQLExecuteQueryOperator(
        task_id="get_countries_from_continent",
        conn_id="test",
        sql=r"""SELECT * FROM rdb.Country where {{ params.column }}='{{ params.value }}';""",
        params={"column": "CONVERT(VARCHAR, continent)", "value": "Asia"},
    )
    
    create_table_mariadb_task >> populate_user_table >> get_all_countries >> get_countries_from_continent
    