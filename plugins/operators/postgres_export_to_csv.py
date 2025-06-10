from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PostgresCsvExportOperator(BaseOperator):
    def __init__(self, postgres_conn_id, sql, filepath, **kwargs):
        super().__init__(**kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.sql = sql
        self.filepath = filepath
        
    def execute(self, context):
        hook = PostgresHook(
            postgres_conn_id = self.postgres_conn_id,
        )
        get_records = hook.get_pandas_df(self.sql)
        get_records.to_csv(self.filepath)
    
    