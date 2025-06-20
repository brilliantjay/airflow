import pendulum
# Airflow 3.0 부터 아래 경로로 import 합니다.
#from airflow.providers.standard.operators.bash import BashOperator
#from airflow.sdk import DAG

# Airflow 2.10.5 이하 버전에서 실습시 아래 경로에서 import 하세요.
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow import DAG
from airflow.models import Variable
from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator


with DAG(
    dag_id="dags_seoul_api_corona",
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    #tb_corona19_count_status = SeoulApiToCsvOperator(
        #task_id = 'tb_corona19_count_status',
        #dataset_nm = 'TbCorona19CountStatus',
        #path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        #path='/opt/airflow/files/TbCorona19CountStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        #file_name = 'TbCorona19CountStatus.csv'
    #)
    
    
    tv_corona19_vaccine_stat_new = SeoulApiToCsvOperator(
        task_id = 'tv_corona19_vaccine_stat_new',
        dataset_nm = 'tvCorona19VaccinestatNew',
        path='/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name = 'tvCorona19VaccinestatNew.csv'
    )

    tv_corona19_vaccine_stat_new