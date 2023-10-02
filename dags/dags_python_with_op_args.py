from airflow import DAG
import datetime
import pendulum
from airflow.operators.python import PythonOperator
from common.common_func import regist
import random

# 이 DAG는 어떤 DAG인지 설명하는 부분
with DAG(
    dag_id="dags_python_with_op_args",                       
    schedule="30 6 * * *",                               
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), 
    catchup=False,                                      
) as dag:
    regist_t1 = PythonOperator(
        task_id='regist_t1',
        python_callable=regist,
        op_args=['shoh','man','kr','seoul']
    )
    regist_t1