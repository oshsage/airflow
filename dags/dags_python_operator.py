from airflow import DAG
# python의 DateTime 이라는 타입
import datetime
# DataTime 타입을 좀 더 쉽게 쓸 수 있도록 제공해주는 라이브러리
import pendulum
from airflow.operators.python import PythonOperator
import random

# 이 DAG는 어떤 DAG인지 설명하는 부분
with DAG(
    dag_id="dags_python_operator",                       
    schedule="30 6 * * *",                               
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), 
    catchup=False,                                      
) as dag:
    
    def select_fruit():
        fruit = ['APPLE', 'BANANA', 'ORANGE', 'AVOCADO']
        rand_int = random.randint(0,3)
        print(fruit[rand_int])

    py_t1 = PythonOperator(
        task_id='py_t1',
        python_callable=select_fruit # 어떤 함수를 실행시킬거냐?
    )

    py_t1