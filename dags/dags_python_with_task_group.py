from airflow import DAG
import pendulum
import datetime
from airflow.operators.python import PythonOperator
from airflow.decorators import task
from airflow.decorators import task_group
from airflow.utils.task_group import TaskGroup

with DAG(
    dag_id="dags_python_with_task_group",
    schedule=None,
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def inner_func(**kwargs):
        msg = kwargs.get('msg') or '' 
        print(msg)

    @task_group(group_id='first_group')
    def group_1():
        ''' task_group 데커레이터를 이용한 첫 번째 그룹입니다. '''    # Airflow tooltip에 표시된다.

        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('첫 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg':'첫 번째 TaskGroup내 두 번쨰 task입니다.'}
        )

        inner_func1() >> inner_function2

    
    with TaskGroup(group_id='second_group', tooltip='두 번째 그룹입니다') as group_2:    # tooltip -> Airflow UI에 표시된다.
        ''' 여기에 적은 docstring은 표시되지 않습니다'''
        @task(task_id='inner_function1')
        def inner_func1(**kwargs):
            print('두 번째 TaskGroup 내 첫 번째 task입니다.')

        inner_function2 = PythonOperator(
            task_id='inner_function2',
            python_callable=inner_func,
            op_kwargs={'msg': '두 번째 TaskGroup내 두 번째 task입니다.'}
        )
        inner_func1() >> inner_function2

    group_1() >> group_2

    '''
    확인해야 할 것
    1. group_1 함수 내의 task_id와 TaskGroup 클래스 내의 task_id가 같은데 task_id가 중복되는 것일까?
    => 아니다 중복되지 않고 각각의 task_id로 인식한다.

    2. group_1() >> group_2 처럼 Task Group도 플로우로 지정이 가능한가?
    => 그렇다.

    3. group_1 함수 내의 docstring 부분이 Airflow UI 화면의 Tooltip이라는 이름으로 태스크 그룹에 대한 설명을 제공하는가?
    => 그렇다.
    '''