# Package Import
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.models import Variable
import pendulum

with DAG(
    dag_id='dags_simple_http_operator',
    start_date=pendulum.datetime(2023, 4, 1, tz='Asia/Seoul'),
    catchup=False,
    schedule=None
) as dag:
    
    '''서울시 공공자전거 이용정보(월별)'''
    tb_cycle_station_info = SimpleHttpOperator(
        task_id='tb_cycle_station_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleRentUseMonthInfo/1/10/202208',
        method='GET',
        headers={'Content-Type':'application/json',
                 'charset': 'utf-8',
                 'Accept': '*/*'
                }
    )
    print('{{var.value.apikey_openapi_seoul_go_kr}}')
    
    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_station_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_cycle_station_info >> python_2()