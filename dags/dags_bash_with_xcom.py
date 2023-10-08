from airflow import DAG
import pendulum
import datetime
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_xcom",
    schedule="10 0 * * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    bash_push = BashOperator(
        task_id='bash_push',
        bash_command="echo START && "
                    "echo XCOM_PUSHED "
                    # 템플릿 문법에서 ti 객체를 이용할 수 있다. 
                    "{{ ti.xcom_push(key='bash_pushed',value='first_bash_message') }} && "
                    "echo COMPLETE"    # Bash Operator에서는 마지막 출력문을 자동으로 return_value로 저장함.
    )

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={'PUSHED_VALUE':"{{ ti.xcom_pull(key='bash_pushed') }}",
            'RETURN_VALUE':"{{ ti.xcom_pull(task_ids='bash_push') }}"},    # task_ids만 지정하면 return_value를 가져온다는 것, 즉 위에서 가장 마지막 echo문인 "COMPLETE"가 대상임.
        bash_command="echo $PUSHED_VALUE && echo $RETURN_VALUE ",
        do_xcom_push=False
    )

    bash_push >> bash_pull