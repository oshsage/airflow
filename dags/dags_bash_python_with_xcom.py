from airflow import DAG
import pendulum
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_python_with_xcom",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 4, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    # Python -> Bash
    @task(task_id='python_push')
    def python_push_xcom():
        result_dict = {'status':'Good','data':[1,2,3],'options_cnt':100}
        return result_dict    # Python Operator에서 return을 한 값은 Xcom으로!

    bash_pull = BashOperator(
        task_id='bash_pull',
        env={
            'STATUS':'{{ti.xcom_pull(task_ids="python_push")["status"]}}',              # Good 출력
            'DATA':'{{ti.xcom_pull(task_ids="python_push")["data"]}}',                  # [1,2,3] 출력
            'OPTIONS_CNT':'{{ti.xcom_pull(task_ids="python_push")["options_cnt"]}}'     # 100 출력

        },
        bash_command='echo $STATUS && echo $DATA && echo $OPTIONS_CNT'
    )
    python_push_xcom() >> bash_pull

    # Bash -> Python
    bash_push = BashOperator(
    task_id='bash_push',
    bash_command='echo PUSH_START '
                 '{{ti.xcom_push(key="bash_pushed",value=200)}} && '
                 'echo PUSH_COMPLETE'    # 마지막 echo 값을 Xcom으로!
    )

    @task(task_id='python_pull')
    def python_pull_xcom(**kwargs):
        ti = kwargs['ti']
        status_value = ti.xcom_pull(key='bash_pushed')    # 200 출력
        return_value = ti.xcom_pull(task_ids='bash_push') # COMPLETE 출력
        print('status_value:' + str(status_value))
        print('return_value:' + return_value)

    bash_push >> python_pull_xcom()

