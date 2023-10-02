from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    send_email_task = EmailOperator(        
        task_id = 'send_email_task',    # 태스크 ID
        to='oh12sung@naver.com',        # 받을 사람
        subject='Airflow 성공메일',      # 메일 제목
        html_content='Airflow 작업이 완료되었습니다.' # 메일 내용
    )