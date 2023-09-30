'''
DAG가 될 파일이다.
'''

from airflow import DAG
# python의 DateTime 이라는 타입
import datetime
# DataTime 타입을 좀 더 쉽게 쓸 수 있도록 제공해주는 라이브러리
import pendulum
from airflow.operators.bash import BashOperator

# 이 DAG는 어떤 DAG인지 설명하는 부분
with DAG(
    dag_id="dags_bash_operation",                       # Airflow 처음 들어왔을 때 보이는 Dag 이름들. 파이썬 파일명은 DAG 명과 아무런 상관이 없다. DAG 파일명과 DAG ID는 일치시키는 것이 좋다.
    schedule="0 0 * * *",                               # Cron 스케줄 : 분 시 일 월 요일
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), # 덱이 언제부터 돌건지. utc는 세계표준시로 한국보다 9시간이 느림. 그래서 한국시간으로 바꿔야함
    catchup=False,                                      # 현재 날짜와 start_date 사이에 못 돌린 구간을 모두 돌릴거냐를 결정. false : 돌리지 않은 true : 두 기간 사이의 구간을 모두 돌림. 근데 차례로 돌지 않고 한꺼번에 돎
    # dagrun_timeout=datetime.timedelta(minutes=60),    # timeout 값 설정. 저 설정은 60분이상 돌게 되면 실패하도록 설정되어있음
    # tags=["example", "example2"],                     # Airflow에 들어왔을 때 Dag 이름 밑에 보이는 파란색 박스(태그)를 어떤 값으로 할 지
    # params={"example_key": "example_value"},          # 아래에 Task들을 만들 때 공통된 파라미터가 있다면 여기에 작성한다.
) as dag:
    
    # 변수이름이 task 이름이 되고 task를 오퍼레이터로 실행하게된다.
    bash_t1 = BashOperator(
        task_id="bash_t1",    # airflow 에서 graph를 보면 노드에 들어가는 이름을 설정. 객체 변수명이 이름으로 반영되는게 아님. 그래서 둘을 갖게 하겠음.
        bash_command="echo whoami",  # 우리가 어떤 쉘 스크립트를 수행할거냐. 이러면 나중에 실행하면서 저 스크립트가 수행되고 결과적으로 whoami라는게 출력됨.
             
    )

    bash_t2 = BashOperator(
        task_id ="bash_t2",
        bash_command="echo $HOSTNAME",

    )

    bash_t1 >> bash_t2