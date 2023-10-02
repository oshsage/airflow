from airflow import DAG
# python의 DateTime 이라는 타입
import datetime
# DataTime 타입을 좀 더 쉽게 쓸 수 있도록 제공해주는 라이브러리
import pendulum
from airflow.operators.python import PythonOperator

'''
파이썬 인터프리터 가상환경을 만들었을 때, 기본적으로 프로젝트의 최상위 홈인 AIRFLOW 디렉토리를 PATH로 잡게 됨.
그래서 plugins부터 경로를 입력해줘야 함. 이렇게 하면 우리 개발 환경에서는 에러가 안 나는데 이 상태를 git에 올리고
에어플로우가 인식하게 되면 에어플로우는 에러가 나게 됨. 왜냐하면 에어플로우는 plugins 폴더까지 패스로 잡혀있는 상태여서
그래서 여기서 노란색 경고줄이 뜨더라도 저장하고 에어플로우로 넘기면 에어플로우에서는 에러가 안 날 것이다!
노란색 줄도 안 뜨게 하는 방법은 최상위(AIRFLOW) 폴더 바로 아래에 `.env`라는 폴더를 만들고 아래와 같이 입력한다.

WORKSPACE_FOLDER=C:/Users/osh/vscode/airflow
PYTHONPATH=${WORKSPACE_FOLDER}/plugins

이렇게 하면 plugins 폴더까지 PATH로 잡힐 수 있다.
`.env`까지 git에 올릴 필요는 없기 때문에 gitignore에 추가해주자.
'''
from common.common_func import get_sftp

# 이 DAG는 어떤 DAG인지 설명하는 부분
with DAG(
    dag_id="dags_python_operator",                       
    schedule="30 6 * * *",                               
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), 
    catchup=False,                                      
) as dag:
    
    task_get_sftp = PythonOperator(
        task_id='task_get_sftp',
        python_callable=get_sftp
    )