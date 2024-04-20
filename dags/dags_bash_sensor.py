from airflow.sensors.bash import BashSensor
from airflow.operators.bash import BashOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_bash_sensor',
    start_date=pendulum.datetime(2023,4,1, tz='Asia/Seoul'),
    schedule='0 6 * * *',
    catchup=False
) as dag:

    sensor_task_by_poke = BashSensor(
        task_id='sensor_task_by_poke',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command=f'''echo $FILE &&
                        if [ -f $FILE ]; then
                            exit 0
                        else
                            exit 1
                        fi''',
        poke_interval=30, #체크주기 : 30초
        timeout=60*2, #체크 fail 시기 : 2분
        mode='poke', # 짧은 센싱(초단위) 간격일 때는 poke
        soft_fail=False
    )

    sensor_task_by_reschedule = BashSensor(
        task_id='sensor_task_by_reschedule',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command=f''' echo $FILE &&
                        if [ -f $FILE ]; then
                            exit 0
                        else
                            exit 1
                        fi''',
        poke_interval=60*2, #체크주기 : 2분
        timeout=60*9, #체크 fail 시기 : 9분
        mode='reschedule', # 긴 센싱(분단위) 간격일 때는 reschedule
        soft_fail=True
    )

    bash_task = BashOperator(
        task_id='bash_task',
        env={'FILE':'/opt/airflow/files/tvCorona19VaccinestatNew/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}/tvCorona19VaccinestatNew.csv'},
        bash_command='echo "건수: `cat $FILE | wc -l`"',
    )

    [sensor_task_by_poke, sensor_task_by_reschedule] >> bash_task