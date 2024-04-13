from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd

class CustomPostgresHook(BaseHook):

    def __init__(self, postgres_conn_id, **kwargs):
        self.postgres_conn_id = postgres_conn_id

    # 기존의 get_conn을 오버라이드
    def get_conn(self):
        airflow_conn = BaseHook.get_connection(self.postgres_conn_id)
        self.host = airflow_conn.host
        self.user = airflow_conn.login
        self.password = airflow_conn.password
        self.dbname = airflow_conn.schema
        self.port = airflow_conn.port
        
        # postgres 연결 객체 생성
        self.postgres_conn = psycopg2.connect(host=self.host, user=self.user, password=self.password, dbname=self.dbname, port=self.port)
        return self.postgres_conn

    def bulk_load(self, table_name, file_name, delimiter: str, is_header: bool, is_replace: bool):
        from sqlalchemy import create_engine # 경로에 테이블이 없으면 생성해주게 도와줌

        self.log.info('적재 대상파일:' + file_name)
        self.log.info('테이블 :' + table_name)
        self.get_conn()     # postgres 연결
        header = 0 if is_header else None   # is_header = True면 0, False면 None : header가 테이블에 포함되는 것 방지
        if_exists = 'replace' if is_replace else 'append'   # is_replace = True면 replace, False면 append
        file_df = pd.read_csv(file_name, header=header, delimiter=delimiter)

        for col in file_df.columns:
            try:
                # string 문자열이 아닐 경우 continue
                file_df[col] = file_df[col].str.replace('\r\n','')  # 줄넘김 및 ^M 제거
                self.log.info(f'{table_name}.{col}: 개행문자 제거')
            except:
                continue
        
        self.log.info('적재 건수:' + str(len(file_df)))
        uri = f'postgresql://{self.user}:{self.password}@{self.host}/{self.dbname}'
        engine = create_engine(uri)
        file_df.to_sql(name=table_name,
                       con=engine,
                       schema='public',
                       if_exists=if_exists,
                       index=False)

    