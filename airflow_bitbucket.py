 
#-*- coding: utf-8 -*-
#import airflow
#from airflow import DAG
#from airflow.models import DAG
#from airflow.operators.bash_operator import BashOperator
#from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from datetime import date
import time
import requests
from urllib.request import urlopen, base64
from requests.auth import HTTPBasicAuth
import json
import pandas as pd
import numpy as np
import datetime as dt
import pydata_google_auth
from google.oauth2 import service_account
from google.cloud import bigquery
import os


# Credentials
credentials = service_account.Credentials.from_service_account_file(
    'password file destination')
client = bigquery.Client(project = 'google big query project', credentials=credentials)


# Functions
def update_s_mrr_mes_conta():    
    url = "bitbucket url"
    resposta = requests.get(url, auth=HTTPBasicAuth('login', 'password'))
    resposta = resposta.text.encode('iso-8859-1').decode('utf-8')
    drop_table = """
        DROP TABLE 'droped table'
        """
    create_table = f"""
        CREATE TABLE 'created table'
        as
        (
        {resposta}
        )
        """
    try:
        client.query(drop_table).result()
        print('Tabela excluída')
    except:
        print('A tabela não existe')
        pass
    client.query(create_table).result()
    print('Tabela criada novamente')


default_args = {
  'owner': 'analytics',
  'depends_on_past': False,
  'start_date': datetime(2020,9,22),
  'email': ['login airfow email'],
  'email_on_failure': True,
  'email_on_retry': True,
  'retries': 3,
  'retry_delay': timedelta(minutes=1),
  'catchup' : False
}


dag = DAG('analytics_data_consolidation',
  default_args=default_args, 
    schedule_interval='30 4 * * *')

s_mrr_mes_conta = PythonOperator(
  task_id='s_mrr_mes_conta',
  provide_context=False,
  python_callable = update_s_mrr_mes_conta,
  dag=dag
)

s_mrr_mes_conta